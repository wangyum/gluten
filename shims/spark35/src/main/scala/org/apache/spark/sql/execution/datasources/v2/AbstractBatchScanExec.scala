/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._

import com.google.common.base.Objects

/** Physical plan node for scanning a batch of data from a data source v2. */
abstract class AbstractBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    val runtimeFilters: Seq[Expression],
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None
) extends DataSourceV2ScanExecBase {

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: AbstractBatchScanExec =>
      this.batch != null && this.batch == other.batch &&
      this.runtimeFilters == other.runtimeFilters &&
      this.keyGroupedPartitioning == other.keyGroupedPartitioning
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)

  @transient override lazy val inputPartitions: Seq[InputPartition] = batch.planInputPartitions()

  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceV2Strategy.translateRuntimeFilterV2(e)
      case f => DataSourceV2Strategy.translateScalarSubqueryFilterV2(f)
    }

    val originalPartitioning = outputPartitioning
    if (dataSourceFilters.nonEmpty) {
      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case k: KeyedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException("Data source must have preserved the original partitioning " +
              "during runtime filtering: not all partitions implement HasPartitionKey after " +
              "filtering")
          }

          val inputMap = k.partitionKeys.groupBy(identity).mapValues(_.size)
          val comparableKeyWrapperFactory = InternalRowComparableWrapper
            .getInternalRowComparableWrapperFactory(k.expressionDataTypes)
          val filteredMap = newPartitions.groupBy(
            p => comparableKeyWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey()))

          if (!filteredMap.keySet.subsetOf(inputMap.keySet)) {
            throw new SparkException("During runtime filtering, data source must not report new " +
              "partition keys that are not present in the original partitioning.")
          }

          inputMap.toSeq
            .sortBy(_._1)(k.keyOrdering)
            .flatMap {
              case (key, size) =>
                // We require the new number of partitions to be equal or less than the old number of
                // partitions for a given key. In the case of less than, empty partitions are added.
                val fps = filteredMap.getOrElse(key, Array.empty)

                if (fps.size > size) {
                  throw new SparkException(
                    "During runtime filtering, data source must not report " +
                      s"new partitions for a given key. Before: $size partitions. " +
                      s"After: ${fps.size} partitions")
                }

                fps.map(p => Seq(p)).padTo(size, Seq.empty)
            }

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.toSeq.map(Seq(_))
      }

    } else {
      (originalPartitioning match {
        case k: KeyedPartitioning =>
          inputPartitions.sortBy(_.asInstanceOf[HasPartitionKey].partitionKey())(k.keyRowOrdering)

        case _ => inputPartitions
      }).map(Seq(_))
    }
  }

  override def outputPartitioning: Partitioning = {
    if (
      keyGroupedPartitioning.isDefined &&
      KeyedPartitioning.supportsExpressions(keyGroupedPartitioning.get)
    ) {
      val expressions = keyGroupedPartitioning.get
      val hasPartitionKeyPartitions =
        inputPartitions.filter(_.isInstanceOf[HasPartitionKey])
      if (hasPartitionKeyPartitions.isEmpty) {
        // No HasPartitionKey partitions (e.g. table is empty or all partitions filtered out).
        // Return a KeyedPartitioning with empty keys so that operators like GroupPartitionsExec
        // which require a Partitioning with Expression can still function correctly.
        KeyedPartitioning(expressions, Seq.empty)
      } else {
        val keyRowOrdering =
          RowOrdering.createNaturalAscendingOrdering(expressions.map(_.dataType))
        val partitionKeys = hasPartitionKeyPartitions
          .map(_.asInstanceOf[HasPartitionKey].partitionKey())
          .sorted(keyRowOrdering)
        KeyedPartitioning(expressions, partitionKeys)
      }
    } else {
      super.outputPartitioning
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      new DataSourceRDD(
        sparkContext,
        filteredPartitions.map(part => part.headOption),
        readerFactory,
        supportsColumnar,
        customMetrics)
    }
    postDriverMetrics()
    rdd
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }
}
