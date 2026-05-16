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
    val spjParams: StoragePartitionJoinParams = StoragePartitionJoinParams()
) extends DataSourceV2ScanExecBase {

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: AbstractBatchScanExec =>
      this.batch != null && this.batch == other.batch &&
      this.runtimeFilters == other.runtimeFilters &&
      this.spjParams == other.spjParams
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)

  @transient override lazy val inputPartitions: Seq[InputPartition] = inputPartitionsShim

  @transient protected lazy val inputPartitionsShim: Seq[InputPartition] =
    batch.planInputPartitions()

  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceV2Strategy.translateRuntimeFilterV2(e)
      case _ => None
    }

    if (dataSourceFilters.nonEmpty) {
      val originalPartitioning = outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case k: KeyedPartitioning =>
          val inputMap = k.partitionKeys
            .groupBy(identity)
            .map {
              case (key, parts) => key -> parts.size
            }
            .toSeq
            .sortBy(_._1)(k.keyOrdering)

          val filteredMap = newPartitions
            .groupBy(
              p => InternalRowComparableWrapper(p.asInstanceOf[HasPartitionKey], k.expressions))
            .map {
              case (key, parts) => key -> parts.toSeq
            }

          inputMap.flatMap {
            case (key, size) =>
              // We require the new number of partitions to be equal or less than the old number
              // of partitions for a given key. In the case of less than, empty partitions
              // are added.
              filteredMap.getOrElse(key, Seq.empty)
                .map(Seq(_)).padTo(size, Seq.empty)
          }
        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.map(Seq(_))
      }

    } else {
      partitions
        .map(_.toSeq)
    }
  }

  override def outputPartitioning: Partitioning = {
    super.outputPartitioning match {
      case k: KeyedPartitioning if spjParams.commonPartitionValues.isDefined =>
        // We allow duplicated partition values if
        // `spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled` is true
        val keyWrapperFactory = InternalRowComparableWrapper
          .getInternalRowComparableWrapperFactory(k.expressions.map(_.dataType))
        val newPartValues = spjParams.commonPartitionValues.get.flatMap {
          case (partValue, numSplits) =>
            Seq.fill(numSplits)(keyWrapperFactory(partValue))
        }
        k.copy(partitionKeys = newPartValues, isGrouped = false)
      case p => p
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

  override def keyGroupedPartitioning: Option[Seq[Expression]] =
    spjParams.keyGroupedPartitioning

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }
}
