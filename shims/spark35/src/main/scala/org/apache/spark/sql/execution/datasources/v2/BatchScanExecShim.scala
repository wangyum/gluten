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
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class BatchScanExecShim(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None,
    @transient val table: Table,
    val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    // SPJ reducers are vestigial in spark35: GroupPartitionsExec (SPARK-55535) handles
    // coalescing, so these are never read. Kept for constructor signature compatibility.
    val reducers: Option[Seq[Option[Reducer[_, _]]]] = None,
    val applyPartialClustering: Boolean = false,
    val replicatePartitions: Boolean = false)
  extends AbstractBatchScanExec(
    output,
    scan,
    runtimeFilters,
    ordering,
    table,
    keyGroupedPartitioning
  ) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  lazy val metadataColumns: Seq[AttributeReference] = output.collect {
    case FileSourceConstantMetadataAttribute(attr) => attr
    case FileSourceGeneratedMetadataAttribute(attr, _) => attr
  }

  def hasUnsupportedColumns: Boolean = {
    // TODO, fallback if user define same name column due to we can't right now
    // detect which column is metadata column which is user defined column.
    val metadataColumnsNames = metadataColumns.map(_.name)
    output
      .filterNot(metadataColumns.toSet)
      .exists(v => metadataColumnsNames.contains(v.name))
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Need to implement this method")
  }

  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceV2Strategy.translateRuntimeFilterV2(e)
      case f => DataSourceV2Strategy.translateScalarSubqueryFilterV2(f)
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
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException(
              "Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }

          val inputMap = k.partitionKeys.groupBy(identity).mapValues(_.size)
          val comparableKeyWrapperFactory = InternalRowComparableWrapper
            .getInternalRowComparableWrapperFactory(k.expressionDataTypes)
          val filteredMap = newPartitions.groupBy(
            p => comparableKeyWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey()))

          if (!filteredMap.keySet.subsetOf(inputMap.keySet)) {
            throw new SparkException(
              "During runtime filtering, data source must not report new " +
                "partition keys that are not present in the original partitioning.")
          }

          inputMap.toSeq
            .sortBy(_._1)(k.keyOrdering)
            .flatMap {
              case (key, size) =>
                val fps = filteredMap.getOrElse(key, Array.empty)

                if (fps.size > size) {
                  throw new SparkException(
                    "During runtime filtering, data source must not report " +
                      s"new partitions for a given key. Before: $size partitions. " +
                      s"After: ${fps.size} partitions")
                }

                fps.map(Seq(_)).padTo(size, Seq.empty)
            }

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.toSeq.map(Seq(_))
      }

    } else {
      (outputPartitioning match {
        case k: KeyedPartitioning =>
          inputPartitions
            .sortBy(_.asInstanceOf[HasPartitionKey].partitionKey())(k.keyRowOrdering)
            .map(Seq(_))

        case _ => inputPartitions.map(Seq(_))
      })
    }
  }

  @transient lazy val pushedAggregate: Option[Aggregation] = {
    scan match {
      case s: ParquetScan => s.pushedAggregate
      case o: OrcScan => o.pushedAggregate
      case _ => None
    }
  }
}

abstract class ArrowBatchScanExecShim(original: BatchScanExec)
// SPJ params (commonPartitionValues, reducers, applyPartialClustering,
// replicatePartitions) are intentionally omitted: in spark35, GroupPartitionsExec
// (SPARK-55535) handles SPJ coalescing, so these params are not used.
  extends BatchScanExecShim(
    original.output,
    original.scan,
    original.runtimeFilters,
    original.keyGroupedPartitioning,
    original.ordering,
    original.table
  ) {
  override def scan: Scan = original.scan

  override def ordering: Option[Seq[SortOrder]] = original.ordering

  override def output: Seq[Attribute] = original.output
}
