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
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.truncatedString
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

  @transient override lazy val inputPartitions: Seq[InputPartition] = inputPartitionsShim

  @transient protected lazy val inputPartitionsShim: Seq[InputPartition] =
    batch.planInputPartitions()

  @transient protected def filteredPartitions: Seq[Seq[InputPartition]]

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      // SPARK-55535 changed DataSourceRDD to accept Seq[Option[InputPartition]].
      // Gluten's BatchScanExecShim.filteredPartitions returns Seq[Seq[InputPartition]].
      // Each inner Seq represents a partition slot: empty means None, single means Some(p).
      // The inner Seq is expected to have 0 or 1 element (GroupPartitionsExec handles
      // coalescing in Spark 4.x), so we safely take the head.
      val optionPartitions: Seq[Option[InputPartition]] = filteredPartitions.map {
        case seq if seq.isEmpty => None
        case seq =>
          assert(
            seq.length == 1,
            "Expected 0 or 1 partition per slot; multi-element " +
              "partitions should be handled by GroupPartitionsExec")
          Some(seq.head)
      }
      new DataSourceRDD(
        sparkContext,
        optionPartitions,
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
