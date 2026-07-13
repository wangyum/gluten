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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Provides a common executor to translate an [[RDD]] of [[InternalRow]] into an [[RDD]] of
 * [[ColumnarBatch]]. This is inserted whenever such a transition is determined to be needed.
 */
abstract class RowToColumnarExecBase(child: SparkPlan)
  extends RowToColumnarTransition
  with GlutenPlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genRowToColumnarMetrics(sparkContext)

  final override def output: Seq[Attribute] = child.output

  final override def outputPartitioning: Partitioning = {
    child match {
      case scan: org.apache.spark.sql.execution.datasources.v2.BatchScanExec
          if scan.keyGroupedPartitioning.isDefined &&
            KeyedPartitioning.supportsExpressions(scan.keyGroupedPartitioning.get) &&
            scan.inputPartitions.nonEmpty &&
            scan.inputPartitions.forall(
              _.isInstanceOf[org.apache.spark.sql.connector.read.HasPartitionKey]) =>
        val expressions = scan.keyGroupedPartitioning.get
        val hasPartitionKeyPartitions =
          scan.inputPartitions.map(
            _.asInstanceOf[org.apache.spark.sql.connector.read.HasPartitionKey])
        val dataTypes = expressions.map(_.dataType)
        val keyRowOrdering =
          org.apache.spark.sql.catalyst.expressions.RowOrdering.createNaturalAscendingOrdering(
            dataTypes)
        val partitionKeys = hasPartitionKeyPartitions.map(_.partitionKey())
        try {
          val sorted = partitionKeys.sorted(keyRowOrdering)
          return KeyedPartitioning(expressions, sorted)
        } catch {
          case _: Exception =>
          // sorted or KeyedPartitioning.apply threw (e.g. ClassCastException
          // when expression data types and partition key values have mismatched
          // ordering). Fall back to a KeyedPartitioning bypassing .sorted
          // and .distinct, preserving the unsorted partition keys.
        }
        val wrapperFactory =
          InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(dataTypes)
        new KeyedPartitioning(expressions, partitionKeys.map(wrapperFactory), isGrouped = true)
      case _ =>
        child.outputPartitioning
    }
  }

  final override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override def requiredChildConvention(): Seq[ConventionReq] = {
    Seq(ConventionReq.vanillaRow)
  }

  final override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    // Require for explicit implementation, otherwise throw error.
    super.doExecuteBroadcast[T]()
  }

  def doExecuteColumnarInternal(): RDD[ColumnarBatch]

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }
}
