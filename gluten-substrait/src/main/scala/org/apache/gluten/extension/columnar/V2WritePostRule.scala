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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.ColumnarV2TableWriteExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class V2WritePostRule() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case write: ColumnarV2TableWriteExec =>
      /**
       * If the columnar write's child is aqe, we make aqe "support columnar", then aqe itself will
       * guarantee to generate columnar outputs. thus avoiding the case of c2r->aqe->r2c->writer.
       */
      ensureAQESupportsColumnar(write.query)
        .map(write.withNewQuery).getOrElse(write)

    case v2Command: V2CommandExec =>
      /**
       * For V2CommandExec (e.g., WriteFilesExec for planned writes), ensure its child AQE
       * supports columnar. This is needed for proper shuffle cleanup tracking (SPARK-53413).
       */
      val newChildren = v2Command.children.map(ensureAQESupportsColumnar)
      if (newChildren.forall(_.isEmpty)) {
        v2Command
      } else {
        v2Command.withNewChildren(newChildren.flatten)
      }

    case other => other
  }

  private def ensureAQESupportsColumnar(plan: SparkPlan): Option[SparkPlan] = {
    plan match {
      case aqe: AdaptiveSparkPlanExec if !aqe.supportsColumnar =>
        Some(aqe.copy(supportsColumnar = true))
      case _ =>
        None
    }
  }
}
