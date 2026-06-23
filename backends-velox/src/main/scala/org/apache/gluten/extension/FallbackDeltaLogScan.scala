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
package org.apache.gluten.extension

import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

/**
 * Forces any FileSourceScanExec that reads from the Delta transaction log (_delta_log) to fall back
 * to JVM execution.
 *
 * Without this rule, when the Gluten package is built without -Pdelta, the generic OffloadOthers
 * rule converts _delta_log checkpoint scans (e.g. those issued by VACUUM) to native Velox scans.
 * Velox misreads the SingleAction Parquet schema and returns no AddFile rows, causing VacuumCommand
 * to compute an empty validFiles set and delete every live data file. This rule is registered
 * unconditionally in VeloxRuleApi so that the protection is present regardless of whether the
 * gluten-delta module (-Pdelta) is on the classpath.
 *
 * Detection: Delta always uses DeltaLogFileIndex as the FileIndex for all transaction log scans
 * (both checkpoint Parquet and JSON commit files). The class name check avoids a compile-time
 * dependency on delta-spark, so this rule compiles and runs regardless of whether -Pdelta is
 * active.
 *
 * Test-only control: set spark.gluten.test.fallbackDeltaLogScan.enabled=false to disable this rule
 * for negative-path regression tests.
 */
object FallbackDeltaLogScan {
  val TEST_FALLBACK_DELTA_LOG_SCAN_ENABLED: String =
    "spark.gluten.test.fallbackDeltaLogScan.enabled"
}

case class FallbackDeltaLogScan() extends Rule[SparkPlan] {
  private val deltaLogFileIndexClassName = "org.apache.spark.sql.delta.DeltaLogFileIndex"

  override def apply(plan: SparkPlan): SparkPlan = {
    val ruleEnabled = SparkSession
      .getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .forall(
        _.sessionState.conf
          .getConfString(FallbackDeltaLogScan.TEST_FALLBACK_DELTA_LOG_SCAN_ENABLED, "true")
          .toBoolean)
    if (!ruleEnabled) {
      return plan
    }

    plan.foreach {
      case scan: FileSourceScanExec
          if scan.relation.location.getClass.getName == deltaLogFileIndexClassName =>
        FallbackTags.add(scan, "Delta _delta_log scan must run on JVM")
      case _ =>
    }
    plan
  }
}
