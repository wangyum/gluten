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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.VacuumCommand

// Verifies that VACUUM on a Delta table produces correct results when Gluten is enabled
// and built WITHOUT -Pdelta.
//
// Root cause (HADP-VACUUM): without -Pdelta, OffloadDeltaScan is absent, so the generic
// OffloadOthers rule sends the _delta_log checkpoint scan to Velox.  Velox misreads the
// SingleAction Parquet schema and returns no AddFile rows, making VacuumCommand compute an
// empty validFiles set.  The resulting left-anti join deletes every live data file.
//
// Fix: FallbackDeltaLogScan (registered unconditionally in VeloxRuleApi) detects any
// FileSourceScanExec whose FileIndex is a DeltaLogFileIndex and adds a FallbackTag before
// OffloadOthers runs, so the checkpoint scan always executes on the JVM.
//
//   Test 1 (bug scenario): disables FallbackDeltaLogScan via a test-only config and runs
//     VACUUM normally.  The _delta_log checkpoint scan is offloaded, validFiles is computed
//     incorrectly, VACUUM deletes live files, and the next read fails with FileNotFound.
//
//   Test 2 (fix verification): verifies that FallbackDeltaLogScan tags the checkpoint
//     scan for JVM fallback and blocks OffloadOthers from native-converting the
//     DeltaLogFileIndex scan, then runs VACUUM end-to-end with Gluten enabled and
//     asserts that all data files survive and all rows are readable.
//
// Note: Delta table writes and final assertions use withSQLConf("spark.gluten.enabled" -> "false")
// because Velox crashes on the Delta write path without -Pdelta (a_precision undefined in
// Velox's type_calculation).  Only VacuumCommand.gc runs with Gluten enabled, which is the
// exact operation the fix targets.
class VeloxDeltaVacuumSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.gluten.forceEnabled", "true")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // UDF example libs are not present in CI artifacts; disable to avoid SparkContext crash.
      .set("velox.udf.lib.path", "")
  }

  private def containsFileNotFound(t: Throwable): Boolean = {
    @scala.annotation.tailrec
    def loop(current: Throwable): Boolean = {
      if (current == null) {
        false
      } else {
        val className = current.getClass.getName
        val message = Option(current.getMessage).getOrElse("")
        if (className.contains("FileNotFound") || message.contains("FileNotFound")) {
          true
        } else {
          loop(current.getCause)
        }
      }
    }

    loop(t)
  }

  private def toQualifiedPath(
      tablePath: String,
      filePath: String,
      hadoopConf: org.apache.hadoop.conf.Configuration): String = {
    val p = new org.apache.hadoop.fs.Path(filePath)
    val abs = if (p.isAbsolute) p else new org.apache.hadoop.fs.Path(tablePath, filePath)
    abs.getFileSystem(hadoopConf).makeQualified(abs).toString
  }

  private def normalizePath(path: String): String = {
    new org.apache.hadoop.fs.Path(path).toUri.getPath
  }

  private def withGlutenThreadEnabled[T](f: => T): T = {
    val key = org.apache.gluten.extension.GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY
    val previous = spark.sparkContext.getLocalProperty(key)
    spark.sparkContext.setLocalProperty(key, "true")
    try {
      f
    } finally {
      spark.sparkContext.setLocalProperty(key, previous)
    }
  }

  private def withFallbackDeltaLogScanEnabled[T](enabled: Boolean)(f: => T): T = {
    val key = org.apache.gluten.extension.FallbackDeltaLogScan.TEST_FALLBACK_DELTA_LOG_SCAN_ENABLED
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, enabled.toString)
    try {
      f
    } finally {
      previous match {
        case Some(value) => spark.conf.set(key, value)
        case None => spark.conf.unset(key)
      }
    }
  }

  private def snapshotLiveFiles(
      tablePath: String,
      deltaLog: DeltaLog,
      hadoopConf: org.apache.hadoop.conf.Configuration): Set[String] = {
    var liveFiles = Set.empty[String]
    withSQLConf(("spark.gluten.enabled", "false")) {
      liveFiles = deltaLog
        .update()
        .allFiles
        .collect()
        .map(addFile => toQualifiedPath(tablePath, addFile.path, hadoopConf))
        .toSet
    }
    liveFiles
  }

  private def buildCheckpointScan(
      checkpointFiles: Array[org.apache.hadoop.fs.FileStatus],
      hadoopConf: org.apache.hadoop.conf.Configuration)
      : org.apache.spark.sql.execution.FileSourceScanExec = {
    val checkpointIndex = org.apache.spark.sql.delta.DeltaLogFileIndex(
      org.apache.spark.sql.delta.DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET,
      checkpointFiles.toSeq
    ).getOrElse(fail("Could not build DeltaLogFileIndex from checkpoint files"))

    val checkpointSchema: org.apache.spark.sql.types.StructType = {
      val cpPath = checkpointFiles.head.getPath
      val footer = org.apache.parquet.hadoop.ParquetFileReader.open(
        org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(cpPath, hadoopConf))
      try {
        new org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter()
          .convert(footer.getFileMetaData.getSchema)
      } finally {
        footer.close()
      }
    }

    val relation = org.apache.spark.sql.execution.datasources.HadoopFsRelation(
      location = checkpointIndex,
      partitionSchema = org.apache.spark.sql.types.StructType(Nil),
      dataSchema = checkpointSchema,
      bucketSpec = None,
      fileFormat =
        new org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat(),
      options = Map.empty
    )(spark)
    val output = relation.schema.map {
      f =>
        org.apache.spark.sql.catalyst.expressions
          .AttributeReference(f.name, f.dataType, f.nullable)()
    }

    org.apache.spark.sql.execution.FileSourceScanExec(
      relation,
      output,
      relation.dataSchema,
      partitionFilters = Nil,
      optionalBucketSet = None,
      optionalNumCoalescedBuckets = None,
      dataFilters = Nil,
      tableIdentifier = None
    )
  }

  // --------------------------------------------------------------------------
  // Test 1 - bug scenario (FallbackDeltaLogScan disabled)
  // --------------------------------------------------------------------------
  test(
    "HADP-VACUUM [before fix]: disabling FallbackDeltaLogScan causes VACUUM to delete " +
      "live files and next read fails") {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath

        withGlutenThreadEnabled {
          // Create table and write data with Gluten disabled: Velox does not handle the Delta
          // write path without -Pdelta (a_precision undefined in Velox type calculation).
          withSQLConf(("spark.gluten.enabled", "false")) {
            spark.sql(s"""
                         |CREATE TABLE delta.`$path` (id INT) USING delta
                         |TBLPROPERTIES ('delta.checkpointInterval' = '1')
                         |""".stripMargin)
            Seq(1).toDF("id").write.format("delta").mode("append").save(path)
            Seq(2).toDF("id").write.format("delta").mode("append").save(path)
            Seq(3).toDF("id").write.format("delta").mode("append").save(path)
          }

          val hadoopConf = spark.sessionState.newHadoopConf()
          val deltaLogPath = new org.apache.hadoop.fs.Path(path, "_delta_log")
          val fs = deltaLogPath.getFileSystem(hadoopConf)

          val checkpointFiles = fs
            .listStatus(deltaLogPath)
            .filter(
              f =>
                f.getPath.getName.contains("checkpoint") &&
                  f.getPath.getName.endsWith(".parquet"))
          assume(
            checkpointFiles.nonEmpty,
            "No checkpoint Parquet found; checkpointInterval=1 should produce one per commit.")
          // Capture live files from the uncorrupted snapshot; these are the files VACUUM must
          // never delete.
          DeltaLog.invalidateCache(spark, new org.apache.hadoop.fs.Path(path))
          val baselineDeltaLog = DeltaLog.forTable(spark, path)
          val liveFilesBefore = snapshotLiveFiles(path, baselineDeltaLog, hadoopConf)
          assert(
            liveFilesBefore.nonEmpty,
            "Expected live data files before running VACUUM with fallback rule disabled")

          // Invalidate Delta's snapshot cache so the next forTable call reads from disk.
          DeltaLog.invalidateCache(spark, new org.apache.hadoop.fs.Path(path))
          val deltaLog = DeltaLog.forTable(spark, path)

          val checkpointScan = buildCheckpointScan(checkpointFiles, hadoopConf)
          val offloadOthers = org.apache.gluten.extension.columnar.offload.OffloadOthers()

          // Verify the config really disables the rule in this test.
          withFallbackDeltaLogScanEnabled(enabled = false) {
            org.apache.gluten.extension.FallbackDeltaLogScan().apply(checkpointScan)
            assert(
              !org.apache.gluten.extension.columnar.FallbackTags.nonEmpty(checkpointScan),
              "FallbackDeltaLogScan should be disabled by test config but it still added tags."
            )
          }

          val offloadedWithoutFallback = offloadOthers.offload(checkpointScan)
          assert(
            offloadedWithoutFallback
              .isInstanceOf[org.apache.gluten.execution.FileSourceScanExecTransformerBase],
            "Expected OffloadOthers to convert DeltaLogFileIndex scan to native when " +
              "FallbackDeltaLogScan is disabled."
          )

          // Run VACUUM with Gluten enabled and FallbackDeltaLogScan disabled. This is the
          // real no-fix path: checkpoint scan can be offloaded and live files may be deleted.
          withFallbackDeltaLogScanEnabled(enabled = false) {
            VacuumCommand.gc(
              spark,
              deltaLog,
              dryRun = false,
              retentionHours = Some(0),
              safetyCheckEnabled = false)
          }

          val deletedLiveFiles = liveFilesBefore.filter {
            file =>
              val fp = new org.apache.hadoop.fs.Path(file)
              !fp.getFileSystem(hadoopConf).exists(fp)
          }
          assert(
            deletedLiveFiles.nonEmpty,
            "Expected VACUUM to delete at least one live file when FallbackDeltaLogScan is " +
              "disabled, but no files were deleted."
          )
          val deletedLiveFilesNormalized = deletedLiveFiles.map(normalizePath)

          // Deleted files must still be referenced by Delta metadata, matching production:
          // metadata points to files that no longer exist on storage.
          DeltaLog.invalidateCache(spark, new org.apache.hadoop.fs.Path(path))
          val refreshedDeltaLog = DeltaLog.forTable(spark, path)
          val trackedFilesAfterRestore = snapshotLiveFiles(path, refreshedDeltaLog, hadoopConf)
          val trackedFilesAfterRestoreNormalized = trackedFilesAfterRestore.map(normalizePath)
          assert(
            deletedLiveFilesNormalized.exists(trackedFilesAfterRestoreNormalized.contains),
            "Expected Delta metadata to still reference at least one deleted live file, " +
              "but none of the deleted files remained in snapshot metadata."
          )

          DeltaLog.invalidateCache(spark, new org.apache.hadoop.fs.Path(path))
          val readError = intercept[Exception] {
            withSQLConf(("spark.gluten.enabled", "false")) {
              spark.read.format("delta").load(path).collect()
            }
          }
          assert(
            containsFileNotFound(readError),
            "Expected FileNotFound-style read failure after VACUUM deleted live files, " +
              s"but got ${readError.getClass.getName}: " +
              s"${Option(readError.getMessage).getOrElse("")}"
          )
        }
    }
  }

  // --------------------------------------------------------------------------
  // Test 2 - fix verification
  // --------------------------------------------------------------------------
  test(
    "HADP-VACUUM [after fix]: FallbackDeltaLogScan ensures checkpoint scan runs on JVM " +
      "so VACUUM preserves all live files") {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath

        withGlutenThreadEnabled {
          withSQLConf(("spark.gluten.enabled", "false")) {
            spark.sql(s"""
                         |CREATE TABLE delta.`$path` (id INT) USING delta
                         |TBLPROPERTIES ('delta.checkpointInterval' = '1')
                         |""".stripMargin)
            Seq(1).toDF("id").write.format("delta").mode("append").save(path)
            Seq(2).toDF("id").write.format("delta").mode("append").save(path)
            Seq(3).toDF("id").write.format("delta").mode("append").save(path)
          }

          val hadoopConf = spark.sessionState.newHadoopConf()
          val deltaLog = DeltaLog.forTable(spark, path)
          val deltaLogPath = new org.apache.hadoop.fs.Path(path, "_delta_log")
          val fs = deltaLogPath.getFileSystem(hadoopConf)
          val checkpointFiles = fs
            .listStatus(deltaLogPath)
            .filter(
              f =>
                f.getPath.getName.contains("checkpoint") &&
                  f.getPath.getName.endsWith(".parquet"))
          assume(
            checkpointFiles.nonEmpty,
            "No checkpoint Parquet found; checkpointInterval=1 should produce one per commit.")

          // ---- Guard: verify FallbackDeltaLogScan tags DeltaLogFileIndex scans ----
          val checkpointIndex = org.apache.spark.sql.delta.DeltaLogFileIndex(
            org.apache.spark.sql.delta.DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET,
            checkpointFiles.toSeq
          ).getOrElse(fail("Could not build DeltaLogFileIndex from checkpoint files"))

          val checkpointSchema: org.apache.spark.sql.types.StructType = {
            val cpPath = checkpointFiles.head.getPath
            val footer = org.apache.parquet.hadoop.ParquetFileReader.open(
              org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(cpPath, hadoopConf))
            try {
              new org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter()
                .convert(footer.getFileMetaData.getSchema)
            } finally {
              footer.close()
            }
          }
          val relation = org.apache.spark.sql.execution.datasources.HadoopFsRelation(
            location = checkpointIndex,
            partitionSchema = org.apache.spark.sql.types.StructType(Nil),
            dataSchema = checkpointSchema,
            bucketSpec = None,
            fileFormat =
              new org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat(),
            options = Map.empty
          )(spark)
          val output = relation.schema.map {
            f =>
              org.apache.spark.sql.catalyst.expressions
                .AttributeReference(f.name, f.dataType, f.nullable)()
          }
          val checkpointScan = org.apache.spark.sql.execution.FileSourceScanExec(
            relation,
            output,
            relation.dataSchema,
            partitionFilters = Nil,
            optionalBucketSet = None,
            optionalNumCoalescedBuckets = None,
            dataFilters = Nil,
            tableIdentifier = None
          )

          // Root-cause guard: without a fallback tag, OffloadOthers offloads this
          // DeltaLogFileIndex scan to a native FileSourceScanExecTransformer.
          val offloadOthers = org.apache.gluten.extension.columnar.offload.OffloadOthers()
          val offloadedWithoutFallback = offloadOthers.offload(checkpointScan)
          assert(
            offloadedWithoutFallback
              .isInstanceOf[org.apache.gluten.execution.FileSourceScanExecTransformerBase],
            "Expected OffloadOthers to convert DeltaLogFileIndex scan to native when no " +
              "fallback tag exists. If this changes, re-check HADP-VACUUM assumptions."
          )

          org.apache.gluten.extension.FallbackDeltaLogScan().apply(checkpointScan)

          assert(
            org.apache.gluten.extension.columnar.FallbackTags.nonEmpty(checkpointScan),
            "FallbackDeltaLogScan did NOT tag the _delta_log checkpoint scan for JVM fallback. " +
              "Check that FallbackDeltaLogScan is registered in VeloxRuleApi.injectPreTransform."
          )

          val offloadedWithFallback = offloadOthers.offload(checkpointScan)
          assert(
            offloadedWithFallback eq checkpointScan,
            "OffloadOthers still transformed a checkpoint scan after FallbackDeltaLogScan " +
              "added fallback tags. This re-opens HADP-VACUUM data-loss risk."
          )

          // ---- End-to-end: VACUUM with Gluten enabled must not delete any live file ----
          val filesBeforeVacuum = snapshotLiveFiles(path, deltaLog, hadoopConf)
          assert(filesBeforeVacuum.nonEmpty, "Expected live data files before VACUUM")

          VacuumCommand.gc(
            spark,
            deltaLog,
            dryRun = false,
            retentionHours = Some(0),
            safetyCheckEnabled = false)

          filesBeforeVacuum.foreach {
            file =>
              val fp = new org.apache.hadoop.fs.Path(file)
              assert(
                fp.getFileSystem(hadoopConf).exists(fp),
                s"VACUUM deleted live file $file. " +
                  "FallbackDeltaLogScan should have kept the checkpoint scan on JVM " +
                  "so validFiles was computed correctly."
              )
          }

          withSQLConf(("spark.gluten.enabled", "false")) {
            checkAnswer(
              spark.read.format("delta").load(path),
              Seq(Row(1), Row(2), Row(3)))
          }
        }
    }
  }
}
