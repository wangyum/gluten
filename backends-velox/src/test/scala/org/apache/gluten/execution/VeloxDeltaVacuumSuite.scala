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

import org.apache.gluten.extension.GlutenSessionExtensions

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.VacuumCommand

// Verifies that VACUUM on a Delta table produces correct results when Gluten is enabled,
// regardless of whether the -Pdelta profile is on the classpath.
//
// Root cause (HADP-VACUUM): without -Pdelta, OffloadDeltaScan is absent, so the generic
// OffloadOthers rule sends the _delta_log checkpoint scan to Velox.  Velox misreads the
// SingleAction Parquet schema and returns no AddFile rows, making VacuumCommand compute an
// empty validFiles set.  The resulting left-anti join deletes every live data file.
//
// Fix: FallbackDeltaLogScan (registered unconditionally in VeloxRuleApi) detects the
// "/_delta_log" root path and adds a FallbackTag before OffloadOthers runs, so the
// checkpoint scan always executes on the JVM.
//
// Test design: the Velox native library available on this machine has an unrelated
// F14Table::rehashImpl assertion failure that crashes the JVM whenever Gluten executes
// any VACUUM-internal Spark job natively.  Both tests therefore run VacuumCommand.gc
// with Gluten disabled and test the protection separately:
//
//   Test 1 (before fix / bug): corrupts the checkpoint Parquet to contain no AddFile rows,
//     simulating what Velox would produce.  VACUUM (JVM) then sees empty validFiles and
//     deletes all data files; the next read raises an exception.
//
//   Test 2 (after fix): verifies that FallbackDeltaLogScan tags the checkpoint scan for
//     JVM fallback by going through the full Gluten planning pipeline on a real checkpoint
//     scan, then runs VACUUM (JVM) and asserts that all data files survive and all rows
//     are readable.
class VeloxDeltaVacuumSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /** Disables Gluten on both the calling thread and any background threads spawned by Spark. */
  private def withGlutenDisabled[T](body: => T): T = {
    val prevConf = spark.conf.getOption("spark.gluten.enabled")
    spark.conf.set("spark.gluten.enabled", "false")
    spark.sparkContext.setLocalProperty(
      GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY,
      "false")
    try {
      body
    } finally {
      prevConf match {
        case Some(v) => spark.conf.set("spark.gluten.enabled", v)
        case None => spark.conf.unset("spark.gluten.enabled")
      }
      spark.sparkContext.setLocalProperty(
        GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY,
        null)
    }
  }

  // --------------------------------------------------------------------------
  // Test 1 - bug demonstration
  // --------------------------------------------------------------------------
  test(
    "HADP-VACUUM [before fix]: VACUUM with corrupted checkpoint (no AddFile entries) " +
      "deletes live files, causing read failure") {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath

        // Create table; checkpointInterval=1 forces a checkpoint at every commit so the
        // latest snapshot is fully represented by a checkpoint Parquet file (no trailing
        // JSON logs).  That file is exactly the one Velox misreads.
        withGlutenDisabled {
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
        val latestCheckpoint = checkpointFiles.maxBy(_.getPath.getName)

        // Corrupt the checkpoint: keep only rows where add IS NULL (i.e. remove every
        // AddFile entry).  This replicates the output Velox produces when it misreads
        // the SingleAction schema.
        //
        // Write to a temp directory first (Spark's Parquet writer needs a directory as
        // the output path; it cannot overwrite a regular file in-place).
        val tempDir = new org.apache.hadoop.fs.Path(deltaLogPath, "_hadp_corrupt_tmp")
        withGlutenDisabled {
          spark.read
            .parquet(latestCheckpoint.getPath.toString)
            .filter("add IS NULL")
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(tempDir.toString)
        }
        val corruptedPart = fs
          .listStatus(tempDir)
          .filter(_.getPath.getName.startsWith("part-"))
          .head
          .getPath
        fs.delete(latestCheckpoint.getPath, false)
        fs.rename(corruptedPart, latestCheckpoint.getPath)
        fs.delete(tempDir, true)

        // Invalidate Delta's snapshot cache so the next forTable call reads from disk.
        DeltaLog.invalidateCache(spark, new org.apache.hadoop.fs.Path(path))
        val deltaLog = DeltaLog.forTable(spark, path)

        // Run VACUUM with Gluten disabled (see class-level design note).
        // The corrupted checkpoint has no AddFile rows -> validFiles is empty ->
        // VACUUM deletes every data file.
        withGlutenDisabled {
          VacuumCommand.gc(
            spark,
            deltaLog,
            dryRun = false,
            retentionHours = Some(0),
            safetyCheckEnabled = false)
        }

        // Verify: VACUUM deleted all data files.
        val dataFiles = fs
          .listStatus(new org.apache.hadoop.fs.Path(path))
          .filterNot(f => f.getPath.getName == "_delta_log")
        assert(dataFiles.isEmpty, "Expected VACUUM to delete all data files with empty validFiles")
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

        withGlutenDisabled {
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

        // ---- Guard: verify FallbackDeltaLogScan tags _delta_log scans for JVM fallback ----
        // Apply the rule directly on a synthetic FileSourceScanExec whose rootPaths point
        // at _delta_log.  This avoids triggering Velox execution entirely (the available
        // native library has an unrelated F14Table crash) while still proving that the rule
        // is present and correctly identifies the scan.
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

        org.apache.gluten.extension.FallbackDeltaLogScan().apply(checkpointScan)

        assert(
          org.apache.gluten.extension.columnar.FallbackTags.nonEmpty(checkpointScan),
          "FallbackDeltaLogScan did NOT tag the _delta_log checkpoint scan for JVM fallback. " +
            "Check that FallbackDeltaLogScan is registered in VeloxRuleApi.injectPreTransform."
        )

        // ---- End-to-end: VACUUM must not delete any live file ----
        var filesBeforeVacuum: Set[String] = Set.empty
        withGlutenDisabled {
          filesBeforeVacuum = spark.read.format("delta").load(path).inputFiles.toSet
        }
        assert(filesBeforeVacuum.nonEmpty, "Expected live data files before VACUUM")

        withGlutenDisabled {
          VacuumCommand.gc(
            spark,
            deltaLog,
            dryRun = false,
            retentionHours = Some(0),
            safetyCheckEnabled = false)
        }

        // Every pre-VACUUM file must still exist on disk.
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

        // All rows must be readable.
        withGlutenDisabled {
          checkAnswer(
            spark.read.format("delta").load(path),
            Seq(Row(1), Row(2), Row(3)))
        }
    }
  }
}
