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
package org.apache.spark.sql.hive.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

import scala.collection.immutable.Seq

class GlutenHiveSQLQuerySuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  testGluten("hive orc scan") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name STRING, favorite_color STRING)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test_1', 'red')")
      val df = spark.sql("select * from test_orc")
      checkAnswer(df, Seq(Row("test_1", "red")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  // copy from GLUTEN-4796, which only added to spark33
  testGluten("Add orc char type validation") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name char(10), id int)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test', 1)")
    }

    def testExecPlan(
        convertMetastoreOrc: String,
        charTypeFallbackEnabled: String,
        shouldFindTransformer: Boolean,
        transformerClass: Class[_ <: SparkPlan]
    ): Unit = {

      withSQLConf(
        "spark.sql.hive.convertMetastoreOrc" -> convertMetastoreOrc,
        GlutenConfig.VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK.key -> charTypeFallbackEnabled
      ) {
        val queries = Seq("select id from test_orc", "select name, id from test_orc")

        queries.foreach {
          query =>
            val executedPlan = getExecutedPlan(spark.sql(query))
            val planCondition = executedPlan.exists(_.find(transformerClass.isInstance).isDefined)

            if (shouldFindTransformer) {
              assert(planCondition)
            } else {
              assert(!planCondition)
            }
        }
      }
    }

    testExecPlan(
      "false",
      "true",
      shouldFindTransformer = false,
      classOf[HiveTableScanExecTransformer])
    testExecPlan(
      "false",
      "false",
      shouldFindTransformer = true,
      classOf[HiveTableScanExecTransformer])

    testExecPlan(
      "true",
      "true",
      shouldFindTransformer = false,
      classOf[FileSourceScanExecTransformer])
    testExecPlan(
      "true",
      "false",
      shouldFindTransformer = true,
      classOf[FileSourceScanExecTransformer])
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("orc.force.positional.evolution maps Hive ORC columns by position") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      withTempDir {
        dir =>
          val orcLoc = s"file:///$dir/test_orc_pos"
          withTable("test_orc_pos", "test_orc_pos_renamed") {
            // Write ORC files whose physical column names are c1, c2 (c1 = 1, c2 = 2).
            sql(
              s"CREATE TABLE test_orc_pos(c1 int, c2 int) " +
                s"USING hive OPTIONS(fileFormat 'orc') LOCATION '$orcLoc'")
            sql("INSERT INTO test_orc_pos SELECT 1, 2")

            // A second table over the SAME files but with mismatched column names (x, y).
            // By name, x/y are not present in the files; only position mapping can read them.
            sql(
              s"CREATE TABLE test_orc_pos_renamed(x int, y int) " +
                s"USING hive OPTIONS(fileFormat 'orc') LOCATION '$orcLoc'")

            // orc.force.positional.evolution=true => read by position: x -> c1 (=1), y -> c2 (=2).
            withSQLConf("spark.hadoop.orc.force.positional.evolution" -> "true") {
              val df = sql("select x, y from test_orc_pos_renamed")
              checkAnswer(df, Seq(Row(1, 2)))
              checkOperatorMatch[HiveTableScanExecTransformer](df)
            }
          }
      }
    }
  }

  test("GLUTEN-11062: Supports mixed input format for partitioned Hive table") {
    withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
      withTempDir {
        dir =>
          val parquetLoc = s"file:///$dir/test_parquet"
          val orcLoc = s"file:///$dir/test_orc"
          withTable("test_parquet", "test_orc") {
            sql(s"""CREATE TABLE test_parquet(id int)
                 USING hive OPTIONS(fileFormat 'parquet')
                 PARTITIONED BY(pid int)
                 LOCATION '$parquetLoc'""")
            sql("INSERT INTO test_parquet PARTITION(pid=1) SELECT 2")
            sql(s"""CREATE TABLE test_orc(id int)
                 USING hive OPTIONS(fileFormat 'orc')
                 PARTITIONED BY(pid int)
                 LOCATION '$orcLoc'""")
            sql("INSERT INTO test_orc PARTITION(pid=2) SELECT 2")
            sql(s"ALTER TABLE test_parquet ADD PARTITION (pid=2) LOCATION '$orcLoc/pid=2'")
            sql("ALTER TABLE test_parquet PARTITION(pid=2) SET FILEFORMAT orc")
            val df = sql("select pid, id from test_parquet order by pid")
            checkAnswer(df, Seq(Row(1, 2), Row(2, 2)))
            checkOperatorMatch[HiveTableScanExecTransformer](df)
          }
      }
    }
  }
}
