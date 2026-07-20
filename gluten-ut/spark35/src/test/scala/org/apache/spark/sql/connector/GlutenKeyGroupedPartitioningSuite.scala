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
package org.apache.spark.sql.connector

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ShuffledHashJoinExecTransformerBase, SortExecTransformer, SortMergeJoinExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.SortedMergeCoalescedRDD
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.{FieldReference, NullOrdering, SortDirection, Transform}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, days, identity, sort, years}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Collections

class GlutenKeyGroupedPartitioningSuite
  extends KeyGroupedPartitioningSuite
  with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf = {
    // Native SQL configs
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "5")
  }

  override def collectAllShuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] = {
    collect(plan) {
      case s: ColumnarShuffleExchangeExec => s
      case s: ShuffleExchangeExec => s
    }
  }

  override def collectShuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    (collect(plan) {
      case s: SortMergeJoinExecTransformer => s
      case s: SortMergeJoinExec => s
    }).flatMap(
      smj =>
        collect(smj) {
          case s: ColumnarShuffleExchangeExec => s
          case s: ShuffleExchangeExec => s
        }).toSet.toSeq
  }

  override protected def collectSMJs(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case s: SortMergeJoinExecTransformer => s
      case s: SortMergeJoinExec => s
    }
  }

  override protected def collectSortsForSMJ(smj: SparkPlan): Seq[SparkPlan] = {
    smj.children.flatMap(
      child =>
        collect(child) {
          case s: SortExecTransformer => s
          case s: SortExec => s
        })
  }

  override protected def collectGroupPartitions(plan: SparkPlan): Seq[GroupPartitionsExec] = {
    // here we skip collecting group partitions that are not associated with SMJ
    (collect(plan) {
      case s: SortMergeJoinExecTransformer => s
      case s: SortMergeJoinExec => s
    }).flatMap(
      smj =>
        collect(smj) {
          case g: GroupPartitionsExec => g
        }).toSet.toSeq
  }

  private val emptyProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }
  private def createTable(
      table: String,
      schema: StructType,
      partitions: Array[Transform],
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(
      Identifier.of(Array("ns"), table),
      schema,
      partitions,
      emptyProps,
      Distributions.unspecified(),
      Array.empty,
      None,
      None,
      numRowsPerSplit = 1)
  }

  private def collectColumnarShuffleExchangeExec(
      plan: SparkPlan): Seq[ColumnarShuffleExchangeExec] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExecTransformer => s
      case s: SortMergeJoinExec => s
    }.flatMap(smj => collect(smj) { case s: ColumnarShuffleExchangeExec => s })
  }

  private val customers: String = "customers"
  private val customers_schema = new StructType()
    .add("customer_name", StringType)
    .add("customer_age", IntegerType)
    .add("customer_id", LongType)

  private val orders: String = "orders"
  private val orders_schema = new StructType()
    .add("order_amount", DoubleType)
    .add("customer_id", LongType)

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      orders_partitions: Array[Transform],
      expectedNumOfShuffleExecs: Int): Unit = {
    createTable(customers, customers_schema, customers_partitions)
    sql(
      s"INSERT INTO testcat.ns.$customers VALUES " +
        s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

    createTable(orders, orders_schema, orders_partitions)
    sql(
      s"INSERT INTO testcat.ns.$orders VALUES " +
        s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

    val df = sql(
      "SELECT customer_name, customer_age, order_amount " +
        s"FROM testcat.ns.$customers c JOIN testcat.ns.$orders o " +
        "ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.length == expectedNumOfShuffleExecs)

    checkAnswer(
      df,
      Seq(
        Row("aaa", 10, 100.0),
        Row("aaa", 10, 200.0),
        Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0),
        Row("bbb", 20, 350.0),
        Row("ccc", 30, 400.50)))
  }

  testGluten("partitioned join: only one side reports partitioning") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(2, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 2)
  }
  testGluten("partitioned join: exact distribution (same number of buckets) from both sides") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 0)
  }

  private val items: String = "items"
  private def selectWithMergeJoinHint(t1: String, t2: String): String = {
    s"SELECT /*+ MERGE($t1, $t2) */ "
  }
  private val items_schema: StructType = new StructType()
    .add("id", LongType)
    .add("name", StringType)
    .add("price", FloatType)
    .add("arrive_time", TimestampType)
  private val purchases: String = "purchases"
  private val purchases_schema: StructType = new StructType()
    .add("item_id", LongType)
    .add("price", FloatType)
    .add("time", TimestampType)

  testGluten(
    "SPARK-41413: partitioned join: partition values" +
      " from one side are subset of those from the other side") {
    val items_partitions = Array(bucket(4, "id"))
    createTable(items, items_schema, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(4, "item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)

    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
        }
    }
  }

  testGluten("SPARK-41413: partitioned join: partition values from both sides overlaps") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 19.5, cast('2020-02-01' as timestamp)), " +
        "(4, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 19.5)))
        }
    }
  }

  testGluten("SPARK-41413: partitioned join: non-overlapping partition values from both sides") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(4, 42.0, cast('2020-01-01' as timestamp)), " +
        "(5, 19.5, cast('2020-02-01' as timestamp)), " +
        "(6, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq.empty)
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered:" +
      " with same partition keys and one side fully clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 3)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.isEmpty, "should not contain any shuffle")
              if (pushDownValues) {
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.forall(_.outputPartitioning.numPartitions == expected))
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered:" +
      " with same partition keys and both sides partially clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 22.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 7), ("false", 3)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.isEmpty, "should not contain any shuffle")
              if (pushDownValues) {
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.forall(_.outputPartitioning.numPartitions == expected))
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 40.0, 55.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(1, "aa", 41.0, 55.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 10.0, 22.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered: with different" +
      " partition keys and both sides partially clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 10), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.forall(_.outputPartitioning.numPartitions == expected))
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 40.0, 55.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(1, "aa", 41.0, 55.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 10.0, 25.0),
                  Row(2, "bb", 10.0, 30.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered: with different" +
      " partition keys and missing keys on left-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 9), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.forall(_.outputPartitioning.numPartitions == expected))
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered:" +
      " with different partition keys and missing keys on right-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 6), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.forall(_.outputPartitioning.numPartitions == expected))
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten("SPARK-42038: partially clustered: left outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 15.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // In a left-outer join, and when the left side has larger stats, partially clustered
    // distribution should kick in and pick the right hand side to replicate partitions.
    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 7), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i LEFT JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(
                  groupPartitions.forall(_.outputPartitioning.numPartitions == expected),
                  s"Expected $expected but got " +
                    s"${groupPartitions.headOption.map(_.outputPartitioning.numPartitions)}"
                )
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, null),
                  Row(1, "aa", 41.0, null),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 15.0, null),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten("SPARK-42038: partially clustered: right outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // The left-hand side is picked as the side to replicate partitions based on stats, but since
    // this is right outer join, partially clustered distribution won't kick in, and Spark should
    // only push down partition values on both side.
    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i RIGHT JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.map(_.outputPartitioning.numPartitions).toSet.size == 1)
                assert(
                  groupPartitions.forall(_.outputPartitioning.numPartitions == expected),
                  s"Expected $expected but got " +
                    s"${groupPartitions.headOption.map(_.outputPartitioning.numPartitions)}"
                )
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(null, null, null, 25.0),
                  Row(null, null, null, 30.0),
                  Row(1, "aa", 40.0, 45.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten("SPARK-42038: partially clustered: full outer join is not applicable") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i FULL OUTER JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                assert(groupPartitions.map(_.outputPartitioning.numPartitions).toSet.size == 1)
                assert(
                  groupPartitions.forall(_.outputPartitioning.numPartitions == expected),
                  s"Expected $expected but got " +
                    s"${groupPartitions.headOption.map(_.outputPartitioning.numPartitions)}"
                )
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(null, null, null, 20.0),
                  Row(null, null, null, 25.0),
                  Row(null, null, null, 30.0),
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 41.0, null),
                  Row(2, "bb", 10.0, 15.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  testGluten("SPARK-44641: duplicated records when SPJ is not triggered") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, items_schema, items_partitions)
    sql(s"""
        INSERT INTO testcat.ns.$items VALUES
        (1, 'aa', 40.0, cast('2020-01-01' as timestamp)),
        (1, 'aa', 41.0, cast('2020-01-15' as timestamp)),
        (2, 'bb', 10.0, cast('2020-01-01' as timestamp)),
        (2, 'bb', 10.5, cast('2020-01-01' as timestamp)),
        (3, 'cc', 15.5, cast('2020-02-01' as timestamp))""")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(s"""INSERT INTO testcat.ns.$purchases VALUES
        (1, 42.0, cast('2020-01-01' as timestamp)),
        (1, 44.0, cast('2020-01-15' as timestamp)),
        (1, 45.0, cast('2020-01-15' as timestamp)),
        (2, 11.0, cast('2020-01-01' as timestamp)),
        (3, 19.5, cast('2020-02-01' as timestamp))""")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(true, false).foreach {
          partiallyClusteredEnabled =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClusteredEnabled.toString
            ) {

              // join keys are not the same as the partition keys, therefore SPJ is not triggered.
              val df = sql(s"""
               SELECT id, name, i.price as purchase_price, p.item_id, p.price as sale_price
               FROM testcat.ns.$items i JOIN testcat.ns.$purchases p
               ON i.arrive_time = p.time ORDER BY id, purchase_price, p.item_id, sale_price
               """)

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.nonEmpty, "shuffle should exist when SPJ is not used")

              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 1, 42.0),
                  Row(1, "aa", 40.0, 2, 11.0),
                  Row(1, "aa", 41.0, 1, 44.0),
                  Row(1, "aa", 41.0, 1, 45.0),
                  Row(2, "bb", 10.0, 1, 42.0),
                  Row(2, "bb", 10.0, 2, 11.0),
                  Row(2, "bb", 10.5, 1, 42.0),
                  Row(2, "bb", 10.5, 2, 11.0),
                  Row(3, "cc", 15.5, 3, 19.5)
                )
              )
            }
        }
    }
  }

  testGluten("partitioned join:  join with two partition keys and matching & sorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(
      df,
      Seq(
        Row(1, "aa", 40.0, 42.0),
        Row(1, "aa", 41.0, 44.0),
        Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0),
        Row(2, "bb", 10.5, 11.0),
        Row(3, "cc", 15.5, 19.5)))
  }

  testGluten("partitioned join: join with two partition keys and unsorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(
      df,
      Seq(
        Row(1, "aa", 40.0, 42.0),
        Row(1, "aa", 41.0, 44.0),
        Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0),
        Row(2, "bb", 10.5, 11.0),
        Row(3, "cc", 15.5, 19.5)))
  }

  testGluten("partitioned join: join with two partition keys and different # of partition keys") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id AND i.arrive_time = p.time " +
              "ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
            assert(
              groupPartitions.size === 2,
              "should add group partitions when partition values mismatch")
          } else {
            assert(shuffles.nonEmpty, "should add shuffle when partition keys mismatch")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 11.0)))
        }
    }
  }

  testGluten("data source partitioning + dynamic partition filtering") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10"
    ) {
      val items_partitions = Array(identity("id"))
      createTable(items, items_schema, items_partitions)
      sql(
        s"INSERT INTO testcat.ns.$items VALUES " +
          s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
          s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchases_schema, purchases_partitions)
      sql(
        s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
          s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
          s"(3, 19.5, cast('2020-02-01' as timestamp))")

      Seq(true, false).foreach {
        pushDownValues =>
          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
            // number of unique partitions changed after dynamic filtering - the gap
            // should be filled with empty partitions and the job should still succeed
            var df = sql(
              s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
                "WHERE i.id = p.item_id AND i.price > 40.0")
            checkAnswer(df, Seq(Row(131)))

            // dynamic filtering doesn't change partitioning so storage-partitioned join should kick
            // in
            df = sql(
              s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
                "WHERE i.id = p.item_id AND i.price >= 10.0")
            val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
            assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
            checkAnswer(df, Seq(Row(303.5)))
          }
      }
    }
  }

  testGluten("SPARK-56549: k-way merge enabled only when parent requires ordering") {
    // Both tables are partitioned by id/item_id and report a two-column ordering.
    // Key 1 appears on two splits on each side, so GroupPartitionsExec must coalesce.
    //
    // Dynamic gate: with the config enabled, k-way merge must be activated only when the parent
    // actually requires ordering (SMJ), and must stay off when the parent does not (hash join).
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    catalog.createTable(
      Identifier.of(Array("ns"), items),
      items_schema,
      Array(identity("id")),
      emptyProps,
      Distributions.unspecified(),
      itemOrdering,
      None,
      None,
      numRowsPerSplit = 1)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
      s"(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
      s"(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
      s"(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
      s"(2, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    catalog.createTable(
      Identifier.of(Array("ns"), purchases),
      purchases_schema,
      Array(identity("item_id")),
      emptyProps,
      Distributions.unspecified(),
      purchaseOrdering,
      None,
      None,
      numRowsPerSplit = 1
    )
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(2, 50.0, cast('2025-09-01' as timestamp)), " +
      s"(1, 10.0, cast('2021-05-20' as timestamp)), " +
      s"(3, 40.0, cast('2024-01-01' as timestamp)), " +
      s"(2, 30.0, cast('2023-06-15' as timestamp)), " +
      s"(1, 20.0, cast('2022-03-10' as timestamp))")

    withSQLConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
      SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true"
    ) {
      val hashDf = sql(
        s"""
           |SELECT /*+ SHUFFLE_HASH(i, p) */ i.id, i.name
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
           |""".stripMargin)
      checkAnswer(hashDf, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))
      val hashPlan = hashDf.queryExecution.executedPlan
      assert(
        collect(hashPlan) {
          case j: ShuffledHashJoinExec => j
          case j: ShuffledHashJoinExecTransformerBase => j
        }.nonEmpty,
        "expected ShuffledHashJoinExec")
      assert(collectAllShuffles(hashPlan).isEmpty, "should not shuffle for compatible partitioning")
      val hashCoalescing =
        collectAllGroupPartitions(hashPlan).filter(_.groupedPartitions.exists(_._2.size > 1))
      assert(hashCoalescing.nonEmpty, "expected coalescing GroupPartitionsExec")
      hashCoalescing.foreach {
        gp =>
          assert(
            !gp.enableSortedMerge,
            "hash join does not require ordering: enableSortedMerge must stay false")
          // In Gluten, the child of GroupPartitionsExec may be columnar-only, so use
          // executeColumnar() when supportsColumnar is true. Calling execute()/executeColumnar()
          // directly on a plan node is safe here because the node is part of a fully executed
          // plan (df.collect() was called above), so children and metrics are initialized.
          val rdd = if (gp.supportsColumnar) gp.executeColumnar() else gp.execute()
          assert(
            !rdd.isInstanceOf[SortedMergeCoalescedRDD[_]],
            "hash join does not require ordering: must use simple CoalescedRDD")
      }

      val smjDf = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |i.id, i.name
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
           |""".stripMargin)
      checkAnswer(smjDf, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))
      val smjPlan = smjDf.queryExecution.executedPlan
      val smjs = collectSMJs(smjPlan)
      assert(smjs.nonEmpty, "expected SortMergeJoinExec")
      assert(collectAllShuffles(smjPlan).isEmpty, "should not shuffle for compatible partitioning")
      val smjCoalescing =
        collectAllGroupPartitions(smjPlan).filter(_.groupedPartitions.exists(_._2.size > 1))
      assert(smjCoalescing.nonEmpty, "expected coalescing GroupPartitionsExec")
      if (smjCoalescing.forall(_.enableSortedMerge)) {
        smjCoalescing.foreach {
          gp =>
            if (gp.supportsColumnar) {
              // Columnar path: doExecuteColumnar always uses CoalescedRDD, not SortedMerge.
              assert(
                !gp.executeColumnar().isInstanceOf[SortedMergeCoalescedRDD[_]],
                "columnar path should use CoalescedRDD")
            } else {
              // Row-based path: doExecute should use SortedMergeCoalescedRDD.
              assert(
                gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
                "sort-merge join requires ordering: must use SortedMergeCoalescedRDD")
            }
        }
      } else {
        // In this branch, additional safety checks may keep k-way merge disabled and rely on
        // SortExec to satisfy SMJ ordering requirements.
        val sortsBeforeSmj = smjs.flatMap(smj => collectSortsForSMJ(smj))
        assert(
          sortsBeforeSmj.nonEmpty,
          "if sorted merge is not enabled, SMJ should be satisfied via SortExec")
      }
    }
  }

  testGluten(
    "SPARK-55715: preserve outputOrdering when coalescing partitions with sorted merge") {
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    catalog.createTable(
      Identifier.of(Array("ns"), items),
      items_schema,
      Array(identity("id")),
      emptyProps,
      Distributions.unspecified(),
      itemOrdering,
      None,
      None,
      numRowsPerSplit = 1)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
      s"(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
      s"(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
      s"(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
      s"(2, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    catalog.createTable(
      Identifier.of(Array("ns"), purchases),
      purchases_schema,
      Array(identity("item_id")),
      emptyProps,
      Distributions.unspecified(),
      purchaseOrdering,
      None,
      None,
      numRowsPerSplit = 1
    )
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(2, 50.0, cast('2025-09-01' as timestamp)), " +
      s"(1, 10.0, cast('2021-05-20' as timestamp)), " +
      s"(3, 40.0, cast('2024-01-01' as timestamp)), " +
      s"(2, 30.0, cast('2023-06-15' as timestamp)), " +
      s"(1, 20.0, cast('2022-03-10' as timestamp))")

    Seq(true, false).foreach {
      preserveOrdering =>
        withSQLConf(
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            preserveOrdering.toString
        ) {
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("i", "p")}
               |i.id, i.name
               |FROM testcat.ns.$items i
               |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
               |""".stripMargin)
          checkAnswer(
            df,
            Seq(
              Row(1, "aa"),
              Row(1, "bb"),
              Row(2, "cc"),
              Row(2, "ee"),
              Row(3, "dd")))

          val plan = df.queryExecution.executedPlan
          assert(collectAllShuffles(plan).isEmpty, "should not contain any shuffle")

          val groupPartitions = collectAllGroupPartitions(plan)
          assert(groupPartitions.nonEmpty, "should contain GroupPartitionsExec for coalescing")
          assert(
            groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
            "expected coalescing GroupPartitionsExec")

          val smjs = collectSMJs(plan)
          assert(smjs.nonEmpty, "expected SortMergeJoinExec in plan")
          // Gluten's native execution nodes share native state across partitions and are not
          // SafeForKWayMerge, so SortedMergeCoalescedRDD is not activated. A SortExec is always
          // added to satisfy SMJ ordering, regardless of the config. This is a known limitation.
          smjs.foreach {
            smj =>
              assert(
                collectSortsForSMJ(smj).nonEmpty,
                "Gluten does not support k-way merge: SortExec should be present before SMJ")
          }
        }
    }
  }

  testGluten(
    "SPARK-55715: preserve outputOrdering when coalescing transform-partitioned splits") {
    val itemOrdering = Array(
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    catalog.createTable(
      Identifier.of(Array("ns"), items),
      items_schema,
      Array(years("arrive_time")),
      emptyProps,
      Distributions.unspecified(),
      itemOrdering,
      None,
      None,
      numRowsPerSplit = 1
    )
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(2, 'bb', 20.0, cast('2022-09-20' as timestamp)), " +
      s"(4, 'dd', 40.0, cast('2023-11-05' as timestamp)), " +
      s"(1, 'aa', 10.0, cast('2022-03-15' as timestamp)), " +
      s"(3, 'cc', 30.0, cast('2023-01-10' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    catalog.createTable(
      Identifier.of(Array("ns"), purchases),
      purchases_schema,
      Array(years("time")),
      emptyProps,
      Distributions.unspecified(),
      purchaseOrdering,
      None,
      None,
      numRowsPerSplit = 1
    )
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(2, 20.0, cast('2022-09-20' as timestamp)), " +
      s"(4, 40.0, cast('2023-11-05' as timestamp)), " +
      s"(1, 10.0, cast('2022-03-15' as timestamp)), " +
      s"(3, 30.0, cast('2023-01-10' as timestamp))")

    Seq(true, false).foreach {
      preserveOrdering =>
        withSQLConf(
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            preserveOrdering.toString) {
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("i", "p")}
               |i.id, i.name
               |FROM testcat.ns.$items i
               |JOIN testcat.ns.$purchases p ON p.time = i.arrive_time
               |""".stripMargin)
          checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb"), Row(3, "cc"), Row(4, "dd")))

          val plan = df.queryExecution.executedPlan
          assert(collectAllShuffles(plan).isEmpty, "should not contain any shuffle")

          val groupPartitions = collectAllGroupPartitions(plan)
          assert(groupPartitions.nonEmpty, "should contain GroupPartitionsExec for coalescing")
          assert(
            groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
            "expected coalescing GroupPartitionsExec")

          val smjs = collectSMJs(plan)
          assert(smjs.nonEmpty, "expected SortMergeJoinExec in plan")
          // Gluten's native execution nodes share native state across partitions and are not
          // SafeForKWayMerge, so SortedMergeCoalescedRDD is not activated. A SortExec is always
          // added to satisfy SMJ ordering, regardless of the config. This is a known limitation.
          smjs.foreach {
            smj =>
              assert(
                collectSortsForSMJ(smj).nonEmpty,
                "Gluten does not support k-way merge: SortExec should be present before SMJ")
          }
        }
    }
  }
}
