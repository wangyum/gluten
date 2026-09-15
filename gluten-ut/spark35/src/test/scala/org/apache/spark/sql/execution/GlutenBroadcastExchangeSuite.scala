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
package org.apache.spark.sql.execution

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.broadcast.TorrentBroadcast
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, GlutenTestsBaseTrait, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.broadcast

class GlutenBroadcastExchangeSuite extends BroadcastExchangeSuite with GlutenSQLTestsBaseTrait {}

// Additional tests run in 'local-cluster' mode.
class GlutenLocalBroadcastExchangeSuite
  extends SparkFunSuite
  with LocalSparkContext
  with GlutenTestsBaseTrait
  with AdaptiveSparkPlanHelper {

  def newSparkConf(): SparkConf = {
    val conf = new SparkConf().setMaster("local-cluster[2,1,1024]")
    // Local-cluster executors are separate JVMs whose classpath puts the driver's
    // classpath ahead of the spark.test.home distribution jars. The Spark 3.5
    // distribution bundles Arrow 12.0.1, whose arrow-memory-netty jar still carries
    // the legacy org/apache/arrow/memory/DefaultAllocationManagerFactory.class
    // resource. Arrow 18's CheckAllocator picks that legacy resource first and maps
    // the jar name to the new-style
    // org.apache.arrow.memory.netty.DefaultAllocationManagerFactory, which exists
    // nowhere on this classpath (Gluten bundles arrow-memory-unsafe only), so
    // ArrowBufferAllocators class-init fails on executors with
    // ExceptionInInitializerError. Pin the unsafe allocator to skip the classpath
    // scan. (SparkContext.supplementJavaModuleOptions already supplements
    // spark.executor.extraJavaOptions with the required --add-opens.)
    conf.set("spark.executor.extraJavaOptions", "-Darrow.allocation.manager.type=Unsafe")
    GlutenSQLTestsBaseTrait.nativeSparkConf(conf, warehouse)
  }

  test("SPARK-39983 - Broadcasted relation is not cached on the driver") {
    // Use distributed cluster as in local mode the broabcast value is actually cached.
    val conf = newSparkConf()
    sc = new SparkContext(conf)
    val spark = new SparkSession(sc)

    val df = spark.range(1).toDF()
    val joinDF = df.join(broadcast(df), "id")
    joinDF.collect()
    val broadcastExchangeExec = collect(joinDF.queryExecution.executedPlan) {
      case p: ColumnarBroadcastExchangeExec => p
    }
    assert(broadcastExchangeExec.size == 1, "one and only ColumnarBroadcastExchangeExec")

    // The broadcasted relation should not be cached on the driver.
    val broadcasted =
      broadcastExchangeExec(0).relationFuture.get().asInstanceOf[TorrentBroadcast[Any]]
    assert(!broadcasted.hasCachedValue)
  }
}
