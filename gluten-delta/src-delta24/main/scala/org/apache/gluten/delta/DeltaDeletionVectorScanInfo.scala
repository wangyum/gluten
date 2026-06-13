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
package org.apache.gluten.delta

import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.DeltaFileReadOptions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}

import org.apache.hadoop.fs.Path

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/**
 * Materializes per-file Delta DV read options for eBay Delta (3.0.0-ebay), which stores a broadcast
 * map keyed by absolute file URIs (Map[URI, DeletionVectorDescriptorWithFilterType]) on the
 * DeltaParquetFileFormat. The key is created by absolutePath(tablePath, addFile.path).toUri in
 * PreprocessTableWithDVs.createBroadcastDVMap.
 */
object DeltaDeletionVectorScanInfo {

  def normalize(
      partitionColumnCount: Int,
      partitionFiles: Seq[PartitionedFile],
      fileFormat: FileFormat =
        null): Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    fileFormat match {
      case delta: DeltaParquetFileFormat =>
        extractFromBroadcastDvMap(delta, partitionFiles)
      case _ => None
    }
  }

  private def extractFromBroadcastDvMap(
      delta: DeltaParquetFileFormat,
      partitionFiles: Seq[PartitionedFile])
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    val broadcastDvMapOpt = delta.broadcastDvMap
    val tablePathOpt = delta.tablePath

    if (broadcastDvMapOpt.isEmpty || tablePathOpt.isEmpty) {
      return None
    }

    val dvMap = broadcastDvMapOpt.get.value
    if (dvMap.isEmpty) {
      return None
    }

    val spark = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse(
        throw new IllegalStateException(
          "Active SparkSession is required to materialize Delta deletion vectors"))

    val tablePathStr = tablePathOpt.get
    val tablePath = new Path(tablePathStr)
    val dvStore = new HadoopFileSystemDVStore(spark.sessionState.newHadoopConf())

    val scanInfos = partitionFiles.map {
      file =>
        // The broadcastDvMap is keyed by absolute URI, created by
        // absolutePath(tahoeFileIndex.path.toString, addFile.path).toUri in
        // PreprocessTableWithDVs.createBroadcastDVMap.
        val absoluteUri = file.filePath.toUri
        val otherMetadata = SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file)
        val normalizedMeta: Map[String, Object] =
          if (otherMetadata == null) Map.empty
          else otherMetadata.asScala.toMap
        dvMap.get(absoluteUri) match {
          case Some(dvWithFilterType) =>
            val descriptor = dvWithFilterType.descriptor
            val filterType = dvWithFilterType.filterType
            val serializedPayload = StoredBitmap
              .create(descriptor, tablePath)
              .load(dvStore)
              .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
            (
              normalizedMeta.asJava,
              new DeltaFileReadOptions(
                toSubstraitRowIndexFilterType(filterType),
                true,
                descriptor.cardinality,
                serializedPayload))
          case None =>
            (
              normalizedMeta.asJava,
              new DeltaFileReadOptions(
                DeltaLocalFilesNode.RowIndexFilterType.KEEP_ALL,
                false,
                0L,
                Array.emptyByteArray))
        }
    }

    val hasDv = scanInfos.exists(_._2.hasDeletionVector())
    if (hasDv) {
      Some((scanInfos.map(_._1), scanInfos.map(_._2)))
    } else {
      None
    }
  }

  private def toSubstraitRowIndexFilterType(
      filterType: RowIndexFilterType): DeltaLocalFilesNode.RowIndexFilterType = {
    filterType match {
      case RowIndexFilterType.IF_CONTAINED => DeltaLocalFilesNode.RowIndexFilterType.IF_CONTAINED
      case RowIndexFilterType.IF_NOT_CONTAINED =>
        DeltaLocalFilesNode.RowIndexFilterType.IF_NOT_CONTAINED
      case _ => DeltaLocalFilesNode.RowIndexFilterType.KEEP_ALL
    }
  }
}
