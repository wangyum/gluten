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

import java.net.URI
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/**
 * Materializes per-file Delta DV read options for eBay Delta (3.0.0-ebay), which stores a
 * broadcast map keyed by relative file URIs (Map[URI, DeletionVectorDescriptorWithFilterType])
 * on the DeltaParquetFileFormat rather than per-file otherConstantMetadataColumnValues keys.
 */
object DeltaDeletionVectorScanInfo {

  def normalize(
      partitionColumnCount: Int,
      partitionFiles: Seq[PartitionedFile],
      fileFormat: FileFormat = null): Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
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
        // The broadcastDvMap is keyed by relative URI (as stored in AddFile.path).
        // Derive the relative URI by stripping the absolute table path prefix.
        val absolutePathStr = file.filePath.toString
        val relativeUri = toRelativeUri(absolutePathStr, tablePathStr)
        val otherMetadata = SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file)
        val normalizedMeta: Map[String, Object] =
          if (otherMetadata == null) Map.empty
          else otherMetadata.asScala.toMap
        dvMap.get(relativeUri) match {
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

  /**
   * Derives the relative URI of a file with respect to the table root. The broadcastDvMap in eBay
   * Delta uses relative URIs (from AddFile.pathAsUri which is new URI(addFile.path)) as keys. For
   * partitioned tables the relative URI includes the partition directory prefix.
   */
  private def toRelativeUri(absolutePathStr: String, tablePathStr: String): URI = {
    // Normalize both paths to remove trailing slashes and ensure consistent format.
    val tablePrefixWithSlash =
      if (tablePathStr.endsWith("/")) tablePathStr else tablePathStr + "/"
    if (absolutePathStr.startsWith(tablePrefixWithSlash)) {
      new URI(absolutePathStr.substring(tablePrefixWithSlash.length))
    } else {
      // Fallback: try using just the URI path without scheme/authority.
      val fileUri = new URI(absolutePathStr)
      val tableUri = new URI(tablePathStr)
      val filePath = fileUri.getPath
      val tablePathPrefix = if (tableUri.getPath.endsWith("/")) tableUri.getPath
        else tableUri.getPath + "/"
      if (filePath.startsWith(tablePathPrefix)) {
        new URI(filePath.substring(tablePathPrefix.length))
      } else {
        // Last resort: use the full URI (may not match in the map).
        fileUri
      }
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
