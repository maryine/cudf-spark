/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.{RapidsConf, ShimReflectionUtils}
import com.nvidia.spark.rapids.delta.{DeltaConfigChecker, DeltaProvider}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Clock

case class StartTransactionArg(log: DeltaLog, conf: RapidsConf, clock: Clock,
    catalogTable: Option[CatalogTable] = None, snapshot: Option[Snapshot] = None)

trait DeltaRuntimeShim {
  def getDeltaConfigChecker: DeltaConfigChecker
  def getDeltaProvider: DeltaProvider

  /** Unknown runtimes must not assume that a VARIANT table contains only unshredded files. */
  def mayContainShreddedVariant(protocol: Protocol): Boolean = true
  def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock)
  : GpuOptimisticTransactionBase = {
    startTransaction(StartTransactionArg(log, conf, clock))
  }
  def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase
  def stringFromStringUdf(f: String => String): UserDefinedFunction
  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot
  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike

  def createCpuWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType]): WriteIntoDelta = {
    require(catalogTableOpt.isEmpty, "Catalog tables require Delta 3.3 or later")
    WriteIntoDelta(
      deltaLog,
      mode,
      options,
      partitionColumns,
      configuration,
      data,
      schemaInCatalog = schemaInCatalog)
  }

  def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    throw new UnsupportedOperationException("Write operation metadata is not implemented")
  }

  def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    throw new UnsupportedOperationException("Replace table metadata is not implemented")
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog

  /**
   * Detect a DataFrameWriter V1 mode("overwrite").saveAsTable operation so it retains the
   * existing table metadata. Delta versions before 4.1 require stack-trace inspection.
   */
  def isV1WriterSaveAsTableOverwrite(options: DeltaOptions, mode: SaveMode): Boolean = {
    mode == SaveMode.Overwrite && Thread.currentThread().getStackTrace.exists(_.toString.contains(
      classOf[DataFrameWriter[_]].getCanonicalName + "."))
  }
}

object DeltaRuntimeShim {
  private val SparkVersion = """^(\d+)\.(\d+)\.(\d+).*""".r

  private def parseSparkVersion(sparkVersion: String): (Int, Int, Int) = sparkVersion match {
    case SparkVersion(major, minor, patch) => (major.toInt, minor.toInt, patch.toInt)
    case _ => throw new IllegalStateException(s"Unable to parse Spark version $sparkVersion")
  }

  private[rapids] def getDelta42ShimClassName(
      deltaVersion: String,
      sparkVersion: String): Option[String] = {
    if (deltaVersion.startsWith("4.2.")) {
      val parsedSparkVersion = parseSparkVersion(sparkVersion)
      (deltaVersion, parsedSparkVersion) match {
        case ("4.2.0", (4, 0, 1) | (4, 1, 1)) =>
          Some("org.apache.spark.sql.delta.rapids.delta42x.Delta42xRuntimeShim")
        case _ =>
          throw new IllegalStateException(
            s"Unsupported Delta Lake $deltaVersion and Spark $sparkVersion combination")
      }
    } else {
      None
    }
  }

  private[rapids] def getDelta43ShimClassName(
      deltaVersion: String,
      sparkVersion: String): Option[String] = {
    if (deltaVersion.startsWith("4.3.")) {
      val parsedSparkVersion = parseSparkVersion(sparkVersion)
      (deltaVersion, parsedSparkVersion) match {
        case ("4.3.0", (4, 0, 1) | (4, 1, 1)) =>
          Some("org.apache.spark.sql.delta.rapids.delta43x.Delta43xRuntimeShim")
        case _ =>
          throw new IllegalStateException(
            s"Unsupported Delta Lake $deltaVersion and Spark $sparkVersion combination")
      }
    } else {
      None
    }
  }

  private def getPreDelta42ShimClassName(
      deltaVersion: String,
      sparkVersion: String): String = {
    val shim = (deltaVersion.split('.').take(2).mkString("."),
      parseSparkVersion(sparkVersion)) match {
      case ("2.0", (3, 2, _)) => "delta20x.Delta20xRuntimeShim"
      case ("2.1", (3, 3, _)) => "delta21x.Delta21xRuntimeShim"
      case ("2.2", (3, 3, _)) => "delta22x.Delta22xRuntimeShim"
      case ("2.3", (3, 3, _)) => "delta23x.Delta23xRuntimeShim"
      case ("2.4", (3, 4, _)) => "delta24x.Delta24xRuntimeShim"
      case ("3.3", (3, 5, patch)) if patch > 2 => "delta33x.Delta33xRuntimeShim"
      case ("4.0", (4, 0, _)) => "delta40x.Delta40xRuntimeShim"
      case ("4.1", (4, 1, _)) => "delta41x.Delta41xRuntimeShim"
      case _ =>
        throw new IllegalStateException(
          s"Unsupported Delta Lake $deltaVersion and Spark $sparkVersion combination")
    }
    s"org.apache.spark.sql.delta.rapids.$shim"
  }

  private[rapids] def getShimClassName(deltaVersion: String, sparkVersion: String): String = {
    getDelta43ShimClassName(deltaVersion, sparkVersion)
      .orElse(getDelta42ShimClassName(deltaVersion, sparkVersion))
      .getOrElse(getPreDelta42ShimClassName(deltaVersion, sparkVersion))
  }

  private[rapids] lazy val shimInstance = {
    val shimClassName = getShimClassName(io.delta.VERSION, SPARK_VERSION)
    val shimClass = ShimReflectionUtils.loadClass(shimClassName)
    shimClass.getConstructor().newInstance().asInstanceOf[DeltaRuntimeShim]
  }

  private[rapids] def getShimInstance: DeltaRuntimeShim = shimInstance

  def getDeltaProvider: DeltaProvider = shimInstance.getDeltaProvider

  def mayContainShreddedVariant(protocol: Protocol): Boolean =
    shimInstance.mayContainShreddedVariant(protocol)

  def getDeltaConfigChecker: DeltaConfigChecker = {
    shimInstance.getDeltaConfigChecker
  }

  def startTransaction(txArg: StartTransactionArg): GpuOptimisticTransactionBase = {
    shimInstance.startTransaction(txArg)
  }

  def stringFromStringUdf(f: String => String): UserDefinedFunction =
    shimInstance.stringFromStringUdf(f)

  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    shimInstance.unsafeVolatileSnapshotFromLog(deltaLog)

  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat =
    shimInstance.fileFormatFromLog(deltaLog)

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike = {
    shimInstance.createGpuWrite(gpuDeltaLog, cpuWrite)
  }

  def createCpuWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType]): WriteIntoDelta = {
    shimInstance.createCpuWrite(
      deltaLog, mode, options, partitionColumns, configuration, data,
      catalogTableOpt, schemaInCatalog)
  }

  def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    shimInstance.buildWriteOperation(mode, partitionColumns, options)
  }

  def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    shimInstance.buildReplaceTableOperation(
      metadata, isManaged, orCreate, asSelect, options, clusterBy, isV1SaveAsTableOverwrite)
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean =
    shimInstance.getTightBoundColumnOnFileInitDisabled(spark)

  def isV1WriterSaveAsTableOverwrite(options: DeltaOptions, mode: SaveMode): Boolean =
    shimInstance.isV1WriterSaveAsTableOverwrite(options, mode)

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog = {
    shimInstance.getGpuDeltaCatalog(cpuCatalog, rapidsConf)
  }
}
