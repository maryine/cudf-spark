/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.delta.{DeltaProbeImpl, DeltaProvider, NoDeltaProvider}
import org.scalatest.funsuite.AnyFunSuite

class DeltaProviderDiscoverySuite extends AnyFunSuite {
  test("runtime shim selection checks both Delta Lake and Spark versions") {
    val supported = Seq(
      ("2.0.2", "3.2.4", "delta20x"),
      ("2.1.1", "3.3.0", "delta21x"),
      ("2.2.0", "3.3.1", "delta22x"),
      ("2.3.0", "3.3.2", "delta23x"),
      ("2.4.0", "3.4.4", "delta24x"),
      ("3.3.3", "3.5.3", "delta33x"),
      ("3.3.3", "3.5.3-dataproc", "delta33x"),
      ("3.3.3", "3.5.9", "delta33x"),
      ("4.0.0", "4.0.0", "delta40x"),
      ("4.0.1", "4.0.4", "delta40x"),
      ("4.1.0", "4.1.1", "delta41x"),
      ("4.2.0", "4.0.1", "delta42x"),
      ("4.2.0", "4.1.1", "delta42x"),
      ("4.3.0", "4.0.1", "delta43x"),
      ("4.3.0", "4.1.1", "delta43x"))
    supported.foreach { case (deltaVersion, sparkVersion, shim) =>
      assert(DeltaRuntimeShim.getShimClassName(deltaVersion, sparkVersion).contains(s".$shim."))
    }
  }

  test("unsupported Delta Lake versions do not select a shim from Spark alone") {
    val unsupported = Seq(
      ("3.2.1", "3.5.3"),
      ("3.2.1", "3.5.3-dataproc"),
      ("3.0.0", "3.5.3"),
      ("3.3.3", "3.5.2"),
      ("3.3.3", "4.0.1"),
      ("4.0.1", "3.5.3"),
      ("4.1.0", "4.0.1"),
      ("4.0.1", "4.1.1"),
      ("4.2.0", "4.1.2"),
      ("4.3.1", "4.0.1"),
      ("5.0.0", "4.1.1"))
    unsupported.foreach { case (deltaVersion, sparkVersion) =>
      val error = intercept[IllegalStateException] {
        DeltaRuntimeShim.getShimClassName(deltaVersion, sparkVersion)
      }
      assert(error.getMessage.contains(deltaVersion))
      assert(error.getMessage.contains(sparkVersion))
    }
  }

  test("Delta discovery falls back on unsupported versions and linkage errors") {
    Seq(
      new IllegalStateException("Unsupported Delta Lake 3.2.1 and Spark 3.5.3 combination"),
      new NoClassDefFoundError("org/apache/spark/internal/LoggingShims"),
      new NoSuchMethodError("incompatible Delta Lake API"),
      new java.lang.reflect.InvocationTargetException(new NoClassDefFoundError("Delta API")))
      .foreach { error =>
        val probe = new DeltaProbeImpl {
          override protected def loadDeltaProvider: DeltaProvider = throw error
        }
        assert(probe.getDeltaProvider eq NoDeltaProvider)
      }
  }

  test("Delta discovery does not swallow fatal JVM errors") {
    val error = new OutOfMemoryError("test fatal error propagation")
    val probe = new DeltaProbeImpl {
      override protected def loadDeltaProvider: DeltaProvider = throw error
    }
    assert(intercept[OutOfMemoryError](probe.getDeltaProvider) eq error)
  }

  test("Delta discovery preserves the supported provider") {
    val probe = new DeltaProbeImpl
    assert(probe.getDeltaProvider ne NoDeltaProvider)
  }
}
