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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.service.AbstractService

private [spark] trait YarnService extends AbstractService {
    // For Yarn services, SparkContext, and ApplicationId is the basic info required.
    // May change upon new services added.
    def start(sc: SparkContext, appId: ApplicationId): Boolean
}

private[spark] object YarnService extends Logging{
  var service: YarnService = null
  def start(sc: SparkContext, appId: ApplicationId) {
    try {
      service = Class.forName("org.apache.spark.deploy.yarn.history.YarnHistoryService")
        .newInstance()
        .asInstanceOf[YarnService]
      service.start(sc, appId)

    } catch {
      case e: Exception =>
        logWarning("Cannot instantiate Yarn service.", e)
    }
  }

  //stop all services
  def stop() {
    service.stop
  }
}

