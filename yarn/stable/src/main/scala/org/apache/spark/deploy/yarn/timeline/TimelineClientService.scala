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

package org.apache.spark.deploy.yarn.timeline

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.io._
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.json4s.JsonAST._

import org.apache.spark._
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.hadoop.service.AbstractService
import org.apache.commons.logging.{LogFactory, Log}
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse
import org.apache.hadoop.yarn.client.api.TimelineClient
import java.util.Map
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.ApplicationAccessType
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.event.EventHandler
import org.apache.hadoop.yarn.util.Clock
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.apache.hadoop.yarn.event.Event
import org.apache.hadoop.yarn.api.records.timeline._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler._


class ATSHistoryLoggingService(sc: SparkContext, appId: ApplicationId) extends AbstractService("ATS") with Logging {
  logInfo("sparkContext: " + sc)
  var timelineClient: Option[TimelineClient] = None
  var listener: ATSSparkListener = _
  val ENTITY_TYPE = "SparkApplication"
  val PriFilter: String = null
  var appName: String = null
  var userName: String = null
  var batchSize: Int = 100
  var conf: Configuration = _
  import java.util.concurrent.LinkedBlockingQueue

  // enqueue event to avoid blocking on main thread.
  private var eventQueue = new LinkedBlockingQueue[Any]
  // cache layer to handle ats client failure.
  private var entityList = new util.LinkedList[TimelineEntity]()
  private var eventHandlingThread: Thread = null
  private var stopped: AtomicBoolean = new AtomicBoolean(false)
  private var eventCounter: Int = 0
  private var eventsProcessed: Int = 0
  private final val lock: AnyRef = new AnyRef
  private var maxTimeToWaitOnShutdown: Long = 0L


  def getTimelineClient = timelineClient.getOrElse{
    val client = TimelineClient.createTimelineClient()
    client.init(conf)
    client.start
    timelineClient = Some(client)
    client

  }
  override def serviceInit(config: Configuration) {
    logInfo("Initializing ATSService")
    conf = config
    getTimelineClient
    //  maxTimeToWaitOnShutdown = conf.getLong(TezConfiguration.
    // YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
    // TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT)
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("terminating logging service") {
      override def run(): Unit = Utils.logUncaughtExceptions {
        logDebug("Shutdown hook called")
        stopATS
      }
    })
  }

  def stopATS(): Boolean = {
    if (!stopped.getAndSet(true)) {
      logInfo("Stopping ATS service")
      stop
    }
    true
  }

  def startATS(): Boolean = {
    logInfo("Starting ats service with hadoopConf ...: " + sc.hadoopConfiguration)
    addShutdownHook
    init(sc.hadoopConfiguration)
    start()
    listener = new ATSSparkListener(sc, this)
    sc.listenerBus.addListener(listener)
    logInfo("ATS service started ...")

    true
  }


  def enqueue(event: Any) = {
    if (!stopped.get()) {
      eventQueue.add(event)
    } else {
      logWarning("ATS service stopped")
    }
  }


  override def serviceStart {
    logInfo("Starting ATSService")
   // timelineClient.start

    eventHandlingThread = new Thread(new Runnable {
      def run {
        var event: Any = null
        /*  try {
            logInfo("wait on latch")
            ATSHistoryLoggingService.appMasterLatch.await()
            logInfo("latch passed")
          } catch {
            case e: Exception => {
              logError("Error handling latch", e)
            //break //todo: break is not supported
            }
          }*/
        log.info("Starting service to appId " + appId)
        while (!stopped.get && !Thread.currentThread.isInterrupted) {
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            //  LOG.info("Event queue stats" + ", eventsProcessedSinceLastUpdate=" +
            // eventsProcessed + ", eventQueueSize=" + eventQueue.size)
            eventCounter = 0
            eventsProcessed = 0
          } else {
            eventCounter += 1
          }
          try {
            event = eventQueue.take
          } catch {
            case e: InterruptedException => {
              logError("queue take exception", e)
              //  LOG.info("EventQueue take interrupted. Returning")
              return
            }
          }
          lock synchronized {
            eventsProcessed += 1
            try {
              handleEvent(event.asInstanceOf[SparkListenerEvent], false)
            }
            catch {
              case e: Exception => {
                logError("handle event exception", e)
                // LOG.warn("Error handling event", e)
              }
            }
          }
        }
      }
    }, "HistoryEventHandlingThread")
    eventHandlingThread.start
  }

  override def serviceStop {
    logInfo("Stopping ATSService")
    stopped.set(true)
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt
    }
    lock synchronized {
      if (!eventQueue.isEmpty) {
        // LOG.warn("ATSService being stopped" + ", eventQueueBacklog="
        // + eventQueue.size + ", maxTimeLeftToFlush=" + maxTimeToWaitOnShutdown)
        val startTime: Long = System.currentTimeMillis()
        if (maxTimeToWaitOnShutdown > 0) {
          val endTime: Long = startTime + maxTimeToWaitOnShutdown
          while (endTime >= System.currentTimeMillis()) {
            val event: Any = eventQueue.poll
            if (event == null) {
              //break //todo: break is not supported
            }
            try {
              handleEvent(event.asInstanceOf[SparkListenerEvent], true)
            } catch {
              case e: Exception => {
                logError("Error handling event", e)
              }
            }
          }
        }
      }
    }
    if (!eventQueue.isEmpty) {
      logWarning("Did not finish flushing eventQueue before " +
        "stopping ATSService, eventQueueBacklog=" + eventQueue.size)
    }
    getTimelineClient.stop
  }

  var curEntity: Option[TimelineEntity] = None
  // Do we have enough information filled for the entity
  var bInit = false
  // How many event we saved
  var curEventNum = 0

  def getCurrentEntity = {
    curEntity.getOrElse {
      val entity: TimelineEntity = new TimelineEntity
      curEventNum = 0
      if (bInit) {
        entity.setEntityType(ENTITY_TYPE)
        entity.setEntityId(appId.toString)
        //sparkEntity.setTimestamp(java.lang.System.curimaryFilter("appName", appName)
        entity.addPrimaryFilter("appUser", userName)
        entity.addOtherInfo("appName", appName)
        entity.addOtherInfo("sparkUser", userName)
      }
      entity
    }
  }

  // If there is any available entity to be sent, push to ATS
  // Append to the available list on failure.
  def flushEntity(): Unit = {
    // if we don't have enough information yet, hold it
    if (entityList.isEmpty) {
      return
    }
    entityList.foreach {
      entity => {
        try {
          val response: TimelinePutResponse = getTimelineClient.putEntities(entity)
          if (response != null && !response.getErrors.isEmpty) {
            val err: TimelinePutResponse.TimelinePutError = response.getErrors.get(0)
            if (err.getErrorCode != 0) {
              stopped.set(true)
              logError("Could not post history event to ATS, eventType=" + err.getErrorCode)
            }
          } else {
            logInfo("entity pushed: " + curEntity.get)
          }
        } catch {
          case e: Exception => {
            timelineClient = None
            logError("Could not handle history entity: " + curEntity.get)
            entityList.addFirst(entity)
          }
        }
      }
    }
  }

  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event
   * @param flush
   * @return
   */
  private def handleEvent(event: SparkListenerEvent, flush: Boolean) = {
    event match {
      case e: SparkListenerApplicationStart =>
        // we already have all information,
        // flush it for old one to switch to new one
        if (bInit) {
          entityList.add(curEntity.get)
        } else {
          curEntity match {
            case Some(e) => e.setEntityType(ENTITY_TYPE)
              e.setEntityId(appId.toString)
              //sparkEntity.setTimestamp(java.lang.System.curimaryFilter("appName", appName)
              e.addPrimaryFilter("appUser", userName)
              e.addOtherInfo("appName", appName)
              e.addOtherInfo("sparkUser", userName)
            case None =>
          }

        }
        appName = e.appName;
        userName = e.sparkUser

      case _ =>
    }

    val tlEvent = new TimelineEvent()
    tlEvent.setEventType("spark_event_" + appId.toString)
    //TODO
    val kvMap = new util.HashMap[String, Object]();
    kvMap.put(Utils.getFormattedClassName(event).toString, event)
    tlEvent.setEventInfo(kvMap)
    getCurrentEntity.addEvent(tlEvent)
    curEventNum += 1
    if (curEventNum == batchSize || flush) {
      entityList.add(curEntity.get)
      curEntity = null
    }
    flushEntity()
  }
}
