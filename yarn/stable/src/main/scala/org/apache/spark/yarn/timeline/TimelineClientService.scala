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

package org.apache.spark.yarn.timeline

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


/**
 * Context interface for sharing information across components in Tez DAG
 */
@InterfaceAudience.Private abstract trait AppContext {
  def getAppMaster: ApplicationMaster

  def getAMConf: Configuration

  def getApplicationID: ApplicationId

  def getApplicationAttemptId: ApplicationAttemptId

  def getApplicationName: String

  def getApplicationACLs: Map[ApplicationAccessType, String]

  def getStartTime: Long

  def getUser: String


  @SuppressWarnings(Array("rawtypes")) def getEventHandler: EventHandler[_ <: Event[_]]

  def getClock: Clock

  def getSessionResources: Map[String, LocalResource]

  def isSession: Boolean

  def getCurrentRecoveryDir: Path

  def isRecoveryEnabled: Boolean
}

abstract class SparkHistoryEvent(eventType: Int) {
  def convertToTimelineEntity: TimelineEntity
}

abstract class HistoryLoggingService(appContext: AppContext) extends AbstractService("ATS") {
  /**
   * Handle logging of history event
   * @param event History event to be logged
   */
  def enqueue(event: Any)

}
/**
 * Created by zzhang on 8/1/14.
*/
object ATSHistoryLoggingService {
  val appMasterLatch = new CountDownLatch(1)
  var appId: ApplicationId = null
  def startLoggingService(id: ApplicationId) = {
    appId = id
    appMasterLatch.countDown()
  }
}

class ATSHistoryLoggingService(sc: SparkContext) extends HistoryLoggingService(null) with Logging {
  logInfo("sparkContext: " + sc)
  val appContext: AppContext = null
  var timelineClient :TimelineClient = _
  var listener: ATSSparkListener = _

  override def serviceInit(conf: Configuration) {
    logInfo("Initializing ATSService")
    timelineClient = TimelineClient.createTimelineClient()
    timelineClient.init(conf)
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


  override def enqueue(event: Any) = {
    if (!stopped.get()) {
      eventQueue.add(event)
    } else {
      logWarning("ATS service stopped")
    }
  }


  override def serviceStart {
    logInfo("Starting ATSService")
    timelineClient.start

    eventHandlingThread = new Thread(new Runnable {
      def run {
           var event: Any = null
           try {
             logInfo("wait on latch")
             ATSHistoryLoggingService.appMasterLatch.await()
             logInfo("latch passed")
           } catch {
             case e: Exception => {
               logError("Error handling latch", e)
             //break //todo: break is not supported
             }
           }
            log.info("Starting service to appId " + ATSHistoryLoggingService.appId)
            while (!stopped.get && !Thread.currentThread.isInterrupted) {
              if (eventCounter != 0 && eventCounter % 1000 == 0) {
                //  LOG.info("Event queue stats" + ", eventsProcessedSinceLastUpdate=" +
                // eventsProcessed + ", eventQueueSize=" + eventQueue.size)
                eventCounter = 0
                eventsProcessed = 0
              }
              else {
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
                  handleEvent(event)
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
        val startTime: Long = appContext.getClock.getTime
        if (maxTimeToWaitOnShutdown > 0) {
          val endTime: Long = startTime + maxTimeToWaitOnShutdown
          while (endTime >= appContext.getClock.getTime) {
            val event: Any = eventQueue.poll
            if (event == null) {
             //break //todo: break is not supported
            }
            try {
              handleEvent(event)
            }
            catch {
              case e: Exception => {
                //LOG.warn("Error handling event", e)
                //break //todo: break is not supported
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
    timelineClient.stop
  }


  private def handleEvent(event: Any) = {
     logInfo("handleEvent " + event)

     val entity = event match {

      case _ =>
        val sparkEntity: TimelineEntity = new TimelineEntity
        import org.apache.spark.deploy.yarn.Client
        sparkEntity.setEntityId("Spark_" + ATSHistoryLoggingService.appId.toString)
        //sparkEntity.setEntityId("spark_test")


        sparkEntity.setEntityType("spark_type")
        sparkEntity.addPrimaryFilter("pf", ATSHistoryLoggingService.appId.toString)
        val startEvt: TimelineEvent = new TimelineEvent

        startEvt.setEventType("spark_event_" + ATSHistoryLoggingService.appId.toString)


        startEvt.setTimestamp(java.lang.System.currentTimeMillis())
        sparkEntity.addEvent(startEvt)
        sparkEntity
    }


    try {
      val response: TimelinePutResponse = timelineClient.putEntities(entity)
      if (response != null && !response.getErrors.isEmpty) {
        val err: TimelinePutResponse.TimelinePutError = response.getErrors.get(0)
        if (err.getErrorCode != 0) {
          stopped.set(true)
          logError("Could not post history event to ATS, eventType=" +  err.getErrorCode)
        }
      } else {
        logInfo("entity pushed: " + entity)
      }
    }
    catch {
      case e: Exception => {
        logError("Could not handle history event: " + event)
      }
    }
  }
  import java.util.concurrent.LinkedBlockingQueue

  private var eventQueue = new LinkedBlockingQueue[Any]
  private var eventHandlingThread: Thread = null
  private var stopped: AtomicBoolean = new AtomicBoolean(false)
  private var eventCounter: Int = 0
  private var eventsProcessed: Int = 0
  private final val lock: AnyRef = new AnyRef
  private var maxTimeToWaitOnShutdown: Long = 0L
}

