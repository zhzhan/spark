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
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap, Collection}

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
import java.util.{ArrayList => JArrayList, Collection => JCollection, HashMap => JHashMap,
Map => JMap}
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent
import org.json4s.JsonAST._

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.apache.spark.deploy.yarn.timeline.TimedEvent
import scala.collection.mutable.LinkedList


class ATSHistoryLoggingService(sc: SparkContext, appId: ApplicationId)
  extends AbstractService("ATS") with Logging {
  logInfo("sparkContext: " + sc)
  var timelineClient: Option[TimelineClient] = None
  var listener: ATSSparkListener = _
  val ENTITY_TYPE = "SparkApplication"
  val PriFilter: String = null
  var appName: String = null
  var userName: String = null
  var startTime: Long = _
  var endTime: Long = _
  var bEnd = false
  var batchSize: Int = 3
  var conf: Configuration = _

  import java.util.concurrent.LinkedBlockingQueue

  // enqueue event to avoid blocking on main thread.
  private var eventQueue = new LinkedBlockingQueue[TimedEvent]
  // cache layer to handle ats client failure.
  private var entityList = new LinkedList[TimelineEntity]
  var curEntity: Option[TimelineEntity] = None
  // Do we have enough information filled for the entity
  var bInit = false
  // How many event we saved
  var curEventNum = 0
  private var eventHandlingThread: Thread = null
  private var stopped: AtomicBoolean = new AtomicBoolean(false)
  private var eventCounter: Int = 0
  private var eventsProcessed: Int = 0
  private final val lock: AnyRef = new AnyRef
  private var maxTimeToWaitOnShutdown: Long = 5000L
  private var clientCreateNum = 0


  def createTimeClient = {
    clientCreateNum += 1
    logInfo("Creating timelineClient " + clientCreateNum)
    val client = TimelineClient.createTimelineClient()
    client.init(conf)
    client.start
    timelineClient = Some(client)
    client
  }

  def getTimelineClient = timelineClient.getOrElse {
    clientCreateNum += 1
    logInfo("Creating timelineClient " + clientCreateNum)
    val client = TimelineClient.createTimelineClient()
    client.init(conf)
    client.start
    timelineClient = Some(client)
    client
  }

  def stopTimelineClient = {
    timelineClient match {
      case Some(t) => t.stop
      case _ =>
    }
    timelineClient = None
  }

  override def serviceInit(config: Configuration) {
    logInfo("Initializing ATSService")
    conf = config
    createTimeClient
    // getTimelineClient
    //  maxTimeToWaitOnShutdown = conf.getLong(TezConfiguration.
    // YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
    // TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT)
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("terminating logging service") {
      /* override def run(): Unit = Utils.logUncaughtExceptions {
        logInfo("Shutdown hook called")
        stopATS
      }
      */
      override def run() = {
        logInfo("Shutdown hook called")
        serviceStop
      }
    })
  }


  def startATS(): Boolean = {
    logInfo("Starting ATS service ...")
    addShutdownHook
    init(sc.hadoopConfiguration)
    start()
    listener = new ATSSparkListener(sc, this)
    sc.listenerBus.addListener(listener)
    true
  }


  def enqueue(event: TimedEvent) = {
    if (!stopped.get()) {
      eventQueue.add(event)
    } else {
      logWarning("ATS service stopped")
    }
  }


  override def serviceStart {
    eventHandlingThread = new Thread(new Runnable {
      def run {
        var event: Any = null
        log.info("Starting service for AppId " + appId)
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
            eventsProcessed += 1
            handleEvent(event.asInstanceOf[TimedEvent], false)
          } catch {
            case _ => {
              logWarning("EventQueue take interrupted. Returning")
            }
          }
        }
      }
    }, "HistoryEventHandlingThread")
    eventHandlingThread.start
  }

  private def stopATS(): Boolean = {
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt
    }

    logInfo("push out all events")
    if (!eventQueue.isEmpty) {
      val curTime: Long = System.currentTimeMillis()
      if (maxTimeToWaitOnShutdown > 0) {
        val endTime: Long = curTime + maxTimeToWaitOnShutdown
        while (endTime >= System.currentTimeMillis()) {
          val event = eventQueue.poll
          handleEvent(event, true)
        }
      }
    } else {
      handleEvent(null, true)
    }
    if (!eventQueue.isEmpty) {
      logWarning("Did not finish flushing eventQueue before " +
        "stopping ATSService, eventQueueBacklog=" + eventQueue.size)
    }
    stopTimelineClient
    logInfo("ATS service terminated")
    // new Throwable().printStackTrace()
    true
  }

  override def serviceStop {
    logInfo("Stopping ATS service")
    if (!bEnd) {
      eventQueue.add(new TimedEvent(SparkListenerApplicationEnd(System.currentTimeMillis()),
        System.currentTimeMillis()))
    }
    if (!stopped.getAndSet(true)) {
      stopATS
      stop
    }
  }

  /*
  override def serviceStop {
    stopped.set(true)

  }
*/


  def getCurrentEntity = {
    curEntity.getOrElse {
      val entity: TimelineEntity = new TimelineEntity
      logInfo("Create new entity")
      curEventNum = 0
      entity.setEntityType(ENTITY_TYPE)
      entity.setEntityId(appId.toString)
      if (bInit) {

        // sparkEntity.setTimestamp(java.lang.System.curimaryFilter("appName", appName)
        entity.addPrimaryFilter("appName", appName)
        entity.addPrimaryFilter("appUser", userName)
        entity.addOtherInfo("appName", appName)
        entity.addOtherInfo("appUser", userName)
      }
      curEntity = Some(entity)
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
    logInfo("before pushEntities: " + entityList.size())
    var client = timelineClient.getOrElse(createTimeClient) // getTimelineClient
    entityList = entityList.filter {
      en => {
        if (en == null) {
          false
        } else {
          try {
            val response: TimelinePutResponse = client.putEntities(en)
            if (response != null && !response.getErrors.isEmpty) {
              val err: TimelinePutResponse.TimelinePutError = response.getErrors.get(0)
              if (err.getErrorCode != 0) {
                timelineClient = None
                logError("Could not post history event to ATS, eventType=" + err.getErrorCode)
              }
              true
            } else {
              logInfo("entity pushed: " + en)
              false
            }
          } catch {
            case e: Exception => {
              timelineClient = None
              client = getTimelineClient
              logError("Could not handle history entity: " + e)
              true
            }
          }
        }
      }
    }
    logInfo("after pushEntities: " + entityList.size())
  }


  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event
   * @param flush
   * @return
   */
  private def handleEvent(event: TimedEvent,  flush: Boolean): Unit = {
    logInfo("handle event")
    var push = false
    // if we receive a new appStart event, we always push
    // not much contention here, only happens when servcie is stopped
    lock synchronized {
      if (event != null) {
        logInfo("Handle event: " + event)
        val obj = JsonProtocol.sparkEventToJson(event.sparkEvent)
        val map = compact(render(obj))
        if (map == null || map == "") return
        logInfo("event detail: " + map)
        event.sparkEvent match {
          case start: SparkListenerApplicationStart =>
            // we already have all information,
            // flush it for old one to switch to new one
            logInfo("Receive application start event: " + event)
            // flush this entity
            entityList :+= curEntity.getOrElse(null)
            curEntity = None
            appName =start.appName;
            userName = start.sparkUser
            startTime = start.time
            bInit = true
            val en = getCurrentEntity
            en.addPrimaryFilter("startApp", "newApp")
            push = true
          case end: SparkListenerApplicationEnd =>
            if (!bEnd) {
              // we already have all information,
              // flush it for old one to switch to new one
              logInfo("Receive application end event: " + event)
              // flush this entity
              entityList :+= curEntity.getOrElse(null)
              curEntity = None
              bEnd = true
              val en = getCurrentEntity
              en.addPrimaryFilter("endApp", "oldApp")
              en.addOtherInfo("startTime", startTime)
              en.addOtherInfo("endTime", end.time)
              push = true
            }
          case _ =>
        }

        val tlEvent = new TimelineEvent()
        tlEvent.setEventType(Utils.getFormattedClassName(event.sparkEvent).toString)
        tlEvent.setTimestamp(event.time)
        // TODO
        val kvMap = new JHashMap[String, Object]();
        kvMap.put(Utils.getFormattedClassName(event.sparkEvent).toString, map)
        tlEvent.setEventInfo(kvMap)
        getCurrentEntity.addEvent(tlEvent)
        curEventNum += 1
      }
      logInfo("current event num: " + curEventNum)
      if (curEventNum == batchSize || flush || push) {
        entityList :+= curEntity.getOrElse(null)
        // entityList :+= curEntity.get
        curEntity = None
        curEventNum = 0
      }
      flushEntity()
    }
  }
}

object ATSHistoryLoggingService {

  /**
   * Converts a Java object to its equivalent json4s representation.
   */
  def toJValue(obj: Object): JValue = obj match {
    case str: String => JString(str)
    case dbl: java.lang.Double => JDouble(dbl)
    case dec: java.math.BigDecimal => JDecimal(dec)
    case int: java.lang.Integer => JInt(BigInt(int))
    case long: java.lang.Long => JInt(BigInt(long))
    case bool: java.lang.Boolean => JBool(bool)
    case map: JMap[_, _] =>
      val jmap = map.asInstanceOf[JMap[String, Object]]
      JObject(jmap.entrySet().map { e => (e.getKey() -> toJValue(e.getValue())) }.toList)
    case array: JCollection[_] =>
      JArray(array.asInstanceOf[JCollection[Object]].map(o => toJValue(o)).toList)
    case null => JNothing
  }

  /**
   * Converts a JValue into its Java equivalent.
   */
  def toJavaObject(v: JValue): Object = v match {
    case JNothing => null
    case JNull => null
    case JString(s) => s
    case JDouble(num) => java.lang.Double.valueOf(num)
    case JDecimal(num) => num.bigDecimal
    case JInt(num) => java.lang.Long.valueOf(num.longValue)
    case JBool(value) => java.lang.Boolean.valueOf(value)
    case obj: JObject => toJavaMap(obj)
    case JArray(vals) => {
      val list = new JArrayList[Object]()
      vals.foreach(x => list.add(toJavaObject(x)))
      list
    }
  }

  /**
   * Converts a json4s list of fields into a Java Map suitable for serialization by Jackson,
   * which is used by the ATS client library.
   */
  def toJavaMap(obj: JObject): JHashMap[String, Object] = {
    val map = new JHashMap[String, Object]()
    obj.obj.foreach(f => map.put(f._1, toJavaObject(f._2)))
    map
  }

  def toSparkEvent(event: TimelineEvent): SparkListenerEvent = {
    JsonProtocol.sparkEventFromJson(toJValue(event.getEventInfo()))
  }

}
