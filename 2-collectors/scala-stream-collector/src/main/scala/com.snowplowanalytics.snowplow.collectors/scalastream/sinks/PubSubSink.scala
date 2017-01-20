/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors
package scalastream
package sinks

// Java
import java.util.Properties
import java.io.FileInputStream

// PubSub
import com.google.cloud.ByteArray
import com.google.cloud.pubsub.{PubSub, PubSubOptions, Topic, TopicInfo, Message}
import com.google.auth.oauth2.GoogleCredentials

// Config
import com.typesafe.config.Config

// Logging
import org.slf4j.LoggerFactory

/**
 * PubSub Sink for the Scala collector
 */
class PubSubSink(config: CollectorConfig, inputType: InputType.InputType) extends AbstractSink {

  import log.{error, debug, info, trace}

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000L

  private val topicName = inputType match {
    case InputType.Good => config.pubsubTopicGoodName
    case InputType.Bad  => config.pubsubTopicBadName
  }

  private var pubSubTopic = createTopic

  /**
   * Creates a new PubSub Topic with the given
   * configuration options
   *
   * @return a new PubSub Topic
   */
  private def createTopic: Topic = {
    val pubsubOptions = if (config.googleAuthPath == "env") {
        PubSubOptions.getDefaultInstance()
    } else {
        val optBuilder = PubSubOptions.newBuilder()
        optBuilder.setProjectId(config.googleProjectId)
        optBuilder.setCredentials(GoogleCredentials.fromStream(new FileInputStream(config.googleAuthPath)))
        optBuilder.build()
    }
    val pubsub = pubsubOptions.getService()
    pubsub.getTopic(topicName)
  }

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key The partition key to use
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    debug(s"Writing ${events.size} Thrift records to PubSub topic ${topicName}")
    events foreach {
      event => {
        try {
          pubSubTopic.publish(Message.of(ByteArray.copyFrom(event)))
        } catch {
          case e: Exception => {
            error(s"Unable to send event, see PubSub log for more details: ${e.getMessage}")
            e.printStackTrace()
          }
        }
      }
    }
    Nil
  }

  override def getType = Sink.PubSub
}
