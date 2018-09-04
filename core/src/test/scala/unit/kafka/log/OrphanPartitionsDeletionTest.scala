/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.log

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, MockTime, TestUtils}
import kafka.zk.{ReassignPartitionsZNode, ZkVersion, ZooKeeperTestHarness}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}
import org.apache.kafka.common.{TopicPartition, TopicPartitionReplica}

import scala.collection.Map
import scala.collection.Seq
import scala.util.Random
import java.io.File

import org.apache.kafka.clients.producer.ProducerRecord

class OrphanPartitionsDeletionTest extends ZooKeeperTestHarness with Logging {
  val partitionId = 0
  var servers: Seq[KafkaServer] = null
  val topicName = "my-topic"
  val delayMs = 1000
  val time = new MockTime()

  def zkUpdateDelay(): Unit = Thread.sleep(delayMs)

  @Before
  override def setUp() {
    super.setUp()
  }

  def startBrokers(brokerIds: Seq[Int]) {
    servers = brokerIds.map {
      i =>
        val props = createBrokerConfig(i, zkConnect, enableControlledShutdown = false, logDirCount = 3)
        props.put(KafkaConfig.AutoOrphanPartitionRemovalDelayMsProp, "3600000")
        props
    }.map(c => createServer(KafkaConfig.fromProps(c), this.time))
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  /**
    *
    */
  @Test
  def shouldCleanOrphanPartitions(): Unit = {
    startBrokers(Seq(0, 1, 2))
    createTopic(zkClient, "topic1", Map(0 -> List(0, 1), 1 -> List(1, 2)), servers)
    createTopic(zkClient, "topic2", Map(0 -> List(0, 1), 1 -> List(1, 2)), servers)
    createTopic(zkClient, "topic3", Map(0 -> List(0, 1), 1 -> List(2, 0)), servers)


    val numMessages = 500
    val msgSize = 100 * 1000

    produceMessages("topic1", numMessages, acks = -1, msgSize)
    produceMessages("topic2", numMessages, acks = -1, msgSize)
    produceMessages("topic3", numMessages, acks = -1, msgSize)

    // force a new segment to create multi segments log
    servers.foreach {
      server =>
        server.logManager.allLogs.foreach {
          case (log) =>
            log.roll()
        }
    }

    // create some data on current active segment
    produceMessages("topic1", numMessages, acks = -1, msgSize)
    produceMessages("topic2", numMessages, acks = -1, msgSize)
    produceMessages("topic3", numMessages, acks = -1, msgSize)

    // broker-2 now offline
    servers(2).shutdown()

    val firstMove = Map(
      new TopicPartition("topic1", 0) -> Seq(0, 1), // stay
      new TopicPartition("topic1", 1) -> Seq(1, 0), // move
      new TopicPartition("topic3", 1) -> Seq(1, 0) // move
    )

    // partition reassignment while broker-2 is offline: topic1-1 and topic3-1 become orphan partitions in broker-2
    zkClient.setOrCreatePartitionReassignment(firstMove, ZkVersion.MatchAnyVersion)

    waitForReassignmentToComplete()

    // broker-2 is now back online
    servers(2).startup()

    val broker2LogManager = servers(2).logManager

    val defaultRetentionMs = broker2LogManager.currentDefaultConfig.retentionMs
    val defaultDeleteDelay = broker2LogManager.currentDefaultConfig.fileDeleteDelayMs
    val defaultInitDelay = broker2LogManager.retentionCheckMs + servers(2).logManager.InitialTaskDelayMs


    // wait until first leaderAndISR request is received by broker-2
    waitUntilTrue(() => broker2LogManager.allOrphanLogs.size < broker2LogManager.allLogs.size,
      s"broker2 does not receive any leaderAndISR request", pause = 100L)

    assertTrue(broker2LogManager.orphanPartitionRemovalDelayMs > 0)

    // topic1-1 and topic3-1 are two orphan partitions
    assertEquals("should have 2 orphan logs", 2, broker2LogManager.allOrphanLogs.size)

    // kafka scheduler does not work with mock time. trigger the removal manually
    // segment retention time has not reached yet. No orphan log can be removed.
    broker2LogManager.removeOrphanPartitions()
    assertEquals("should still have 2 orphan logs", 2, broker2LogManager.allOrphanLogs.size)

    time.sleep(defaultRetentionMs + 1)
    time.sleep(defaultDeleteDelay + 1)
    time.sleep(defaultInitDelay + 1)

    // orphan partitions should be removed now
    broker2LogManager.removeOrphanPartitions()
    assertEquals("should have 0 orphan log", 0, broker2LogManager.allOrphanLogs.size)
    assertEquals("should have 1 current log", 1, broker2LogManager.allLogs.size)
  }

  def waitForReassignmentToComplete(pause: Long = 100L) {
    waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      s"Znode ${ReassignPartitionsZNode.path} wasn't deleted", pause = pause)
  }


  private def produceMessages(topic: String, numMessages: Int, acks: Int, valueLength: Int): Unit = {
    val records = (0 until numMessages).map(_ => new ProducerRecord[Array[Byte], Array[Byte]](topic,
      new Array[Byte](valueLength)))
    TestUtils.produceMessages(servers, records, acks)
  }
}
