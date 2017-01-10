/*
package com.quantifind.kafka.core

import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetsResponse}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.coordinator.GroupTopicPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{Matchers => MockitoMatchers, Mockito}
import org.scalatest._

class KafkaOffsetGetterSpec extends FlatSpec with ShouldMatchers {

  trait Fixture {

    val mockedZkClient = Mockito.mock(classOf[ZkClient])
    val mockedZkUtil =  Mockito.mock(classOf[ZkUtils])
    val mockedConsumer = Mockito.mock(classOf[SimpleConsumer])
    val testPartitionLeader = 1

    val offsetGetter = new KafkaOffsetGetter(mockedZkUtil)
    offsetGetter.consumerMap += (testPartitionLeader -> Some(mockedConsumer))
  }

  "KafkaOffsetGetter" should "be able to build offset data for given partition" in new Fixture {

    val testGroup = "testgroup"
    val testTopic = "testtopic"
    val testPartition = 1

    val topicAndPartition = TopicAndPartition(testTopic, testPartition)
    val groupTopicPartition = GroupTopicPartition(testGroup, topicAndPartition)
    val offsetAndMetadata = OffsetAndMetadata(100, "meta", System.currentTimeMillis)
    KafkaOffsetGetter.committedOffsetMap += (groupTopicPartition -> offsetAndMetadata)

    when(mockedZkUtil.getLeaderForPartition(MockitoMatchers.eq(mockedZkClient), MockitoMatchers.eq(testTopic), MockitoMatchers.eq(testPartition)))
      .thenReturn(Some(testPartitionLeader))

    val partitionErrorAndOffsets = Map(topicAndPartition -> PartitionOffsetsResponse(0,Seq(102)))
    val offsetResponse = OffsetResponse(1, partitionErrorAndOffsets)
    when(mockedConsumer.getOffsetsBefore(any[OffsetRequest])).thenReturn(offsetResponse)

    offsetGetter.processPartition(testGroup, testTopic, testPartition) match {
      case Some(offsetInfo) =>
        offsetInfo.topic shouldBe testTopic
        offsetInfo.group shouldBe testGroup
        offsetInfo.partition shouldBe testPartition
        offsetInfo.offset shouldBe 100
        offsetInfo.logSize shouldBe 102
      case None => fail("Failed to build offset data")
    }
    
  }
}
*/