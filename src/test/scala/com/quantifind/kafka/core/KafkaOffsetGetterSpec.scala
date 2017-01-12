package com.quantifind.kafka.core

import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetsResponse}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.coordinator.GroupTopicPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ZkUtils
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{Mockito, Matchers => MockitoMatchers}
import org.scalatest._

class KafkaOffsetGetterSpec extends FlatSpec with ShouldMatchers {

  trait Fixture {

    val mockedZkUtil =  Mockito.mock(classOf[ZkUtils])
    val mockedConsumer = Mockito.mock(classOf[SimpleConsumer])
    val testPartitionLeader = 1

    val args = new OffsetGetterArgs

    val offsetGetter = new KafkaOffsetGetter(args)
    offsetGetter.consumerMap += (testPartitionLeader -> Some(mockedConsumer))
  }

  "KafkaOffsetGetter" should "be able to build offset data for given partition" in new Fixture {

    val testGroup = "testgroup"
    val testTopic = "testtopic"
    val testPartition = 1
    val committedOffset = 100
    val logEndOffset = 102

    val topicAndPartition = TopicAndPartition(testTopic, testPartition)
    val groupTopicPartition = GroupTopicPartition(testGroup, topicAndPartition)
    val offsetAndMetadata = OffsetAndMetadata(committedOffset, "meta", System.currentTimeMillis)
    KafkaOffsetGetter.committedOffsetMap += (groupTopicPartition -> offsetAndMetadata)

    //topicPartitionOffsetsMap
    KafkaOffsetGetter.topicPartitionOffsetsMap += (topicAndPartition -> logEndOffset)

    when(mockedZkUtil.getLeaderForPartition(MockitoMatchers.eq(testTopic), MockitoMatchers.eq(testPartition)))
      .thenReturn(Some(testPartitionLeader))

    val partitionErrorAndOffsets = Map(topicAndPartition -> PartitionOffsetsResponse(0,Seq(logEndOffset)))
    val offsetResponse = OffsetResponse(1, partitionErrorAndOffsets)
    when(mockedConsumer.getOffsetsBefore(any[OffsetRequest])).thenReturn(offsetResponse)

    offsetGetter.processPartition(testGroup, testTopic, testPartition) match {
      case Some(offsetInfo) =>
        offsetInfo.topic shouldBe testTopic
        offsetInfo.group shouldBe testGroup
        offsetInfo.partition shouldBe testPartition
        offsetInfo.offset shouldBe committedOffset
        offsetInfo.logSize shouldBe logEndOffset
      case None => fail("Failed to build offset data")
    }
    
  }
}