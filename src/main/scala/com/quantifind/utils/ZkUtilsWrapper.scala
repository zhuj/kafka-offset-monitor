package com.quantifind.utils

import kafka.api.LeaderAndIsr
import kafka.cluster.{Cluster, Broker}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerThreadId
import kafka.controller.{ReassignedPartitionsContext, LeaderIsrAndControllerEpoch}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

import scala.collection.mutable

/*
  This class is mainly to help us mock the ZkUtils class. It is really painful to get powermock to work with scalatest,
  so we created this class with a little help from IntelliJ to auto-generate the delegation code
 */
class ZkUtilsWrapper {

  val ConsumersPath = ZkUtils.ConsumersPath

  val delegator =  ZkUtils

  def getAllPartitions(zkClient: ZkClient): collection.Set[TopicAndPartition] = delegator.getAllPartitions(zkClient)

  def getAllTopics(zkClient: ZkClient): Seq[String] = delegator.getAllTopics(zkClient)

  def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = delegator.getBrokerInfo(zkClient, brokerId)

  def getConsumersPerTopic(zkClient: ZkClient, group: String, excludeInternalTopics: Boolean): mutable.Map[String, List[ConsumerThreadId]] = delegator.getConsumersPerTopic(zkClient, group, excludeInternalTopics)

  def getConsumersInGroup(zkClient: ZkClient, group: String): Seq[String] = delegator.getConsumersInGroup(zkClient, group)

  def deletePartition(zkClient: ZkClient, brokerId: Int, topic: String): Unit = delegator.deletePartition(zkClient, brokerId, topic)

  def getPartitionsUndergoingPreferredReplicaElection(zkClient: ZkClient): collection.Set[TopicAndPartition] = delegator.getPartitionsUndergoingPreferredReplicaElection(zkClient)

  def updatePartitionReassignmentData(zkClient: ZkClient, partitionsToBeReassigned: collection.Map[TopicAndPartition, Seq[Int]]): Unit = delegator.updatePartitionReassignmentData(zkClient, partitionsToBeReassigned)

  def getPartitionReassignmentZkData(partitionsToBeReassigned: collection.Map[TopicAndPartition, Seq[Int]]): String = delegator.getPartitionReassignmentZkData(partitionsToBeReassigned)

  def parseTopicsData(jsonData: String): Seq[String] = delegator.parseTopicsData(jsonData)

  def parsePartitionReassignmentData(jsonData: String): collection.Map[TopicAndPartition, Seq[Int]] = delegator.parsePartitionReassignmentData(jsonData)

  def parsePartitionReassignmentDataWithoutDedup(jsonData: String): Seq[(TopicAndPartition, Seq[Int])] = delegator.parsePartitionReassignmentDataWithoutDedup(jsonData)

  def getPartitionsBeingReassigned(zkClient: ZkClient): collection.Map[TopicAndPartition, ReassignedPartitionsContext] = delegator.getPartitionsBeingReassigned(zkClient)

  def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = delegator.getPartitionsForTopics(zkClient, topics)

  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = delegator.getPartitionAssignmentForTopics(zkClient, topics)

  def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = delegator.getReplicaAssignmentForTopics(zkClient, topics)

  def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topicAndPartitions: collection.Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = delegator.getPartitionLeaderAndIsrForTopics(zkClient, topicAndPartitions)

  def pathExists(client: ZkClient, path: String): Boolean = delegator.pathExists(client, path)

  def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = delegator.getChildrenParentMayNotExist(client, path)

  def getChildren(client: ZkClient, path: String): Seq[String] = delegator.getChildren(client, path)

  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = delegator.readDataMaybeNull(client, path)

  def readData(client: ZkClient, path: String): (String, Stat) = delegator.readData(client, path)

  def maybeDeletePath(zkUrl: String, dir: String): Unit = delegator.maybeDeletePath(zkUrl, dir)

  def deletePathRecursive(client: ZkClient, path: String): Unit = delegator.deletePathRecursive(client, path)

  def deletePath(client: ZkClient, path: String): Boolean = delegator.deletePath(client, path)

  def updateEphemeralPath(client: ZkClient, path: String, data: String): Unit = delegator.updateEphemeralPath(client, path, data)

  def conditionalUpdatePersistentPathIfExists(client: ZkClient, path: String, data: String, expectVersion: Int): (Boolean, Int) = delegator.conditionalUpdatePersistentPathIfExists(client, path, data, expectVersion)

  def conditionalUpdatePersistentPath(client: ZkClient, path: String, data: String, expectVersion: Int, optionalChecker: Option[(ZkClient, String, String) => (Boolean, Int)]): (Boolean, Int) = delegator.conditionalUpdatePersistentPath(client, path, data, expectVersion, optionalChecker)

  def updatePersistentPath(client: ZkClient, path: String, data: String): Unit = delegator.updatePersistentPath(client, path, data)

  def createSequentialPersistentPath(client: ZkClient, path: String, data: String): String = delegator.createSequentialPersistentPath(client, path, data)

  def createPersistentPath(client: ZkClient, path: String, data: String): Unit = delegator.createPersistentPath(client, path, data)

  def createEphemeralPathExpectConflictHandleZKBug(zkClient: ZkClient, path: String, data: String, expectedCallerData: Any, checker: (String, Any) => Boolean, backoffTime: Int): Unit = delegator.createEphemeralPathExpectConflictHandleZKBug(zkClient, path, data, expectedCallerData, checker, backoffTime)

  def createEphemeralPathExpectConflict(client: ZkClient, path: String, data: String): Unit = delegator.createEphemeralPathExpectConflict(client, path, data)

  def makeSurePersistentPathExists(client: ZkClient, path: String): Unit = delegator.makeSurePersistentPathExists(client, path)

  def replicaAssignmentZkData(map: collection.Map[String, Seq[Int]]): String = delegator.replicaAssignmentZkData(map)

  def leaderAndIsrZkData(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = delegator.leaderAndIsrZkData(leaderAndIsr, controllerEpoch)

  def getConsumerPartitionOwnerPath(group: String, topic: String, partition: Int): String = delegator.getConsumerPartitionOwnerPath(group, topic, partition)

  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, port: Int, timeout: Int, jmxPort: Int): Unit = delegator.registerBrokerInZk(zkClient, id, host, port, timeout, jmxPort)

  def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = delegator.getReplicasForPartition(zkClient, topic, partition)

  def getInSyncReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = delegator.getInSyncReplicasForPartition(zkClient, topic, partition)

  def getEpochForPartition(zkClient: ZkClient, topic: String, partition: Int): Int = delegator.getEpochForPartition(zkClient, topic, partition)

  def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = delegator.getLeaderForPartition(zkClient, topic, partition)

  def setupCommonPaths(zkClient: ZkClient): Unit = delegator.setupCommonPaths(zkClient)

  def getLeaderAndIsrForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[LeaderAndIsr] = delegator.getLeaderAndIsrForPartition(zkClient, topic, partition)

  def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = delegator.getAllBrokersInCluster(zkClient)

  def getSortedBrokerList(zkClient: ZkClient): Seq[Int] = delegator.getSortedBrokerList(zkClient)

  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String = delegator.getTopicPartitionLeaderAndIsrPath(topic, partitionId)

  def getTopicPartitionPath(topic: String, partitionId: Int): String = delegator.getTopicPartitionPath(topic, partitionId)

  def getController(zkClient: ZkClient): Int = delegator.getController(zkClient)

  def getDeleteTopicPath(topic: String): String = delegator.getDeleteTopicPath(topic)

  def getTopicConfigPath(topic: String): String = delegator.getTopicConfigPath(topic)

  def getTopicPartitionsPath(topic: String): String = delegator.getTopicPartitionsPath(topic)

  def getTopicPath(topic: String): String = delegator.getTopicPath(topic)

}
