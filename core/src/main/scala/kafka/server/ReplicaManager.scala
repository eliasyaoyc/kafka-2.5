/**
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
package kafka.server

import java.io.File
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.common.RecordValidationException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchMetadata => SFetchMetadata}
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica.{ClientMetadata, _}
import org.apache.kafka.common.requests.DescribeLogDirsResponse.{LogDirInfo, ReplicaInfo}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,
                         lastStableOffset: Option[Long],
                         preferredReadReplica: Option[Int] = None,
                         followerNeedsHwUpdate: Boolean = false,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString =
    s"Fetch Data: [$info], HW: [$highWatermark], leaderLogStartOffset: [$leaderLogStartOffset], leaderLogEndOffset: [$leaderLogEndOffset], " +
    s"followerLogStartOffset: [$followerLogStartOffset], fetchTimeMs: [$fetchTimeMs], readSize: [$readSize], lastStableOffset: [$lastStableOffset], error: [$error]"

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)


/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller indicating
 * that it should be either a leader or follower of a partition.
 */
sealed trait HostedPartition
object HostedPartition {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartition

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final object Offline extends HostedPartition
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
}

class ReplicaManager(val config: KafkaConfig, // 配置管理类
                     metrics: Metrics,  // 监控指标类
                     time: Time, // 定时器类
                     val zkClient: KafkaZkClient,  // ZooKeeper客户端
                     /*
                       定时器，四个定时任务:
                        1.shutdown-idle-replica-alter-log-dirs-thread
                        2.highwatermark-checkpoint
                        3.isr-expiration
                        4.isr-change-propagation
                      */
                     scheduler: Scheduler,
                     val logManager: LogManager,// 对分区的读写操作都委托给底层的日志存储子系统
                     val isShuttingDown: AtomicBoolean, // 是否已经关闭
                     quotaManagers: QuotaManagers,  // 配额管理器
                     val brokerTopicStats: BrokerTopicStats,// Broker主题监控指标类
                     val metadataCache: MetadataCache,// Broker元数据缓存
                     logDirFailureChannel: LogDirFailureChannel,
                     // 延迟操作
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                     threadNamePrefix: Option[String]) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkClient: KafkaZkClient,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           threadNamePrefix: Option[String] = None) {
    this(config, metrics, time, zkClient, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      threadNamePrefix)
  }

  /* epoch of the controller that last changed the leader */
  /*
     controller 的年代信息，当重新选举 controller leader 的时候该字段会递增，ReplicaManager 处理来自 KafkaController 的请求时，
     会先检测请求中携带的年代信息是否等于controllerEpoch 字段的值，避免脑裂
   */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  //当前broker id，主要用于查找 local replica
  private val localBrokerId = config.brokerId
  //当前broker 上分配的所有的partition
  private val allPartitions = new Pool[TopicPartition, HostedPartition](
    valueFactory = Some(tp => HostedPartition.Online(Partition(tp, time, this)))
  )
  private val replicaStateChangeLock = new Object
  //管理多个ReplicaFetcherThread 线程， 用于向leader 副本发送 fetchRequest 请求来获取消息实现 follower 与 leader 副本同步。
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  //用于缓存每个log 目录与 offsetCheckpoint 之间的对应关系，offsetCheckpoint 记录了 对应log目录下的 replication-offset-checkpoint 文件，该文件用于记录了 data目录下每个partition的hw
  @volatile var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

  newGauge("LeaderCount", () => leaderPartitionsIterator.size)
  // Visible for testing
  private[kafka] val partitionCount = newGauge("PartitionCount", () => allPartitions.size)
  newGauge("OfflineReplicaCount", () => offlinePartitionCount)
  newGauge("UnderReplicatedPartitions", () => underReplicatedPartitionCount)
  newGauge("UnderMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  newGauge("AtMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isAtMinIsr))
  newGauge("ReassigningPartitions", () => reassigningPartitionsCount)

  def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition): Unit = {
    isrChangeSet synchronized {
      isrChangeSet += topicPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges(): Unit = {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    val completed = delayedElectLeaderPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log diretory failure if IBP < 1.0
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasOnlinePartition = allPartitions.values.exists {
      case HostedPartition.Online(partition) => topic == partition.topic
      case HostedPartition.None | HostedPartition.Offline => false
    }
    if (!topicHasOnlinePartition)
      brokerTopicStats.removeMetrics(topic)
  }

  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean)  = {
    stateChangeLogger.trace(s"Handling stop replica (delete=$deletePartition) for partition $topicPartition")

    //③ 根据 deletePartition 的值 决定是否对log进行删除
    if (deletePartition) {
      getPartition(topicPartition) match {
        case HostedPartition.Offline =>
          throw new KafkaStorageException(s"Partition $topicPartition is on an offline disk")

        case hostedPartition @ HostedPartition.Online(removedPartition) =>
          if (allPartitions.remove(topicPartition, hostedPartition)) {
            maybeRemoveTopicMetrics(topicPartition.topic)
            // this will delete the local log. This call may throw exception if the log is on offline directory
            removedPartition.delete() // 删除副本，log会被删除
          }

        case HostedPartition.None =>
          stateChangeLogger.trace(s"Ignoring stop replica (delete=$deletePartition) for partition " +
            s"$topicPartition as replica doesn't exist on broker")
      }

      // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
      // This could happen when topic is being deleted while broker is down and recovers.
      if (logManager.getLog(topicPartition).isDefined)
        logManager.asyncDelete(topicPartition)
      if (logManager.getLog(topicPartition, isFuture = true).isDefined)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }

    // If we were the leader, we may have some operations still waiting for completion.
    // We force completion to prevent them from timing out.
    completeDelayedFetchOrProduceRequests(topicPartition)

    stateChangeLogger.trace(s"Finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
  }

  private def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      //① 检查请求中的 controllerEpoch 值
      if (stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Received stop replica request from an old controller epoch " +
          s"${stopReplicaRequest.controllerEpoch}. Latest known controller epoch is $controllerEpoch")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala.toSet
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(partitions)
        replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)
        for (topicPartition <- partitions){
          try {
            // ② 停止指定分区的同步操作
            stopReplica(topicPartition, stopReplicaRequest.deletePartitions)
            responseMap.put(topicPartition, Errors.NONE)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Ignoring stop replica (delete=${stopReplicaRequest.deletePartitions}) for " +
                s"partition $topicPartition due to storage exception", e)
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
          }
        }
        (responseMap, Errors.NONE)
      }
    }
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
  }

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  // An iterator over all non offline partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  private def nonOfflinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartition.Offline)
  }

  def getPartitionOrException(topicPartition: TopicPartition, expectLeader: Boolean): Partition = {
    getPartitionOrError(topicPartition, expectLeader) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition, expectLeader: Boolean): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        if (expectLeader) {
          // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER which
          // forces clients to refresh metadata to find the new location. This can happen, for example,
          // during a partition reassignment if a produce request from the client is sent to a broker after
          // the local replica has been deleted.
          Left(Errors.NOT_LEADER_FOR_PARTITION)
        } else {
          Left(Errors.REPLICA_NOT_AVAILABLE)
        }

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition, expectLeader = false).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition, expectLeader = false).futureLocalLogOrException
  }

  def localLog(topicPartition: TopicPartition): Option[Log] = {
    nonOfflinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localLog(topicPartition).map(_.dir.getParent)
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   *
   * 需要副本写入的场景有 4 个：
   * 1. 生产者向 Leader 副本写入消息
   * 2. Follower 副本拉取消息后写入副本
   * 3. 消费者组写入组信息
   * 4. 事务管理器写入事务消息（事务标记、事务元数据）
   */
  def appendRecords(timeout: Long, // 请求处理超时时间
                    requiredAcks: Short, // 请求acks设置
                    internalTopicsAllowed: Boolean, // 是否允许写入内部主题
                    origin: AppendOrigin, // 写入方来源
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],// 需要添加的条目，key是key-partition，value 是memoryRecords
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit, // 回调逻辑
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()): Unit = {
    // requiredAcks合法取值是-1，0，1，否则视为非法
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      //重点方法：添加到本地log中  调用appendToLocalLog方法写入消息集合到本地日志
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        origin, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset  设置下一条待写入消息的位移值
                  // 构建PartitionResponse封装写入结果
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime,
                    result.info.logStartOffset, result.info.recordErrors.asJava, result.info.errorMessage)) // response status
      }

      // 尝试更新消息格式转换的指标数据
      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

      //ack 设置的如果是 -1 0 那么放入延迟队列。
      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        // 封装成 delayProduce
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        // 再一次尝试完成该延时请求
        // 如果暂时无法完成，则将对象放入到相应的Purgatory中等待后续处理
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else { // 无需等待其他副本写入完成，可以立即发送Response
        // we can respond immediately
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        // 调用回调逻辑然后返回即可
        responseCallback(produceResponseStatus)
      }
    } else { // 如果requiredAcks值不合法
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      // 构造INVALID_REQUIRED_ACKS异常并封装进回调函数调用中
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition, expectLeader = true)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (Log.logFutureDirName(topicPartition).size > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with ReplicaNotAvailableException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw ReplicaNotAvailableException if replica does not exist for the given partition
          val partition = getPartitionOrException(topicPartition, expectLeader = false)
          partition.localLogOrException

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            (topicPartition, Errors.forException(e))
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): Map[String, LogDirInfo] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.dir.getParent)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val replicaInfos = logs.filter { log =>
              partitions.contains(log.topicPartition)
            }.map { log =>
              log.topicPartition -> new ReplicaInfo(log.size, getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture), log.isFuture)
            }.toMap

            (absolutePath, new LogDirInfo(Errors.NONE, replicaInfos.asJava))
          case None =>
            (absolutePath, new LogDirInfo(Errors.NONE, Map.empty[TopicPartition, ReplicaInfo].asJava))
        }

      } catch {
        case _: KafkaStorageException =>
          (absolutePath, new LogDirInfo(Errors.KAFKA_STORAGE_ERROR, Map.empty[TopicPartition, ReplicaInfo].asJava))
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          (absolutePath, new LogDirInfo(Errors.forException(t), Map.empty[TopicPartition, ReplicaInfo].asJava))
      }
    }.toMap
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsResponse.PartitionResponse] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsResponse.PartitionResponse(result.lowWatermark, result.error)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {

    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = getPartition(topicPartition) match {
        case HostedPartition.Online(partition) => partition.logStartOffset
        case HostedPartition.None | HostedPartition.Offline => -1L
      }
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    trace(s"Append [$entriesPerPartition] to local log")
    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      // 是否是内部topic
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          //获取partition
          val partition = getPartitionOrException(topicPartition, expectLeader = true)
          //如果该partition不是leader 则发送请求到leader partition 上
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks)
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
            s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(
              logStartOffset, recordErrors, rve.invalidException.getMessage), Some(rve.invalidException)))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel,
                    clientMetadata: Option[ClientMetadata]): Unit = {
    //检查是否是follower 来发的fetch 请求，用于进行同步
    val isFromFollower = Request.isValidBrokerId(replicaId)
    //consumer 用来进行消费
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    // 根据请求发送方判断可读取范围
    // 如果请求来自于普通消费者，那么可以读到高水位值
    // 如果请求来自于配置了READ_COMMITTED的消费者，那么可以读到Log Stable Offset值
    // 如果请求来自于Follower副本，那么可以读到LEO值
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted
    else
      FetchHighWatermark

    // Restrict fetching to leader if request is from follower or from a client with older version (no ClientMetadata)
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      //从log 中读取消息
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        fetchIsolation = fetchIsolation,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        clientMetadata = clientMetadata)
      /*
         用来处理 follower 副本的 fetchRequest 请求，主要做下面4件事情
         1. 在leader 中维护了 follower 副本的各种状态，这里会更新对应 follower 副本的状态、例如，更新leo 等
         2. 检查是否需要对ISR集合进行扩张，如果ISR集合发生变化，则将新的 ISR 集合的记录保存到 zookeeper 种
         3. 检测是否后移 leader 的hw
         4. 检测 delayedProducePurgatory 中相关key 对应的 DelayedProduce，满足条件则将其执行完成。
       */
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }

    val logReadResults = readFromLog()

    // check if this fetch request can be satisfied right away
    //统计从 log 中读取的字节总数
    var bytesReadable: Long = 0
    //统计再从log种读取消息的时候，是否发生了异常
    var errorReadingData = false
    val logReadResultMap = new mutable.HashMap[TopicPartition, LogReadResult]
    var anyPartitionsNeedHwUpdate = false
    // 统计总共可读取的字节数
    logReadResults.foreach { case (topicPartition, logReadResult) =>
      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicPartition, logReadResult)
      if (isFromFollower && logReadResult.followerNeedsHwUpdate) {
        anyPartitionsNeedHwUpdate = true
      }
    }

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) any of the requested partitions need HW update

    /*
       判断是否能够立即返回FetchResponse
       1) FetchRequest 的 timeout <= 0 ，即消费者或者 follower 副本不希望等待
       2) FetchRequest 没有指定要读取的分区， 即 fetchInfos.size <= 0
       3) 已经读取了足够的数据，即 bytesReadable >= fetchMinBytes
       4) 在读取过程中发生了异常，即检查 errorReadingData
       5) 请求来自follower的fetch，要更新hw
     */
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData || anyPartitionsNeedHwUpdate) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions, result.preferredReadReplica, isFromFollower && isAddingReplica(tp, replicaId))
      }
      //直接调用回调函数，生成并发送 fetchResponse
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicPartition, FetchPartitionStatus)]
      //对读取的结果进行转换
      fetchInfos.foreach { case (topicPartition, partitionData) =>
        logReadResultMap.get(topicPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }
      val fetchMetadata: SFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
        fetchOnlyFromLeader, fetchIsolation, isFromFollower, replicaId, fetchPartitionStatus)
      //创建delayedFetch 对象
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, clientMetadata,
        responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      //尝试完成，否则放入 delayedFetchPurgatory 中进行管理
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       fetchIsolation: FetchIsolation,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, LogReadResult)] = {

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        val partition = getPartitionOrException(tp, expectLeader = fetchOnlyFromLeader)
        val fetchTimeMs = time.milliseconds

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = clientMetadata.flatMap(
          metadata => findPreferredReadReplica(tp, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach{ selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for $clientMetadata")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        } else {
          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          val readInfo: LogReadInfo = partition.readRecords(
            fetchOffset = fetchInfo.fetchOffset,
            currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
            maxBytes = adjustedMaxBytes,
            fetchIsolation = fetchIsolation,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            minOneMessage = minOneMessage)

          // Check if the HW known to the follower is behind the actual HW
          val followerNeedsHwUpdate: Boolean = partition.getReplica(replicaId)
            .exists(replica => replica.lastSentHighWatermark < readInfo.highWatermark)

          val fetchDataInfo = if (shouldLeaderThrottle(quota, tp, replicaId)) {
            // If the partition is being throttled, simply return an empty set.
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else if (!hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            readInfo.fetchedData
          }

          LogReadResult(info = fetchDataInfo,
            highWatermark = readInfo.highWatermark,
            leaderLogStartOffset = readInfo.logStartOffset,
            leaderLogEndOffset = readInfo.logEndOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = fetchTimeMs,
            readSize = adjustedMaxBytes,
            lastStableOffset = Some(readInfo.lastStableOffset),
            preferredReadReplica = preferredReadReplica,
            followerNeedsHwUpdate = followerNeedsHwUpdate,
            exception = None)
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = Request.describeReplicaId(replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            lastStableOffset = None,
            exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(tp: TopicPartition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    val partition = getPartitionOrException(tp, expectLeader = false)

    if (partition.isLeader) {
      if (Request.isValidBrokerId(replicaId)) {
        // Don't look up preferred for follower fetches via normal replication
        Option.empty
      } else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(tp, new ListenerName(clientMetadata.listenerName))
          var replicaInfoSet: Set[ReplicaView] = partition.remoteReplicas
            // Exclude replicas that don't have the requested offset (whether or not if they're in the ISR)
            .filter(replica => replica.logEndOffset >= fetchOffset)
            .filter(replica => replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))
            .toSet

          if (partition.leaderReplicaIdOpt.isDefined) {
            val leaderReplica: ReplicaView = partition.leaderReplicaIdOpt
              .map(replicaId => replicaEndpoints.getOrElse(replicaId, Node.noNode()))
              .map(leaderNode => new DefaultReplicaView(leaderNode, partition.localLogOrException.logEndOffset, 0L))
              .get
            replicaInfoSet ++= Set(leaderReplica)

            val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
            replicaSelector.select(tp, clientMetadata, partitionInfo).asScala
              .filter(!_.endpoint.isEmpty)
              // Even though the replica selector can return the leader, we don't want to send it out with the
              // FetchResponse, so we exclude it here
              .filter(!_.equals(leaderReplica))
              .map(_.endpoint.id)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val isReplicaInSync = nonOfflinePartition(topicPartition).exists(_.inSyncReplicaIds.contains(replicaId))
    !isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.messageFormatVersion.recordVersion.value)

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        val deletedPartitions = metadataCache.updateMetadata(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  // 具体处理 LeaderAndIsrRequest 请求的地方，同时也是副本管理器添加分区的地方。
  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    if (stateChangeLogger.isTraceEnabled) {
      leaderAndIsrRequest.partitionStates.asScala.foreach { partitionState =>
        stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
          s"correlation id $correlationId from controller ${leaderAndIsrRequest.controllerId} " +
          s"epoch ${leaderAndIsrRequest.controllerEpoch}")
      }
    }
    replicaStateChangeLock synchronized {//加锁
      // 如果LeaderAndIsrRequest携带的Controller Epoch
      // 小于当前Controller的Epoch值
      if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller ${leaderAndIsrRequest.controllerId} with " +
          s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
          s"Latest known controller epoch is $controllerEpoch")
        // 说明Controller已经易主，抛出相应异常
        leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
      } else {
        //统计返回的错误码
        val responseMap = new mutable.HashMap[TopicPartition, Errors]
        val controllerId = leaderAndIsrRequest.controllerId
        // 更新当前Controller Epoch值
        controllerEpoch = leaderAndIsrRequest.controllerEpoch

        // First check partition's leader epoch
        //进行一些准备工作，统计进行切换需要使用的信息
        val partitionStates = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()
        val newPartitions = new mutable.HashSet[Partition]

        leaderAndIsrRequest.partitionStates.asScala.foreach { partitionState =>
          val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          //获取partition对象,找不到就创建新的 partition 对象
          val partitionOpt = getPartition(topicPartition) match {
            // 如果是Offline状态
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              // 添加对象异常到Response，并设置分区对象变量partitionOpt=None
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
              None

            // 如果是Online状态，直接赋值partitionOpt即可
            case HostedPartition.Online(partition) => Some(partition)

            // 如果是None状态，则表示没有找到分区对象
            // 那么创建新的分区对象将，新创建的分区对象加入到allPartitions统一管理
            // 然后赋值partitionOpt字段
            case HostedPartition.None =>
              val partition = Partition(topicPartition, time, this)
              allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
              newPartitions.add(partition)
              Some(partition)
          }

          /*
          首先，比较 LeaderAndIsrRequest 携带的 Controller Epoch 值和当前 Controller Epoch 值。
          如果发现前者小于后者，说明 Controller 已经变更到别的 Broker 上了，需要构造一个 STALE_CONTROLLER_EPOCH 异常并封装进 Response 返回。
          否则，代码进入 else 分支。然后，becomeLeaderOrFollower 方法会更新当前缓存的 Controller Epoch 值，再提取出 LeaderAndIsrRequest 请求中涉及到的分区，
          之后依次遍历这些分区，并执行下面的两步逻辑。
           */
          partitionOpt.foreach { partition =>
            val currentLeaderEpoch = partition.getLeaderEpoch
            val requestLeaderEpoch = partitionState.leaderEpoch
            //检查 leaderEpoch
            if (requestLeaderEpoch > currentLeaderEpoch) {
              // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
              // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
              //判断该分区的副本是否被分配到了当前的 broker
              if (partitionState.replicas.contains(localBrokerId))
                partitionStates.put(partition, partitionState)
              else {
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                  s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                  s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
              }
            } else if (requestLeaderEpoch < currentLeaderEpoch) {
              stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                s"leader epoch $requestLeaderEpoch is smaller than the current " +
                s"leader epoch $currentLeaderEpoch")
              responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
            } else {
              stateChangeLogger.debug(s"Ignoring LeaderAndIsr request from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                s"leader epoch $requestLeaderEpoch matches the current leader epoch")
              responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
            }
          }
        }

        // 根据 partitionState 中指定的角色进行分类 确定Broker上副本是哪些分区的Leader副本
        val partitionsTobeLeader = partitionStates.filter { case (_, partitionState) =>
          partitionState.leader == localBrokerId
        }
        // 确定Broker上副本是哪些分区的Follower副本
        val partitionsToBeFollower = partitionStates -- partitionsTobeLeader.keys

        val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        //将指定分区的副本切换成leader 副本
        val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
        // 调用makeLeaders方法为partitionsToBeLeader所有分区
        // 执行"成为Leader副本"的逻辑
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap,
            highWatermarkCheckpoints)
        else
          Set.empty[Partition]
        //将指定分区的副本切换成follower 副本
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
            highWatermarkCheckpoints)
        else
          Set.empty[Partition]

        /*
         * KAFKA-8392
         * For topic partitions of which the broker is no longer a leader, delete metrics related to
         * those topics. Note that this means the broker stops being either a replica or a leader of
         * partitions of said topics
         */
        val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
        val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
        // 对于当前Broker成为Follower副本的主题
        // 移除它们之前的Leader副本监控指标
        followerTopicSet.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

        // remove metrics for brokers which are not followers of a topic
        // 对于当前Broker成为Leader副本的主题
        // 移除它们之前的Follower副本监控指
        leaderTopicSet.diff(followerTopicSet).foreach(brokerTopicStats.removeOldFollowerMetrics)

        leaderAndIsrRequest.partitionStates.asScala.foreach { partitionState =>
          val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          /*
           * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
           * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
           * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
           * we need to map this topic-partition to OfflinePartition instead.
           */
          if (localLog(topicPartition).isEmpty)
            markPartitionOffline(topicPartition)
        }

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions

        // 启动高水位检查点专属线程
        // 定期将Broker上所有非Offline分区的高水位值写入到检查点文件
        startHighWatermarkCheckPointThread()

        val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
        for (partition <- newPartitions) {
          val topicPartition = partition.topicPartition
          if (logManager.getLog(topicPartition, isFuture = true).isDefined) {
            partition.log.foreach { log =>
              val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

              // Add future replica to partition's map
              partition.createLogIfNotExists(Request.FutureLocalReplicaId, isNew = false, isFutureReplica = true,
                highWatermarkCheckpoints)

              // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
              // replica from source dir to destination dir
              logManager.abortAndPauseCleaning(topicPartition)

              futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(leader,
                partition.getLeaderEpoch, log.highWatermark))
            }
          }
        }
        replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)

        //关闭ReplicaFetcherManagerIdle 中空闲的 fetcher 线程
        replicaFetcherManager.shutdownIdleFetcherThreads()
        // 关闭空闲日志路径数据迁移线程
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        //回调函数
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        // 构造LeaderAndIsrRequest请求的Response并返回
        val responsePartitions = responseMap.iterator.map { case (tp, error) =>
          new LeaderAndIsrPartitionError()
            .setTopicName(tp.topic)
            .setPartitionIndex(tp.partition)
            .setErrorCode(error.code)
        }.toBuffer
        new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
          .setErrorCode(Errors.NONE.code)
          .setPartitionErrors(responsePartitions.asJava))
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints): Set[Partition] = {
    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
        s"partition ${partition.topicPartition}")
    }

    for (partition <- partitionStates.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      // Update the partition information to be the leader
      partitionStates.foreach { case (partition, partitionState) =>
        try {
          //① 调用 partition.makeLeader 方法使得当前 replica 成为 leader
          if (partition.makeLeader(controllerId, partitionState, correlationId, highWatermarkCheckpoints)) {
            partitionsToMakeLeaders += partition
            stateChangeLogger.trace(s"Stopped fetchers as part of become-leader request from " +
              s"controller $controllerId epoch $controllerEpoch with correlation id $correlationId for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch})")
          } else
            stateChangeLogger.info(s"Skipped the become-leader state change after marking its " +
              s"partition as leader with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} (last update controller epoch ${partitionState.controllerEpoch}) " +
              s"since it is already the leader for the partition.")
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
    }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            controllerEpoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors],
                            highWatermarkCheckpoints: OffsetCheckpoints) : Set[Partition] = {
    partitionStates.foreach { case (partition, partitionState) =>
      stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
        s"${partitionState.leader}")
    }

    for (partition <- partitionStates.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionStates.foreach { case (partition, partitionState) =>
        val newLeaderBrokerId = partitionState.leader
        try {
          metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
            // Only change partition state when the leader is available
            case Some(_) =>
              if (partition.makeFollower(controllerId, partitionState, correlationId, highWatermarkCheckpoints))
                partitionsToMakeFollower += partition
              else
                stateChangeLogger.info(s"Skipped the become-follower state change after marking its partition as " +
                  s"follower with correlation id $correlationId from controller $controllerId epoch $controllerEpoch " +
                  s"for partition ${partition.topicPartition} (last update " +
                  s"controller epoch ${partitionState.controllerEpoch}) " +
                  s"since the new leader $newLeaderBrokerId is the same as the old leader")
            case None =>
              // The leader broker should always be present in the metadata cache.
              // If not, we should record the error message and abort the transition process for this partition
              stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
                s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
                s"(last update controller epoch ${partitionState.controllerEpoch}) " +
                s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
              // Create the local replica even if the leader is unavailable. This is required to ensure that we include
              // the partition's high watermark in the checkpoint file (see KAFKA-1647)
              partition.createLogIfNotExists(localBrokerId, isNew = partitionState.isNew, isFutureReplica = false,
                highWatermarkCheckpoints)
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower for partition $partition with leader " +
              s"$newLeaderBrokerId in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
          s"epoch $controllerEpoch with correlation id $correlationId for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

      partitionsToMakeFollower.foreach { partition =>
        completeDelayedFetchOrProduceRequests(partition.topicPartition)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(s"Truncated logs and checkpointed recovery boundaries for partition " +
          s"${partition.topicPartition} as part of become-follower request with correlation id $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch with leader ${partitionStates(partition).leader}")
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
            s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
            s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
            "since it is shutting down")
        }
      } else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leader = metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get
            .brokerEndPoint(config.interBrokerListenerName)
          val fetchOffset = partition.localLogOrException.highWatermark
          partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
       }.toMap

        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
        partitionsToMakeFollowerWithLeaderAndOffset.foreach { case (partition, initialFetchState) =>
          stateChangeLogger.trace(s"Started fetcher to new leader as part of become-follower " +
            s"request from controller $controllerId epoch $controllerEpoch with correlation id $correlationId for " +
            s"partition $partition with leader ${initialFetchState.leader}")
        }
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
        s"${partitionStates(partition).leader}")
    }

    partitionsToMakeFollower
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.keys.foreach { topicPartition =>
      nonOfflinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  /**
   * Update the follower's fetch state on the leader based on the last fetch request and update `readResult`.
   * If the follower replica is not recognized to be one of the assigned replicas, do not update
   * `readResult` so that log start/end offset and high watermark is consistent with
   * records in fetch response. Log start/end offset and high watermark may change not only due to
   * this fetch request, e.g., rolling new log segment and removing old log segment may move log
   * start offset further than the last offset in the fetched records. The followers will get the
   * updated leader's state in the next fetch response.
   */
  private def updateFollowerFetchState(followerId: Int,
                                       readResults: Seq[(TopicPartition, LogReadResult)]): Seq[(TopicPartition, LogReadResult)] = {
    readResults.map { case (topicPartition, readResult) =>
      val updatedReadResult = if (readResult.error != Errors.NONE) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned error ${readResult.error}")
        readResult
      } else {
        nonOfflinePartition(topicPartition) match {
          case Some(partition) =>
            if (partition.updateFollowerFetchState(followerId,
              followerFetchOffsetMetadata = readResult.info.fetchOffsetMetadata,
              followerStartOffset = readResult.followerLogStartOffset,
              followerFetchTimeMs = readResult.fetchTimeMs,
              leaderEndOffset = readResult.leaderLogEndOffset,
              lastSentHighwatermark = readResult.highWatermark)) {
              readResult
            } else {
              warn(s"Leader $localBrokerId failed to record follower $followerId's position " +
                s"${readResult.info.fetchOffsetMetadata.messageOffset}, and last sent HW since the replica " +
                s"is not recognized to be one of the assigned replicas ${partition.assignmentState.replicas.mkString(",")} " +
                s"for partition $topicPartition. Empty records will be returned for this partition.")
              readResult.withEmptyFetchInfo
            }
          case None =>
            warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
            readResult
        }
      }
      topicPartition -> updatedReadResult
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    nonOfflinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    nonOfflinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    val localLogs = nonOfflinePartitionsIterator.flatMap { partition =>
      val logsList: mutable.Set[Log] = mutable.Set()
      partition.log.foreach(logsList.add)
      partition.futureLog.foreach(logsList.add)
      logsList
    }.toBuffer
    val logsByDir = localLogs.groupBy(_.dir.getParent)
    for ((dir, logs) <- logsByDir) {
      val hwms = logs.map(log => log.topicPartition -> log.highWatermark).toMap
      try {
        highWatermarkCheckpoints.get(dir).foreach(_.write(hwms))
      } catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $dir", e)
      }
    }
  }

  // Used only by test
  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartition.Offline)
    Partition.removeMetrics(tp)
  }

  // logDir should be an absolute path
  // sendZkNotification is needed for unit test
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = nonOfflinePartitionsIterator.filter { partition =>
        partition.log.exists { _.dir.getParent == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = nonOfflinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.dir.getParent == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification)
      zkClient.propagateLogDirEvent(localBrokerId)
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
    removeMetric("AtMinIsrPartitionCount")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
    }
  }

  def lastOffsetForLeaderEpoch(requestedEpochInfo: Map[TopicPartition, OffsetsForLeaderEpochRequest.PartitionData]): Map[TopicPartition, EpochEndOffset] = {
    requestedEpochInfo.map { case (tp, partitionData) =>
      val epochEndOffset = getPartition(tp) match {
        case HostedPartition.Online(partition) =>
          partition.lastOffsetForLeaderEpoch(partitionData.currentLeaderEpoch, partitionData.leaderEpoch,
            fetchOnlyFromLeader = true)

        case HostedPartition.Offline =>
          new EpochEndOffset(Errors.KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None if metadataCache.contains(tp) =>
          new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None =>
          new EpochEndOffset(Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
      tp -> epochEndOffset
    }
  }

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }

      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // There are no partitions actually being elected, so return immediately
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
  }
}
