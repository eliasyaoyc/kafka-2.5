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

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest.PartitionData

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString: String = {
    "[startOffsetMetadata: " + startOffsetMetadata +
      ", fetchInfo: " + fetchInfo +
      "]"
  }
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchMaxBytes: Int,
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean,
                         fetchIsolation: FetchIsolation,
                         isFromFollower: Boolean,
                         replicaId: Int,
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) {

  override def toString = "FetchMetadata(minBytes=" + fetchMinBytes + ", " +
    "maxBytes=" + fetchMaxBytes + ", " +
    "onlyLeader=" + fetchOnlyLeader + ", " +
    "fetchIsolation=" + fetchIsolation + ", " +
    "replicaId=" + replicaId + ", " +
    "partitionStatus=" + fetchPartitionStatus + ")"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   quota: ReplicaQuota,
                   clientMetadata: Option[ClientMetadata],
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: The replica is no longer available on this broker
   * Case C: This broker does not know of some partitions it tries to fetch
   * Case D: The partition is in an offline log directory on this broker
   * Case E: This broker is the leader, but the requested epoch is now fenced
   * Case F: The fetch offset locates not on the last segment of the log
   * Case G: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case H: The high watermark on this broker has changed within a FetchSession, need to propagate to follower (KIP-392)
   * Upon completion, should return whatever data is available for each valid partition
   *
   * A) 发生副本迁移，当前节点不在是该分区的leader 副本所在的节点
   * B) 当前broker 没有可用的副本
   * C) 当前broker 找不到需要读取的分区
   * D) 分区在当前broker的脱机日志目录中
   * E) 当前broker 是leader，但是他的 leader epoch 是旧的，也就是他发生了分布式分区错误
   * F) 开始读取的offset 不在 activeSegment 中，可能是发生了 log 截断，也有可能是发生了 roll操作产生了新的activeSegment
   * G) 累计读取的字节数超过最小字节数限制
   * H) 当前 broker 的 hw 被 FetchSession 改变了。需要传播给 follower
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0
    //遍历 fetchMetadata 中的所有 partition 的状态
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        //获取前面读取log时的结束位置
        val fetchOffset = fetchStatus.startOffsetMetadata
        //leader epoch
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            // 查找partition ，找不到就会抛出异常
            val partition = replicaManager.getPartitionOrException(topicPartition,
              expectLeader = fetchMetadata.fetchOnlyLeader)
            val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, fetchMetadata.fetchOnlyLeader)

            //更新对应的 LEO HW
            val endOffset = fetchMetadata.fetchIsolation match {
              case FetchLogEnd => offsetSnapshot.logEndOffset
              case FetchHighWatermark => offsetSnapshot.highWatermark
              case FetchTxnCommitted => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case G if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case F.

            // 检查上次读取后 endOffset 是否发生变化，如果没改变，之前读不到足够的数据现在还是读不到，即任务条件依然不满足，
            // 如果变了，则继续下面的检查，看是否真正满足任务执行条件
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case F, this can happen when the new fetch operation is on a truncated leader
                // 情况 F，endOffset 出现减少的情况，跑到了 baseOffset 较小的 Segment 上了，可能是 Leader 副本的 Log 出现了 truncate 操作
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case F, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                //情况 F, 此时 fetchOffset 虽然依然在 endOffset 之前，但是产生了新的 activeSegment ，
                //fetchOffset 在交旧的LogSegment ，而 endOffset 在 activeSegment 中
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                // endOffset 和 fetchOffset 在同一个 LogSegment 中， 且endOffset 向后移动，那就尝试计算累计的字节数
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }

            if (fetchMetadata.isFromFollower) {
              // Case H check if the follower has the latest HW from the leader
              // 情况 h ， hw 不一致了
              if (partition.getReplica(fetchMetadata.replicaId)
                .exists(r => offsetSnapshot.highWatermark.messageOffset > r.lastSentHighWatermark)) {
                return forceComplete()
              }
            }
          }
        } catch {
          case _: NotLeaderForPartitionException =>  // Case A
            debug(s"Broker is no longer the leader of $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: ReplicaNotAvailableException =>  // Case B
            debug(s"Broker no longer has a replica of $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case C
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: KafkaStorageException => // Case D
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case E
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
        }
    }

    // Case G
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
       forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    //重新从 log 中读取数据
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      fetchIsolation = fetchMetadata.fetchIsolation,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      clientMetadata = clientMetadata,
      quota = quota)

    //讲读取结果进行封装
    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
        result.lastStableOffset, result.info.abortedTransactions, result.preferredReadReplica,
        fetchMetadata.isFromFollower && replicaManager.isAddingReplica(tp, fetchMetadata.replicaId))
    }

    //调用回调函数
    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

