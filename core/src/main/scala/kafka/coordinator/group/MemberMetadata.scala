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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe

/**
 * 组成员概要信息,提取了最核心的元数据信息：
 * bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group mygroup --verbose --state
 * 由此对象提供
 *
 * @param memberId 组成员ID， 由 Kafka 自动生成
 * @param groupInstanceId Consumer 端参数 group.instance.id 值
 * @param clientId  client.id 参数值
 * @param clientHost Consumer 端程序主机名
 * @param metadata 消费组成员使用的分配策略 partition.assignment.strategy 参数
 * @param assignment 成员订阅分区
 */
case class MemberSummary(memberId: String,
                         groupInstanceId: Option[String],
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

private object MemberMetadata {
  // 提取分区分配策略集合
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 * Member metadata contains the following metadata:
 *
 * 消费组成员的元数据
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String, // 成员id， 由kafka 自动生成
                                    val groupId: String,
                                    val groupInstanceId: Option[String],
                                    val clientId: String,
                                    val clientHost: String,
                                    val rebalanceTimeoutMs: Int,  // Rebalane操作超时时间
                                    val sessionTimeoutMs: Int, // 会话超时时间
                                    val protocolType: String, // 对消费者组而言，是"consumer"
                                    // 成员配置的多套分区分配策略
                                    var supportedProtocols: List[(String, Array[Byte])]) {
  // 分区分配方案
  var assignment: Array[Byte] = Array.empty[Byte]
  // 表示组成员是否正在等待加入组。
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  // 表示组成员是否正在等待 GroupCoordinator 发送分配方案
  var awaitingSyncCallback: SyncGroupResult => Unit = null
  var latestHeartbeat: Long = -1
  // 表示组成员是否发起“退出组”的操作。
  var isLeaving: Boolean = false
  // 表示是否是消费者组下的新成员。
  var isNew: Boolean = false
  // 是否是静态成员 group.instance.id
  val isStaticMember: Boolean = groupInstanceId.isDefined

  def isAwaitingJoin = awaitingJoinCallback != null
  def isAwaitingSync = awaitingSyncCallback != null

  /**
   * Get metadata corresponding to the provided protocol.
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  def shouldKeepAlive(deadlineMs: Long): Boolean = {
    if (isNew) {
      // New members are expired after the static join timeout
      latestHeartbeat + GroupCoordinator.NewMemberJoinTimeoutMs > deadlineMs
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Don't remove members as long as they have a request in purgatory
      true
    } else {
      // Otherwise check for session expiration
      latestHeartbeat + sessionTimeoutMs > deadlineMs
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }
}
