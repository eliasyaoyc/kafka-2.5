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

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {

  //用来表示 TimerTask 是否完成
  private val completed = new AtomicBoolean(false)
  //用来表示 TimerTask 正在执行 maybeTryComplete 方法，尝试完成
  private val tryCompletePending = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   *
   * 强制完成延迟操作，不管它是否满足完成条件。每当操作满足完成条件或已经过期了，就需要调用该方法完成该操作。
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   *
   * 检查延迟操作是否已经完成。源码使用这个方法来决定后续如何处理该操作。比如如果操作已经完成了，那么通常需要取消该操作
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   *
   * 强制完成之后执行的过期逻辑回调方法。只有真正完成操作的那个线程才有资格调用这个方法。
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   *
   * 完成延迟操作所需的处理逻辑。这个方法只会在 forceComplete 方法中被调用。
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   *
   * 尝试完成延迟操作的顶层方法，内部会调用 forceComplete 方法。
   */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete() that attempts completion only if the lock can be acquired
   * without blocking.
   *
   * If threadA acquires the lock and performs the check for completion before completion criteria is met
   * and threadB satisfies the completion criteria, but fails to acquire the lock because threadA has not
   * yet released the lock, we need to ensure that completion is attempted again without blocking threadA
   * or threadB. `tryCompletePending` is set by threadB when it fails to acquire the lock and at least one
   * of threadA or threadB will attempt completion of the operation if this flag is set. This ensures that
   * every invocation of `maybeTryComplete` is followed by at least one invocation of `tryComplete` until
   * the operation is actually completed.
   *
   * 线程安全版本的 tryComplete 方法。这个方法其实是社区后来才加入的，不过已经慢慢地取代了 tryComplete，现在外部代码调用的都是这个方法了。
   */
  private[server] def maybeTryComplete(): Boolean = {
    var retry = false // 是否需要重试
    var done = false  // 延迟操作是否已完成
    do {
      if (lock.tryLock()) { // 尝试获取锁对象
        try {
          tryCompletePending.set(false)
          done = tryComplete()
        } finally {
          lock.unlock()
        }
        // While we were holding the lock, another thread may have invoked `maybeTryComplete` and set
        // `tryCompletePending`. In this case we should retry.
        // 运行到这里的线程持有锁，其他线程只能运行 else 分支的代码
        // 如果其他线程将 maybeTryComplete 设置为 true，那么retry=true 这就相当与其他线程给了本地线程重试的机会
        retry = tryCompletePending.get()
      } else {
        // Another thread is holding the lock. If `tryCompletePending` is already set and this thread failed to
        // acquire the lock, then the thread that is holding the lock is guaranteed to see the flag and retry.
        // Otherwise, we should set the flag and retry on this thread since the thread holding the lock may have
        // released the lock and returned by the time the flag is set.
        // 运行到这里的线程没有拿到锁
        // 设置tryCompletePending=true给持有锁的线程一个重试的机会
        retry = !tryCompletePending.getAndSet(true)
      }
    } while (!isCompleted && retry)
    done
  }

  /*
   * run() method defines a task that is executed on timeout
   * 调用延迟操作超时后的过期逻辑，也就是组合调用 forceComplete + onExpiration。
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,//systemTimer对象
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,//systemTimer对象
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {
  /* a list of operation watching keys */
  private class WatcherList {
    // 定义一组按照Key分组的Watchers对象
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  //记录了该 DelayedOperationPurgatory 中的 DelayedOperation个数
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  //一个ShutdownableThread 线程对象，主要有两个功能，一个是推进时间轮表针，二是定期清理watchersByKey 中已完成的 DelayedOperation。
  //清理条件有 purgeInterval 字段指定。
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)
  newGauge("PurgatorySize", () => watched, metricsTags)
  newGauge("NumDelayedOperations", () => numDelayed, metricsTags)

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   *
   * 检查DelayedOperation 是否已经完成，若未完成则添加到 watchersByKey 以及 SystemTimer 中。
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    // At this point the only thread that can attempt this operation is this current thread
    // Hence it is safe to tryComplete() without a lock
    //尝试完成延迟操作
    var isCompletedByMe = operation.tryComplete()
    // 如果该延迟请求是由本线程完成的，直接返回true即可
    if (isCompletedByMe)
      return true

    var watchCreated = false
    //传入的key 可能是多个，循环把未完成的添加到 watchersByKey
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      // 再次查看请求的完成状态，如果已经完成，就说明是被其他线程完成的，返回false
      if (operation.isCompleted)
        return false
      // 否则，将该operation加入到Key所在的WatcherList
      watchForOperation(key, operation)

      // 设置watchCreated标记，表明该任务已经被加入到WatcherList
      if (!watchCreated) {
        watchCreated = true
        // 更新Purgatory中总请求数
        estimatedTotalOperations.incrementAndGet()
      }
    }

    //第二次尝试完成
    isCompletedByMe = operation.maybeTryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    //没完成加入 SystemTimer 时间轮
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      //完成 删除。
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val wl = watcherList(key)
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    val numCompleted = if (watchers == null)
      0
    else
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T): Unit = {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    // 遍历 operations 集合 对里面未完成的 operation 执行 tryComplete 方法尝试完成，将已经完成的移除。
    // 如果 operations 队列为空，则将 Watchers 从 DelayedOperationPurgatory.watchersByKey 中删除
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {//遍历
        val curr = iter.next()
        if (curr.isCompleted) {//完成则移除
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.maybeTryComplete()) {//尝试完成
          //成功移除
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    //负责清理 operations 队列， 将已经完成的 DelayOperation 从 operations 队列中移除，
    //如果 operations 队列为空，则将Watchers从watcherForKey集合中删除
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    //尝试推进时间轮的表针
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.

    // DelayedOperation 到期后 被Systemer.taskExecutor完成后，并不会通知 DelayedOperationPurgatory 删除DelayedOperation
    // DelayedOperationPurgatory 与 SystemTimer 中的 DelayedOperation 数量相差到一个阈值时，会调用 purgeCompleted 方法执行清理工作
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      //调用 watchers.purgeCompleted 方法清理已完成的 DelayedOperation
      val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      //最长堵塞时间200ms
      advanceClock(200L)
    }
  }
}
