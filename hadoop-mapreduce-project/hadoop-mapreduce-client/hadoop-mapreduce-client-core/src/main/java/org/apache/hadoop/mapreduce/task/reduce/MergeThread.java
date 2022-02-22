/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class MergeThread<T,K,V> extends Thread {
  
  private static final Logger LOG = LoggerFactory.getLogger(MergeThread.class);

  private AtomicInteger numPending = new AtomicInteger(0);
  private LinkedList<List<T>> pendingToBeMerged; // 待合并队列
  protected final MergeManagerImpl<K,V> manager; // 所属的MergeManagerImpl
  private final ExceptionReporter reporter;
  private boolean closed = false;
  private final int mergeFactor; // 表示这个merger最多可做几路的合并
  
  public MergeThread(MergeManagerImpl<K,V> manager, int mergeFactor,
                     ExceptionReporter reporter) {
    this.pendingToBeMerged = new LinkedList<List<T>>();
    this.manager = manager;
    this.mergeFactor = mergeFactor;
    this.reporter = reporter;
  }
  
  public synchronized void close() throws InterruptedException {
    closed = true;
    waitForMerge(); // 等待，直至该线程的队列中不再有需要合并的数据源
    interrupt(); // 中止线程的运行
  }

  public void startMerge(Set<T> inputs) { // 将数据源集合inputs挂入pendingToBeMerged队列
    if (!closed) {
      numPending.incrementAndGet();
      List<T> toMergeInputs = new ArrayList<T>();
      Iterator<T> iter=inputs.iterator(); // inputs是个序列，这个序列的每个元素
      for (int ctr = 0; iter.hasNext() && ctr < mergeFactor; ++ctr) {
        toMergeInputs.add(iter.next()); // 从inputs搜集需要合并的输入，不超过mergeFactor个，也就是说，这个线程只能进行最多mergeFactor路的合并
        iter.remove();
      }
      LOG.info(getName() + ": Starting merge with " + toMergeInputs.size() + 
               " segments, while ignoring " + inputs.size() + " segments");
      synchronized(pendingToBeMerged) {
        pendingToBeMerged.addLast(toMergeInputs); // 将一组需要合并的输入挂入队列
        pendingToBeMerged.notifyAll(); // 然后发出通知，唤醒相关的线程
      }
    }
  }

  public synchronized void waitForMerge() throws InterruptedException {
    while (numPending.get() > 0) {
      wait();
    }
  }

  public void run() {
    while (true) {
      List<T> inputs = null;
      try {
        // Wait for notification to start the merge...
        synchronized (pendingToBeMerged) {
          while(pendingToBeMerged.size() <= 0) {
            pendingToBeMerged.wait();
          }
          // Pickup the inputs to merge.
          inputs = pendingToBeMerged.removeFirst();
        }

        // Merge
        merge(inputs);
      } catch (InterruptedException ie) {
        numPending.set(0);
        return;
      } catch(Throwable t) {
        numPending.set(0);
        reporter.reportException(t);
        return;
      } finally {
        synchronized (this) {
          numPending.decrementAndGet();
          notifyAll();
        }
      }
    }
  }

  public abstract void merge(List<T> inputs) throws IOException;
}
