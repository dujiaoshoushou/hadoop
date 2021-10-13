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
package org.apache.hadoop.hdfs.server.protocol;

/**
 * Utilization report for a Datanode storage
 */
public class StorageReport {
  private final DatanodeStorage storage; // 一个DatanodeStorage对象
  private final boolean failed; // 设备是否失败
  private final long capacity; // 设备（文件卷）容量
  private final long dfsUsed; // 已由HDFS用去的容量
  private final long nonDfsUsed; // 剩余容量
  private final long remaining; // 剩余容量
  private final long blockPoolUsed; // 由这个BlockBool用去的容量

  public static final StorageReport[] EMPTY_ARRAY = {};

  public StorageReport(DatanodeStorage storage, boolean failed, long capacity,
      long dfsUsed, long remaining, long bpUsed, long nonDfsUsed) {
    this.storage = storage;
    this.failed = failed;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.nonDfsUsed = nonDfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = bpUsed;
  }

  public DatanodeStorage getStorage() {
    return storage;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public long getNonDfsUsed() {
    return nonDfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }
}
