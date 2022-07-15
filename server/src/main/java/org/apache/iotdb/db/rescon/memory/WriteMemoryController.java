/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.rescon.memory;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupInfo;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class WriteMemoryController extends MemoryController<TsFileProcessor> {
  private static final Logger logger = LoggerFactory.getLogger(WriteMemoryController.class);
  private static volatile WriteMemoryController INSTANCE;
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long memorySizeForWrite = config.getAllocateMemoryForWrite();
  private static final double FLUSH_THRESHOLD = memorySizeForWrite * config.getFlushProportion();
  private static final double END_FLUSH_THRESHOLD = 0.7 * FLUSH_THRESHOLD;
  private static final double REJECT_THRESHOLD = memorySizeForWrite * config.getRejectProportion();
  private volatile boolean rejected = false;
  private AtomicLong flushingMemory = new AtomicLong(0);
  private Set<StorageGroupInfo> infoSet = new CopyOnWriteArraySet<>();
  private ExecutorService flushTaskSubmitThreadPool =
      IoTDBThreadPoolFactory.newFixedThreadPool(2, "FlushTask-Submit-Pool");

  public WriteMemoryController(long limitSize) {
    super(limitSize);
    this.triggerThreshold = (long) FLUSH_THRESHOLD;
    this.trigger = this::chooseMemtableToFlush;
  }

  public boolean tryAllocateMemory(long size, StorageGroupInfo info, TsFileProcessor processor) {
    boolean success = super.tryAllocateMemory(size, processor);
    if (memoryUsage.get() > REJECT_THRESHOLD && !rejected) {
      logger.info(
          "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), totalSgMemCost ({}).",
          info.getDataRegion().getLogicalStorageGroupName(),
          size,
          memoryUsage.get());
      rejected = true;
    }
    if (!info.isRecorded()) {
      info.setRecorded(true);
      infoSet.add(info);
    }
    return success;
  }

  public void releaseFlushingMemory(long size, String storageGroup, long memTableId) {
    this.flushingMemory.addAndGet(-size);
    this.releaseMemory(size, storageGroup, memTableId);
  }

  public void releaseMemory(long size, String storageGroup, long memTableId) {
    super.releaseMemory(size);
    if (rejected && memoryUsage.get() < REJECT_THRESHOLD) {
      rejected = false;
    }
  }

  public boolean isRejected() {
    return rejected;
  }

  public static WriteMemoryController getInstance() {
    if (INSTANCE == null) {
      synchronized (WriteMemoryController.class) {
        if (INSTANCE == null) {
          INSTANCE = new WriteMemoryController(memorySizeForWrite);
        }
      }
    }
    return INSTANCE;
  }

  protected void chooseMemtableToFlush(TsFileProcessor currentTsFileProcessor) {
    // If invoke flush by replaying logs, do not flush now!
    if (infoSet.size() == 0) {
      return;
    }
    long memCost = 0;
    long activeMemSize = memoryUsage.get() - flushingMemory.get();
    if (activeMemSize - memCost < FLUSH_THRESHOLD) {
      return;
    }
    PriorityQueue<TsFileProcessor> allTsFileProcessors =
        new PriorityQueue<>(
            (o1, o2) -> Long.compare(o2.getWorkMemTableRamCost(), o1.getWorkMemTableRamCost()));
    for (StorageGroupInfo storageGroupInfo : infoSet) {
      allTsFileProcessors.addAll(storageGroupInfo.getAllReportedTsp());
    }
    long selectedCount = 0;
    while (activeMemSize - memCost > FLUSH_THRESHOLD) {
      if (allTsFileProcessors.isEmpty()
          || allTsFileProcessors.peek().getWorkMemTableRamCost() == 0) {
        return;
      }
      TsFileProcessor selectedTsFileProcessor = allTsFileProcessors.peek();
      if (selectedTsFileProcessor == null) {
        break;
      }
      if (selectedTsFileProcessor.getWorkMemTable() == null
          || selectedTsFileProcessor.getWorkMemTable().shouldFlush()) {
        continue;
      }
      memCost += selectedTsFileProcessor.getWorkMemTableRamCost();
      selectedTsFileProcessor.setWorkMemTableShouldFlush();
      flushingMemory.addAndGet(selectedTsFileProcessor.getWorkMemTableRamCost());
      flushTaskSubmitThreadPool.submit(selectedTsFileProcessor::submitAFlushTask);
      selectedCount++;
      allTsFileProcessors.poll();
    }
    logger.info(
        "Select {} memtable to flush, flushing memory is {}, remaining memory is {}",
        selectedCount,
        flushingMemory.get(),
        memoryUsage.get() - flushingMemory.get());
  }
}
