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

public class WriteMemoryController extends MemoryController<WriteMemoryController.TriggerParam> {
  private static final Logger logger = LoggerFactory.getLogger(WriteMemoryController.class);
  private static volatile WriteMemoryController INSTANCE;
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static long memorySizeForWrite = config.getAllocateMemoryForWrite();
  private static double FLUSH_THRESHOLD = memorySizeForWrite * config.getFlushProportion();
  private static double REJECT_THRESHOLD = memorySizeForWrite * config.getRejectProportion();
  private static double END_FLUSH_THRESHOLD = 0.5 * FLUSH_THRESHOLD;
  private volatile boolean rejected = false;
  private AtomicLong flushingMemory = new AtomicLong(0);
  private Set<StorageGroupInfo> infoSet = new CopyOnWriteArraySet<>();
  private ExecutorService flushTaskSubmitThreadPool =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "FlushTask-Submit-Pool");
  public static final long FRAME_SIZE = config.getWriteMemoryFrameSize() * 1024L;

  public WriteMemoryController(long limitSize) {
    super(limitSize);
    this.triggerThreshold = (long) FLUSH_THRESHOLD;
    this.trigger = this::chooseMemtableToFlush;
  }

  public boolean tryAllocateMemory(long size, StorageGroupInfo info, TsFileProcessor processor) {
    TriggerParam param = new TriggerParam(processor);
    boolean success = super.tryAllocateMemory(size, param);
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

  public boolean allocateFrame(StorageGroupInfo info, TsFileProcessor processor, long memTableId) {
    boolean success = this.tryAllocateMemory(FRAME_SIZE, info, processor);
    if (success) {
      logger.error(
          "Allocate memory frame for {}-{}#{}, current usage is {} MB, remaining is {} MB, flushing memory is {} MB",
          info.getDataRegion().getLogicalStorageGroupName(),
          info.getDataRegion().getDataRegionId(),
          memTableId,
          ((double) memoryUsage.get()) / 1024.0d / 1024.0d,
          ((double) (memorySizeForWrite - memoryUsage.get())) / 1024.0d / 1024.0d,
          ((double) flushingMemory.get()) / 1024.0d / 1024.0d);
    }
    return success;
  }

  public void releaseFlushingMemory(StorageGroupInfo info, long size) {
    this.flushingMemory.addAndGet(-size);
    this.releaseMemory(size);
  }

  public void releaseMemory(long size) {
    super.releaseMemory(size);
    if (rejected && memoryUsage.get() < REJECT_THRESHOLD) {
      rejected = false;
    }
    logger.error(
        "Release {} MB, current usage is {} MB",
        ((double) size) / 1024.0d / 1024.0d,
        ((double) memoryUsage.get()) / 1024.0d / 1024.0d);
  }

  public boolean checkRejected() {
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

  public void applyExternalMemoryForFlushing(long size) {
    memorySizeForWrite -= size;
    FLUSH_THRESHOLD = memorySizeForWrite * config.getFlushProportion();
    REJECT_THRESHOLD = memorySizeForWrite * config.getRejectProportion();
    END_FLUSH_THRESHOLD = 0.5 * FLUSH_THRESHOLD;
  }

  public void releaseExternalMemoryForFlushing(long size) {
    memorySizeForWrite += size;
    FLUSH_THRESHOLD = memorySizeForWrite * config.getFlushProportion();
    REJECT_THRESHOLD = memorySizeForWrite * config.getRejectProportion();
    END_FLUSH_THRESHOLD = 0.5 * FLUSH_THRESHOLD;
  }

  protected void chooseMemtableToFlush(TriggerParam triggerParam) {
    // If invoke flush by replaying logs, do not flush now!
    if (infoSet.size() == 0) {
      return;
    }
    long memCost = 0;
    PriorityQueue<TsFileProcessor> allTsFileProcessors =
        new PriorityQueue<>(
            (o1, o2) ->
                Long.compare(o2.getWorkMemTableAllocateSize(), o1.getWorkMemTableAllocateSize()));
    for (StorageGroupInfo storageGroupInfo : infoSet) {
      allTsFileProcessors.addAll(storageGroupInfo.getAllReportedTsp());
    }
    long selectedCount = 0;
    long activeMemory = memoryUsage.get() - flushingMemory.get();
    while (activeMemory - memCost > FLUSH_THRESHOLD) {
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
        allTsFileProcessors.poll();
        continue;
      }
      if (selectedTsFileProcessor == triggerParam.processor) {
        triggerParam.currentProcessorSelected = true;
      }
      long memUsageForThisMemTable = selectedTsFileProcessor.getWorkMemTableAllocateSize();
      memCost += memUsageForThisMemTable;
      selectedTsFileProcessor.setWorkMemTableShouldFlush();
      flushingMemory.addAndGet(memUsageForThisMemTable);
      flushTaskSubmitThreadPool.submit(selectedTsFileProcessor::submitAFlushTask);
      selectedCount++;
      allTsFileProcessors.poll();
    }
    logger.info(
        "Select {} memtable to flush, flushing memory is {} MB, remaining memory is {} MB",
        selectedCount,
        ((double) flushingMemory.get()) / 1024.0d / 1024.0d,
        ((double) (memoryUsage.get() - flushingMemory.get())) / 1024.0d / 1024.0d);
  }

  static class TriggerParam {
    TsFileProcessor processor;
    public volatile boolean triggerRan = false;
    public volatile boolean currentProcessorSelected = false;

    public TriggerParam(TsFileProcessor processor) {
      this.processor = processor;
    }
  }
}
