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

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class WriteMemoryController extends MemoryController<TsFileProcessor> {
  private static final Logger logger = LoggerFactory.getLogger(WriteMemoryController.class);
  private static volatile WriteMemoryController INSTANCE;
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long memorySizeForWrite = config.getAllocateMemoryForWrite();
  private static final double FLUSH_THRESHOLD = memorySizeForWrite * config.getFlushProportion();
  private static final double REJECT_THRESHOLD = memorySizeForWrite * config.getRejectProportion();
  private volatile boolean rejected = false;
  private volatile long flushingMemory = 0;
  private Map<StorageGroupInfo, Long> reportedStorageGroupMemCostMap = new ConcurrentHashMap<>();
  private ExecutorService flushTaskSubmitThreadPool =
      IoTDBThreadPoolFactory.newSingleThreadExecutor("FlushTask-Submit-Pool");

  public WriteMemoryController(long limitSize) {
    super(limitSize);
    this.triggerThreshold = (long) FLUSH_THRESHOLD;
    this.trigger = this::chooseMemtableToFlush;
  }

  public boolean tryAllocateMemory(long size, StorageGroupInfo info, TsFileProcessor processor) {
    boolean success = super.tryAllocateMemory(size, processor);
    if (memoryUsage.get() > REJECT_THRESHOLD) {
      logger.info(
          "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), totalSgMemCost ({}).",
          info.getDataRegion().getLogicalStorageGroupName(),
          size,
          memoryUsage.get());
      rejected = true;
    }
    reportedStorageGroupMemCostMap.put(info, info.getMemCost());
    return success;
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
    if (reportedStorageGroupMemCostMap.size() == 0) {
      return;
    }
    PriorityQueue<TsFileProcessor> allTsFileProcessors =
        new PriorityQueue<>(
            (o1, o2) -> Long.compare(o2.getWorkMemTableRamCost(), o1.getWorkMemTableRamCost()));
    for (StorageGroupInfo storageGroupInfo : reportedStorageGroupMemCostMap.keySet()) {
      allTsFileProcessors.addAll(storageGroupInfo.getAllReportedTsp());
    }
    boolean isCurrentTsFileProcessorSelected = false;
    long memCost = 0;
    long activeMemSize = memoryUsage.get();
    while (activeMemSize - memCost > FLUSH_THRESHOLD) {
      if (allTsFileProcessors.isEmpty()
          || allTsFileProcessors.peek().getWorkMemTableRamCost() == 0) {
        return;
      }
      TsFileProcessor selectedTsFileProcessor = allTsFileProcessors.peek();
      if (selectedTsFileProcessor == null) {
        break;
      }
      memCost += selectedTsFileProcessor.getWorkMemTableRamCost();
      selectedTsFileProcessor.setWorkMemTableShouldFlush();
      flushTaskSubmitThreadPool.submit(selectedTsFileProcessor::submitAFlushTask);
      if (selectedTsFileProcessor == currentTsFileProcessor) {
        isCurrentTsFileProcessorSelected = true;
      }
      allTsFileProcessors.poll();
    }
  }
}
