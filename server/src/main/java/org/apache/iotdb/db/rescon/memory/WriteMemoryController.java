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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class WriteMemoryController extends MemoryController<TsFileProcessor> {
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
      rejected = true;
    }
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

  protected void chooseMemtableToFlush(TsFileProcessor processor) {}
}
