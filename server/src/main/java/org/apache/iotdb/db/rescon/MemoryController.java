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
package org.apache.iotdb.db.rescon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryController {
  private static Logger log = LoggerFactory.getLogger(MemoryController.class);
  private AtomicLong memoryUsage = new AtomicLong(0);
  private AtomicBoolean triggerRunning = new AtomicBoolean();
  private long triggerThreshold = -1;
  private long limitSize = -1;
  private ReentrantLock lock = new ReentrantLock(false);
  private Condition condition = lock.newCondition();
  private Runnable trigger = null;

  public MemoryController(long limitSize) {
    this.limitSize = limitSize;
  }

  public MemoryController(long limitSize, long triggerThreshold, Runnable trigger) {
    this.limitSize = limitSize;
    this.triggerThreshold = triggerThreshold;
    this.trigger = trigger;
  }

  public boolean tryAllocateMemory(long size) {
    while (true) {
      long current = memoryUsage.get();
      long newUsage = current + size;

      // We allow one request to go over the limit, to make the notification
      // path simpler and more efficient
      if (current > limitSize && limitSize > 0) {
        return false;
      }

      if (memoryUsage.compareAndSet(current, newUsage)) {
        checkTrigger(current, newUsage);
        return true;
      }
    }
  }

  public void allocateMemoryMayBlock(long size) throws InterruptedException {
    if (!tryAllocateMemory(size)) {
      lock.lock();
      try {
        while (!tryAllocateMemory(size)) {
          condition.await();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  public boolean allocateMemoryMayBlock(long size, long maxBlockTime) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    if (!tryAllocateMemory(size)) {
      lock.lock();
      try {
        while (tryAllocateMemory(size)) {
          if (System.currentTimeMillis() - startTime >= maxBlockTime) {
            return false;
          }
          condition.await();
        }
        return true;
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  public void releaseMemory(long size) {
    long newUsage = memoryUsage.addAndGet(-size);
    if (newUsage + size > limitSize && newUsage <= limitSize) {
      lock.lock();
      try {
        condition.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  private void checkTrigger(long prevUsage, long newUsage) {
    if (newUsage >= triggerThreshold && prevUsage < triggerThreshold && trigger != null) {
      if (triggerRunning.compareAndSet(false, true)) {
        try {
          trigger.run();
        } finally {
          triggerRunning.set(false);
        }
      }
    }
  }

  public long getCurrentMemoryUsage() {
    return memoryUsage.get();
  }

  public double getCurrentUsagePercentage() {
    return ((double) memoryUsage.get()) / ((double) limitSize);
  }

  public boolean isMemoryLimited() {
    return this.limitSize > 0;
  }
}
