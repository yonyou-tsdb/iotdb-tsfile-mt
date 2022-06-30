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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryController {
  private static Logger log = LoggerFactory.getLogger(MemoryController.class);
  private static final MemoryController INSTANCE = new MemoryController();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private long memorySizeForWrite = config.getAllocateMemoryForWrite();
  private long FLUSH_THERSHOLD = (long) (memorySizeForWrite * config.getFlushProportion());
  private double REJECT_THERSHOLD = (long) (memorySizeForWrite * config.getRejectProportion());
  private AtomicLong memoryUsage = new AtomicLong(0);

  private MemoryController() {}

  private MemoryController getInstance() {
    return INSTANCE;
  }
}
