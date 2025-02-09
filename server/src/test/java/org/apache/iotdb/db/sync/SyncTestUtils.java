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
package org.apache.iotdb.db.sync;

import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;

import org.junit.Assert;

public class SyncTestUtils {
  public static void checkPipeInfo(
      PipeInfo pipeInfo,
      String pipeName,
      String pipeSinkName,
      Pipe.PipeStatus status,
      long createTime,
      PipeMessage.PipeMessageType messageType) {
    Assert.assertEquals(pipeName, pipeInfo.getPipeName());
    Assert.assertEquals(pipeSinkName, pipeInfo.getPipeSinkName());
    Assert.assertEquals(status, pipeInfo.getStatus());
    Assert.assertEquals(createTime, pipeInfo.getCreateTime());
    Assert.assertEquals(messageType, pipeInfo.getMessageType());
  }
}
