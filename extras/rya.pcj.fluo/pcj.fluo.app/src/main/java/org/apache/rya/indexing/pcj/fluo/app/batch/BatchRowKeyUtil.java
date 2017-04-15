package org.apache.rya.indexing.pcj.fluo.app.batch;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import java.util.UUID;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;

import com.google.common.base.Preconditions;

public class BatchRowKeyUtil {

    public static Bytes getRow(String nodeId) {
        String row = new StringBuilder().append(nodeId).append(IncrementalUpdateConstants.NODEID_BS_DELIM)
                .append(UUID.randomUUID().toString().replace("-", "")).toString();
        return Bytes.of(row);
    }
    
    public static Bytes getRow(String nodeId, String batchId) {
        String row = new StringBuilder().append(nodeId).append(IncrementalUpdateConstants.NODEID_BS_DELIM)
                .append(batchId).toString();
        return Bytes.of(row);
    }
    
    public static String getNodeId(Bytes row) {
        String[] stringArray = row.toString().split(IncrementalUpdateConstants.NODEID_BS_DELIM);;
        Preconditions.checkArgument(stringArray.length == 2);
        return stringArray[0];
    }
    
}
