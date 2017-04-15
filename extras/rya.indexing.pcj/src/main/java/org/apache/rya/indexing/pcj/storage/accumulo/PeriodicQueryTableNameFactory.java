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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

public class PeriodicQueryTableNameFactory {

    public static final String PeriodicTableSuffix = "PERIODIC_QUERY_";
    
    public String makeTableName(final String ryaInstance, final String queryId) {
        requireNonNull(ryaInstance);
        requireNonNull(queryId);
        return ryaInstance + PeriodicTableSuffix + queryId.toString().replaceAll("-", "");
    }

    public String getPeriodicQueryId(final String periodTableName) {
        requireNonNull(periodTableName);
        return periodTableName.split(PeriodicTableSuffix)[1];
    }
    
}
