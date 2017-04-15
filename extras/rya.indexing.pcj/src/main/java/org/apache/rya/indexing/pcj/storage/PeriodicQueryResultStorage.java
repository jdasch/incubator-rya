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
package org.apache.rya.indexing.pcj.storage;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

import info.aduna.iteration.CloseableIteration;

public interface PeriodicQueryResultStorage {
    
    public static String PeriodicBinId = "periodicBinId";

    public String createPeriodicQuery(String sparql) throws PeriodicQueryStorageException;
    
    public String createPeriodicQuery(String queryId, String sparql) throws PeriodicQueryStorageException;
    
    public void createPeriodicQuery(String queryId, String sparql, VariableOrder varOrder) throws PeriodicQueryStorageException;
    
    public PeriodicQueryStorageMetadata getPeriodicQueryMetadata(String queryID) throws PeriodicQueryStorageException;;
    
    public void addPeriodicQueryResults(String queryId, Collection<VisibilityBindingSet> results) throws PeriodicQueryStorageException;;
    
    public void deletePeriodicQueryResults(String queryId, long binID) throws PeriodicQueryStorageException;;
    
    public void deletePeriodicQuery(String queryID) throws PeriodicQueryStorageException;;
    
    public CloseableIterator<BindingSet> listResults(String queryId, Optional<Long> binID) throws PeriodicQueryStorageException;;
    
    public List<String> listPeriodicTables();
    
}
