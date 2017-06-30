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
package org.apache.rya.periodic.notification.api;

import java.util.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryNode;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.periodic.notification.application.PeriodicNotificationApplication;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.openrdf.query.MalformedQueryException;

/**
 * Object that creates a PeriodicQuery. 
 *
 */
public class CreatePeriodicQuery {

    private FluoClient fluoClient;
    private PeriodicQueryResultStorage periodicStorage;
    
    public CreatePeriodicQuery(FluoClient fluoClient, PeriodicQueryResultStorage periodicStorage) {
        this.fluoClient = fluoClient;
        this.periodicStorage = periodicStorage;
    }
    
    /**
     * Creates a PeriodicQuery by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @return PeriodicNotification that can be used to register register this query with the {@link PeriodicNotificationApplication}.
     */
    public PeriodicNotification createPeriodicQuery(String sparql) {
        try {
            Optional<PeriodicQueryNode> optNode = PeriodicQueryUtil.getPeriodicNode(sparql);
            if(optNode.isPresent()) {
                PeriodicQueryNode periodicNode = optNode.get();
                CreatePcj createPcj = new CreatePcj();
                String queryId = createPcj.createPcj(sparql, fluoClient);
                periodicStorage.createPeriodicQuery(queryId, sparql);
                PeriodicNotification notification = PeriodicNotification.builder().id(queryId).period(periodicNode.getPeriod())
                        .timeUnit(periodicNode.getUnit()).build();
                return notification;
            } else {
                throw new RuntimeException("Invalid PeriodicQuery.  Query must possess a PeriodicQuery Filter.");
            }
        } catch (MalformedQueryException | PeriodicQueryStorageException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Creates a PeriodicQuery by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.  In addition, this
     * method registers the PeriodicQuery with the PeriodicNotificationApplication to poll
     * the PeriodicQueryResultStorage table at regular intervals and export results to Kafka.
     * The PeriodicNotificationApp queries the result table at a regular interval indicated by the Period of
     * the PeriodicQuery.
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @param PeriodicNotificationClient - registers the PeriodicQuery with the {@link PeriodicNotificationApplication}
     * @return id of the PeriodicQuery and PeriodicQueryResultStorage table (these are the same)
     */
    public String createQueryAndRegisterWithKafka(String sparql, PeriodicNotificationClient periodicClient) {
        PeriodicNotification notification = createPeriodicQuery(sparql);
        periodicClient.addNotification(notification);
        return notification.getId();
    }
    
}
