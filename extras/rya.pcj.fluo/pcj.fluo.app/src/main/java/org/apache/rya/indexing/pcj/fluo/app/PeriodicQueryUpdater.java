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
package org.apache.rya.indexing.pcj.fluo.app;

import java.util.HashSet;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This class adds the appropriate BinId Binding to each BindingSet that it processes.  The BinIds
 * are used to determine which period a BindingSet (with a temporal Binding) falls into so that
 * a user can receive periodic updates for a registered query.
 *
 */
public class PeriodicQueryUpdater {

    private static final Logger log = LoggerFactory.getLogger(PeriodicQueryUpdater.class);
    private static final ValueFactory vf = new ValueFactoryImpl();
    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();


    public void updatePeriodicBinResults(final TransactionBase tx, final VisibilityBindingSet bs, final PeriodicQueryMetadata metadata) throws Exception {
        final Set<Long> binIds = getBinEndTimes(metadata, bs);
        for(final Long id: binIds) {
            //create binding set value bytes
            final QueryBindingSet binnedBs = new QueryBindingSet(bs);
            binnedBs.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(id));
            final VisibilityBindingSet visibilityBindingSet = new VisibilityBindingSet(binnedBs, bs.getVisibility());
            final Bytes periodicBsBytes = BS_SERDE.serialize(visibilityBindingSet);

            //create row
            final Bytes resultRow = RowKeyUtil.makeRowKey(metadata.getNodeId(), metadata.getVariableOrder(), binnedBs);
            final Column col = FluoQueryColumns.PERIODIC_QUERY_BINDING_SET;
            tx.set(resultRow, col, periodicBsBytes);
        }
    }

    /**
     * This method returns the end times of all period windows containing the time contained in
     * the BindingSet.
     *
     * @param metadata
     * @return Set of period bin end times
     */
    private Set<Long> getBinEndTimes(final PeriodicQueryMetadata metadata, final VisibilityBindingSet bs) {
        final Set<Long> binIds = new HashSet<>();
        try {
            final String timeVar = metadata.getTemporalVariable();
            final Value value = bs.getBinding(timeVar).getValue();
            final Literal temporalLiteral = (Literal) value;
            final long eventDateTime = temporalLiteral.calendarValue().toGregorianCalendar().getTimeInMillis();
            return getEndTimes(eventDateTime, metadata.getWindowSize(), metadata.getPeriod());
        } catch (final Exception e) {
            log.trace("Unable to extract the entity time from BindingSet: " + bs);
        }
        return binIds;
    }

    private long getRightBinEndPoint(final long eventDateTime, final long periodDuration) {
        return (eventDateTime / periodDuration + 1) * periodDuration;
    }

    private long getLeftBinEndPoint(final long eventTime, final long periodDuration) {
        return (eventTime / periodDuration) * periodDuration;
    }

    /**
     * Using the smallest period end time, this method also creates all other period end times
     * that occur within one windowSize of the eventDateTime.
     * @param eventDateTime
     * @param startTime
     * @param windowDuration
     * @param periodDuration
     * @return Set of period bin end times
     */
    private Set<Long> getEndTimes(final long eventDateTime, final long windowDuration, final long periodDuration) {
        final Set<Long> binIds = new HashSet<>();
        final long rightEventBin = getRightBinEndPoint(eventDateTime, periodDuration);
        //get the bin left of the current moment for comparison
        final long currentBin = getLeftBinEndPoint(System.currentTimeMillis(), periodDuration);

        if(currentBin >= rightEventBin) {
            final long numBins = (windowDuration -(currentBin - rightEventBin))/periodDuration;
            for(int i = 0; i < numBins; i++) {
                binIds.add(currentBin + i*periodDuration);
            }
        } else {
            //this corresponds to a future event that is inserted into the system
            final long numBins = windowDuration/periodDuration;
            for(int i = 0; i < numBins; i++) {
                binIds.add(rightEventBin + i*periodDuration);
            }
        }

        return binIds;
    }


}
