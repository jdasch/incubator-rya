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

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Preconditions;

public class PeriodicQueryStorageMetadata {

    private String sparql;
    private VariableOrder varOrder;

    public PeriodicQueryStorageMetadata(String sparql, VariableOrder varOrder) {
        Preconditions.checkNotNull(sparql);
        Preconditions.checkNotNull(varOrder);
        this.sparql = sparql;
        this.varOrder = varOrder;
    }
    
    public PeriodicQueryStorageMetadata(PcjMetadata metadata) {
        this(metadata.getSparql(), metadata.getVarOrders().iterator().next());
    }
    

    public String getSparql() {
        return sparql;
    }
    
    public VariableOrder getVariableOrder() {
        return varOrder;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(sparql, varOrder);
    }
   
    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof PeriodicQueryStorageMetadata) {
                PeriodicQueryStorageMetadata metadata = (PeriodicQueryStorageMetadata) o;
                return new EqualsBuilder().append(sparql, metadata.sparql).append(varOrder, metadata.varOrder).isEquals();
        }

        return false;
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("PeriodicQueryStorageMetadata {\n")
                .append("    SPARQL: " + sparql + "\n")
                .append("    Variable Order: " + varOrder + "\n")
                .append("}")
                .toString();
    }
    
    
}
