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
package org.apache.rya.indexing.pcj.fluo.app.util;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

public class FilterSerializer {

    private static final SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
    private static final SPARQLParser parser = new SPARQLParser();
    
    /**
     * Converts a {@link Filter} to a SPARQL query containing only the SPARQL representation
     * of the Filter along with a Select clause that return all variables.  The argument of the
     * Filter is replaced by a {@link SingletonSet} so that the body of the SPARQL query consists of only a
     * single Filter clause.  
     * @param filter - Filter to be serialized
     * @return - SPARQL String containing a single Filter clause that represents the serialized Filter
     * @throws FilterParseException
     */
    public static String serialize(Filter filter) throws FilterParseException {
        Filter clone = filter.clone();
        clone.setArg(new SingletonSet());
        try {
            return renderer.render(new ParsedTupleQuery(clone));
        } catch (Exception e) {
            throw new FilterParseException("Unable to parse Filter.", e);
        }
    }
    
    /**
     * Converts a SPARQL query consisting of a single Filter clause back to a Filter.
     * @param sparql - SPARQL query representing a Filter
     * @return - parsed Filter included in the SPARQL query
     * @throws FilterParseException
     */
    public static Filter deserialize(String sparql) throws FilterParseException {
        
        try {
            ParsedQuery pq = parser.parseQuery(sparql, null);
            FilterVisitor visitor = new FilterVisitor();
            pq.getTupleExpr().visit(visitor);
            Set<Filter> filters = visitor.getFilters();
            
            if(filters.size() != 1) {
                throw new FilterParseException("Filter String must contain only one Filter.");
            }
            
            return filters.iterator().next();
            
        } catch (Exception e) {
            throw new FilterParseException("Unable to parse Filter.", e);
        }
    }
    
    public static class FilterVisitor extends QueryModelVisitorBase<RuntimeException> {

        private Set<Filter> filters;
        
        public FilterVisitor() {
            filters = new HashSet<>();
        }

        public Set<Filter> getFilters() {
            return filters;
        }

        public void meet(Filter node) {
            filters.add(node);
        }
    }
    
    public static class FilterParseException extends Exception {

        private static final long serialVersionUID = 1L;
        
        /**
         * Constructs an instance of {@link FilterParseException}.
         *
         * @param message - Explains why this exception is being thrown.
         */
        public FilterParseException(final String message) {
            super(message);
        }

        /**
         * Constructs an instance of {@link FilterParseException}.
         *
         * @param message - Explains why this exception is being thrown.
         * @param cause - The exception that caused this one to be thrown.
         */
        public FilterParseException(final String message, final Throwable t) {
            super(message, t);
        }
    }
    
}
