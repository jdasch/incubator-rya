/**
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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.text.DecimalFormat;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.LoadStatementsFile;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link LoadStatementsFile} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloExecuteSparqlQuery extends AccumuloCommand implements ExecuteSparqlQuery {
    private static final Logger log = Logger.getLogger(AccumuloExecuteSparqlQuery.class);

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloExecuteSparqlQuery}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloExecuteSparqlQuery(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }


    @Override
    public String executeSparqlQuery(final String ryaInstanceName, final String sparqlQuery)
            throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(sparqlQuery);

        // Ensure the Rya Instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }


        Sail sail = null;
        SailRepository sailRepo = null;
        SailRepositoryConnection sailRepoConn = null;

        try {
            // Get a Sail object that is connected to the Rya instance.
            final AccumuloRdfConfiguration ryaConf = getAccumuloConnectionDetails().buildAccumuloRdfConfiguration(ryaInstanceName);
            sail = RyaSailFactory.getInstance(ryaConf);

            // Load the file.
            sailRepo = new SailRepository(sail);
            sailRepoConn = sailRepo.getConnection();
            final long start = System.currentTimeMillis();
            final TupleQuery tupleQuery = sailRepoConn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
            final TupleQueryResult result = tupleQuery.evaluate();
            final StringBuilder sb = new StringBuilder();
            final String newline = "\n";
            sb.append("Query Result:").append(newline);
            sb.append("Binding Names: ").append(result.getBindingNames().toString()).append(newline);

            int count = 0;
            while(result.hasNext()) {
                sb.append(result.next().toString()).append(newline);
                count++;
            }
            final String seconds = new DecimalFormat("0.0##").format((System.currentTimeMillis() - start) / 1000.0);
            sb.append("Retrieved ").append(count).append(" results in ").append(seconds).append(" seconds.");

            return sb.toString();

        } catch (final SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException  e) {
            throw new RyaClientException("A problem connecting to the Rya instance named '" + ryaInstanceName + "' has caused the query to fail.", e);
        } catch (final MalformedQueryException e) {
            throw new RyaClientException("There was a problem parsing the supplied query.", e);
        } catch (final QueryEvaluationException e) {
            throw new RyaClientException("There was a problem evaluating the supplied query.", e);
        } catch (final RepositoryException e) {
            throw new RyaClientException("There was a problem executing the query against the Rya instance named " + ryaInstanceName + ".", e);
        } finally {
            // Shut it all down.
            if(sailRepoConn != null) {
                try {
                    sailRepoConn.close();
                } catch (final RepositoryException e) {
                    log.warn("Couldn't close the SailRepoConnection that is attached to the Rya instance.", e);
                }
            }
            if(sailRepo != null) {
                try {
                    sailRepo.shutDown();
                } catch (final RepositoryException e) {
                    log.warn("Couldn't shut down the SailRepository that is attached to the Rya instance.", e);
                }
            }
            if(sail != null) {
                try {
                    sail.shutDown();
                } catch (final SailException e) {
                    log.warn("Couldn't shut down the Sail that is attached to the Rya instance.", e);
                }
            }
        }
    }
}