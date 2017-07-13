package org.apache.rya.fluo.verification;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.AccumuloIndexingConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoClientFactory;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableMap;


/**
 * This utility is for verifying and validating the functionality of a
 * deployment of fluo and the rya_pcj_updater on a remote cluster.
 */
public class RemoteFluoVerification implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(RemoteFluoVerification.class);

    private final SailRepository repo;
	private final SailRepositoryConnection conn;
	private final AccumuloIndexingConfiguration config;

	public RemoteFluoVerification(final AccumuloIndexingConfiguration config) throws SailException, InferenceEngineException, RyaDAOException, AccumuloSecurityException, AccumuloException, RepositoryException {
		this.config = config;
		repo = new SailRepository(RyaSailFactory.getInstance(config));
		conn = repo.getConnection();
	}

	@Override
	public void close() throws Exception {
		try {
			if (conn != null) {
				conn.close();
			}
		} finally {
			if(repo != null) {
				repo.shutDown();
			}
		}
	}

	public void load(final File nTriplesData) throws Exception {
		logger.info("Loading RDF file: {}", nTriplesData);

		final RDFFormat format = RDFFormat.forFileName(nTriplesData.getName());
		if(format == null) {
			throw new Exception("Unable to determine RDF Format for file: " + nTriplesData.toString());
		}

		logger.info("Loading data as format: {}", format);
		//conn.add(file, baseURI, dataFormat, contexts);
		conn.add(nTriplesData, null, format, new URIImpl("http://" + RemoteFluoVerification.class.getSimpleName()));
		logger.info("Loading complete.");
	}



	public void registerPcjQuery(final ImmutableMap<String,String> queries) {
		logger.info("Registering PCJ Queries: {}", queries.keySet());
		logger.info("Rya prefix: {}", config.getTablePrefix());
		try {
			final Connector accumuloConn = ConfigUtils.getConnector(config);
			try (final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, config.getTablePrefix());
					FluoClient fluoClient = FluoClientFactory.getFluoClient(config.getFluoAppUpdaterName(), Optional.empty(), config)) {
				for (final Map.Entry<String, String> q : queries.entrySet()) {

					logger.info("Creating PCJ Query for query: {}", q.getKey());
					final String pcjId = pcjStorage.createPcj(q.getValue());
			        // Tell the Fluo app to maintain the PCJ.
				    final String queryId = new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, config.getTablePrefix());

					logger.info("Created PCJ Query for query: {} with pcjID: {} and queryId: {}", q.getKey(), pcjId, queryId);

				}
			}
		} catch (final Exception e) {
			logger.warn("Error Occurred during query", e);
		}
	}

	public void query(final ImmutableMap<String,String> queries) {
		logger.info("Registering PCJ Queries: {}", queries.keySet());
		logger.info("Rya prefix: {}", config.getTablePrefix());

	}


	private static AccumuloIndexingConfiguration loadConf(final File propertyFile) throws IOException {
		try (FileInputStream fin = new FileInputStream(propertyFile)) {
			final Properties p = new Properties();
			p.load(fin);
			return AccumuloIndexingConfiguration.fromProperties(p);
		}
	}


	private static ImmutableMap<String, String> loadQueries(final List<File> queries) throws IOException {
		final ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
		for(final File file : queries) {
			mapBuilder.put(file.getName(), FileUtils.readFileToString(file, StandardCharsets.UTF_8));
		}
		return mapBuilder.build();
	}


	@Parameters(commandDescription = "Load Sparql Data")
	private static class CommandLoad {

		@Parameter(names = { "-i", "--input-rdf-file" }, description = "File containing RDF Data.  Supported formats include: .nt, .ttl, .n3, .trix, .trig, .owl, .rdf, .brf, .nq, .jsonld, .rj.", required = true, validateValueWith = FileExistsValidator.class)
		private File inputFile;

	}

	@Parameters(commandDescription = "Perform a Query")
	private static class CommandQuery {

		@Parameter(names = { "-q", "--query-sparql-files" }, description = "List of files containing sparql queries that should be executed.", required = true, variableArity = true)
		private List<File> queries;
	}


	private static class CommonOptions {
		@Parameter(names = { "-p", "--accumulo-properties" }, description = "The accumulo.properties file that should be read for processing.", required = true, validateValueWith = FileExistsValidator.class)
		private File accumuloProperties;
	}



//    final Connector accumuloConn = super.getAccumuloConnector();
//    final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
//    final String pcjId = pcjStorage.createPcj(sparql);
//
//    try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
//        // Tell the Fluo app to maintain the PCJ.
//        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());
//



	public static void main(String[] args) {

		args = new String[] {
				"-p", "resources/rya.properties",
				"load", "-i", "resources/LUBM/LUBM-Small.nt"
		};

		/*
		args = new String[] {
				"-p", "resources/rya.properties",
				"register-pcj-query",
				"-q", "resources/LUBM/queries/Query1.sparql",
				"resources/LUBM/queries/Query2.sparql",
				"resources/LUBM/queries/Query3.sparql",
				"resources/LUBM/queries/Query4.sparql"
		};
		*/

		final CommandLoad loadCommand = new CommandLoad();
		final CommandQuery queryCommand = new CommandQuery();
		final CommonOptions commonOptions = new CommonOptions();
		final JCommander jc = new JCommander();
		jc.setProgramName(RemoteFluoVerification.class.getSimpleName());
		jc.addObject(commonOptions);
		jc.addCommand("load", loadCommand);
		jc.addCommand("register-pcj-query", queryCommand);
		jc.addCommand("query", queryCommand);
		try {
			jc.parse(args);
		} catch (final ParameterException e) {
			e.printStackTrace();
			MainUtils.usage(e.getMessage(), jc);
		}

		try {
			final AccumuloIndexingConfiguration config = loadConf(commonOptions.accumuloProperties);
			try (final RemoteFluoVerification v = new RemoteFluoVerification(config)) {

				if("load".equals(jc.getParsedCommand())) {
					v.load(loadCommand.inputFile);
				} else if ("register-pcj-query".equals(jc.getParsedCommand())) {
					v.registerPcjQuery(loadQueries(queryCommand.queries));
				}  else if ("query".equals(jc.getParsedCommand())) {
					v.query(loadQueries(queryCommand.queries));
				}
				else {
					throw new IllegalStateException("Unexpected command");
				}

			}

		} catch (final Exception e) {
			logger.error("Error Occurred", e);
		}
	}


	private static class MainUtils {

		/**
		 * Logs the command line usage error to {@link System#out}, prints the
		 * prgram usage, and then terminates the program with
		 * {@link System#exit(int)} with a status code of 1.
		 *
		 * @param error
		 * @param jc
		 */
		public static void usage(final String error, final JCommander jc) {
			System.out.println(error);
			if(jc != null) {
				jc.usage();
			}
			System.exit(1);
		}
	}

	/**
	 * Ensures that a file exists and it is not a directory
	 *
	 */
	public static class FileExistsValidator implements IValueValidator<File> {


		/* (non-Javadoc)
		 * @see com.beust.jcommander.IValueValidator#validate(java.lang.String, java.lang.Object)
		 */
		@Override
		public void validate(final String name, final File value) throws ParameterException {
			if(!value.exists()) {
				throw new ParameterException("Parameter " + name + " should exist on filesystem (could not find file: " + value.getAbsolutePath() + ")");
			}
			if(value.isDirectory()) {
				throw new ParameterException("Parameter " + name + " was expected to be a File, not a Directory (value: " + value.getAbsolutePath() + ")");
			}
		}
	}

}
