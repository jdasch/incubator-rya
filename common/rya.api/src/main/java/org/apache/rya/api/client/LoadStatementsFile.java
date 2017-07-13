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
package org.apache.rya.api.client;

import java.io.File;

import org.openrdf.rio.RDFFormat;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Loads a local file of RDF statements into an instance of Rya.
 */
@DefaultAnnotation(NonNull.class)
public interface LoadStatementsFile {

	/**
	 * Loads a local file of RDF statements into an instance of Rya.
	 *
	 * @param ryaInstanceName - The name of the Rya instance the statements will be loaded into. (not null)
	 * @param statementsFile - A file that holds RDF statements that will be loaded. (not null)
	 * @param format - The format of the statements file. (not null)
	 * @param flushEachUpdate - If true, each statement will be flushed individually. If false, statements will be
	 *            grouped in an unspecified manner. A value of false will typically yield higher ingest rates.
	 * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
	 * @throws RyaClientException Something caused the command to fail.
	 */
    public void loadStatements(String ryaInstanceName, File statementsFile, RDFFormat format, boolean flushEachUpdate) throws InstanceDoesNotExistException, RyaClientException;
}