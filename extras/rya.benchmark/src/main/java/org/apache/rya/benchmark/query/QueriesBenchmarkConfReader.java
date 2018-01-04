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
package org.apache.rya.benchmark.query;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.rya.api.utils.XmlFactoryConfiguration;
import org.xml.sax.SAXException;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Unmarshalls instances of {@link QueriesBenchmarkConf}.
 */
@DefaultAnnotation(NonNull.class)
public final class QueriesBenchmarkConfReader {

    // It is assumed the schema file is held within the root directory of the packaged jar.
    private static final String SCHEMA_LOCATION = "queries-benchmark-conf.xsd";

    // Only load the Schema once.
    private static final Supplier<Schema> SCHEMA_SUPPLIER = Suppliers.memoize(
            new Supplier<Schema>() {
                @Override
                public Schema get() {
                    final InputStream schemaStream = ClassLoader.getSystemResourceAsStream(SCHEMA_LOCATION);
                    final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    try {
                        return schemaFactory.newSchema( new StreamSource( schemaStream ) );
                    } catch (final SAXException e) {
                        throw new RuntimeException("Could not load the '" + SCHEMA_LOCATION + "' schema file. Make sure it is on the classpath.", e);
                            } finally {
                                if (schemaStream != null) {
                                    try {
                                        schemaStream.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                    }
                }
            });

    /**
     * Unmarshall an instance of {@link QueriesBenchmarkConf} from the XML that
     * is retrieved from an {@link InputStream}.
     *
     * @param xmlStream - The input stream holding the XML. (not null)
     * @return The {@link BenchmarkQueries} instance that was read from the stream.
     * @throws JAXBException There was a problem with the formatting of the XML.
     * @throws ParserConfigurationException There was a problem creating the DocumentBuilder.
     * @throws IOException There was a problem reading the xmlStream.
     * @throws SAXException There was a problem parsing the xmlStream.
     */
    public QueriesBenchmarkConf load(final InputStream xmlStream) throws JAXBException, ParserConfigurationException, SAXException, IOException {
        requireNonNull(xmlStream);

        // Load the schema that describes the stream.
        final Schema schema = SCHEMA_SUPPLIER.get();

        // Unmarshal the object from the stream.
        final JAXBContext context = JAXBContext.newInstance( QueriesBenchmarkConf.class );
        final Unmarshaller unmarshaller = context.createUnmarshaller();
        unmarshaller.setSchema(schema);
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        XmlFactoryConfiguration.harden(dbf);
        final DocumentBuilder db = dbf.newDocumentBuilder();
        return (QueriesBenchmarkConf) unmarshaller.unmarshal(db.parse(xmlStream));
    }
}