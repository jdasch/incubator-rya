package mvm.rya.sail.config;

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

import mvm.rya.accumulo.AccumuloRdfConfiguration;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailImplConfigBase;

public class RyaAccumuloSailConfig extends SailImplConfigBase {

    public static final String NAMESPACE = "http://rya.apache.org/RyaAccumuloSail/Config#";

    public static final URI INSTANCE;
    public static final URI USER;
    public static final URI PASSWORD;
    public static final URI ZOOKEEPERS;
    public static final URI IS_MOCK;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        USER = factory.createURI(NAMESPACE, "user");
        PASSWORD = factory.createURI(NAMESPACE, "password");
        INSTANCE = factory.createURI(NAMESPACE, "instance");
        ZOOKEEPERS = factory.createURI(NAMESPACE, "zookeepers");
        IS_MOCK = factory.createURI(NAMESPACE, "isMock");
    }
    
    private String user = "";
    private String userp = "";
    private String instance = "";
    private String zookeepers = "";
    private boolean isMock = false;
    
    public RyaAccumuloSailConfig() {
        super(RyaAccumuloSailFactory.SAIL_TYPE);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return userp;
    }

    public void setPassword(String password) {
        this.userp = password;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public boolean isMock() {
        return isMock;
    }

    public void setMock(boolean isMock) {
        this.isMock = isMock;
    }

    public AccumuloRdfConfiguration toRdfConfiguation() {
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        return conf;
    }

    @Override
    public void validate() throws SailConfigException {
        super.validate();
    }

    @Override
    public Resource export(Graph graph) {
        Resource implNode = super.export(graph);

        @SuppressWarnings("deprecation")
        ValueFactory v = graph.getValueFactory();

        graph.add(implNode, USER, v.createLiteral(user));
        graph.add(implNode, PASSWORD, v.createLiteral(userp));
        graph.add(implNode, INSTANCE, v.createLiteral(instance));
        graph.add(implNode, ZOOKEEPERS, v.createLiteral(zookeepers));
        graph.add(implNode, IS_MOCK, v.createLiteral(isMock));

        return implNode;
    }

    @Override
    public void parse(Graph graph, Resource implNode) throws SailConfigException {
        super.parse(graph, implNode);
        System.out.println("parsing");

        try {
            Literal userLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, USER);
            if (userLit != null) {
                setUser(userLit.getLabel());
            }
            Literal pwdLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, PASSWORD);
            if (pwdLit != null) {
                setPassword(pwdLit.getLabel());
            }
            Literal instLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, INSTANCE);
            if (instLit != null) {
                setInstance(instLit.getLabel());
            }
            Literal zooLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, ZOOKEEPERS);
            if (zooLit != null) {
                setZookeepers(zooLit.getLabel());
            }
            Literal mockLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, IS_MOCK);
            if (mockLit != null) {
                setMock(Boolean.parseBoolean(mockLit.getLabel()));
            }
        } catch (GraphUtilException e) {
            throw new SailConfigException(e.getMessage(), e);
        }
    }
}
