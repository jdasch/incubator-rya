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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import org.apache.fluo.api.data.Bytes;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Serializes and deserializes a {@link VisibilityBindingSet} to and from {@link Bytes} objects.
 */
@DefaultAnnotation(NonNull.class)
public class VisibilityBindingSetSerDe {

    /**
     * Serializes a {@link VisibilityBindingSet} into a {@link Bytes} object.
     *
     * @param bindingSet - The binding set that will be serialized. (not null)
     * @return The serialized object.
     * @throws Exception A problem was encountered while serializing the object.
     */
    public Bytes serialize(final VisibilityBindingSet bindingSet) throws Exception {
        requireNonNull(bindingSet);

        final ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try(final ObjectOutputStream oos = new ObjectOutputStream(boas)) {
            oos.writeObject(bindingSet);
        }

        return Bytes.of(boas.toByteArray());
    }

    /**
     * Deserializes a {@link VisibilityBindingSet} from a {@link Bytes} object.
     *
     * @param bytes - The bytes that will be deserialized. (not null)
     * @return The deserialized object.
     * @throws Exception A problem was encountered while deserializing the object.
     */
    public VisibilityBindingSet deserialize(final Bytes bytes) throws Exception {
        requireNonNull(bytes);

        try (final ObjectInputStream ois = new LookAheadObjectInputStream(new ByteArrayInputStream(bytes.toArray()))) {
            final Object o = ois.readObject();
            if(o instanceof VisibilityBindingSet) {
                return (VisibilityBindingSet) o;
            } else {
                throw new Exception("Deserialized Object is not a VisibilityBindingSet. Was: " + o.getClass());
            }
        }
    }

    /**
     * Only deserialize instances of our expected VisibilityBindingSet class or it's superclasses.
     * Without this, ObjectInputStream will instantiate a class, running it's constructor,
     * before we examine what it is. This is unsafe!
     */
    private class LookAheadObjectInputStream extends ObjectInputStream {

        public LookAheadObjectInputStream(InputStream inputStream) throws IOException {
            super(inputStream);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            final String name = desc.getName();
            System.out.println(name);
            if (!name.equals(org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet.class.getName())//
                            && !name.equals(org.apache.rya.indexing.pcj.storage.accumulo.BindingSetDecorator.class.getName()) //
                            && !name.equals(org.openrdf.query.impl.MapBindingSet.class.getName()) //
                            && !name.equals(java.util.LinkedHashMap.class.getName()) //
                            && !name.equals(java.util.HashMap.class.getName()) //
                            && !name.equals(org.openrdf.query.impl.BindingImpl.class.getName()) //
                            && !name.equals(org.openrdf.model.impl.LiteralImpl.class.getName()) //
                            && !name.equals(org.openrdf.model.impl.URIImpl.class.getName())) {
                throw new InvalidClassException(name, "Deserialized Object is not a VisibilityBindingSet.");
            }
            return super.resolveClass(desc);
        }
    }
}