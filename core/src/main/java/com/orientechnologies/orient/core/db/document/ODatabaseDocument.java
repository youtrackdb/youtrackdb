/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.OEdgeInternal;

/**
 * Generic interface for document based Database implementations.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabaseDocument extends ODatabase<ORecord> {
  /**
   * Create a new instance of a blob containing the given bytes.
   *
   * @param bytes content of the OBlob
   * @return the OBlob instance.
   */
  OBlob newBlob(byte[] bytes);

  /**
   * Create a new empty instance of a blob.
   *
   * @return the OBlob instance.
   */
  OBlob newBlob();

  /**
   * Flush all indexes and cached storage content to the disk.
   *
   * <p>After this call users can perform only select queries. All write-related commands will
   * queued till {@link #release()} command will be called.
   *
   * <p>Given command waits till all on going modifications in indexes or DB will be finished.
   *
   * <p>IMPORTANT: This command is not reentrant.
   */
  void freeze();

  /**
   * Allows to execute write-related commands on DB. Called after {@link #freeze()} command.
   */
  void release();

  /**
   * Flush all indexes and cached storage content to the disk.
   *
   * <p>After this call users can perform only select queries. All write-related commands will
   * queued till {@link #release()} command will be called or exception will be thrown on attempt to
   * modify DB data. Concrete behaviour depends on <code>throwException</code> parameter.
   *
   * <p>IMPORTANT: This command is not reentrant.
   *
   * @param throwException If <code>true</code>
   *                       {@link
   *                       com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *                       exception will be thrown in case of write command will be performed.
   */
  void freeze(boolean throwException);

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   * otherwise.
   */
  boolean isPooled();

  /**
   * Add a cluster for blob records.
   *
   * @param iClusterName Cluster name
   * @param iParameters  Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addBlobCluster(String iClusterName, Object... iParameters);

  OElement newElement();

  OElement newElement(final String className);

  /**
   * Creates a new Edge of type E
   *
   * @param from the starting point vertex
   * @param to   the endpoint vertex
   * @return the edge
   */
  default OEdge newEdge(OVertex from, OVertex to) {
    return newEdge(from, to, "E");
  }

  /**
   * Creates a new Edge
   *
   * @param from the starting point vertex
   * @param to   the endpoint vertex
   * @param type the edge type
   * @return the edge
   */
  OEdge newEdge(OVertex from, OVertex to, OClass type);

  /**
   * Creates a new Edge
   *
   * @param from the starting point vertex
   * @param to   the endpoint vertex
   * @param type the edge type
   * @return the edge
   */
  OEdgeInternal newEdge(OVertex from, OVertex to, String type);

  /**
   * Creates a new Vertex of type V
   */
  default OVertex newVertex() {
    return newVertex("V");
  }

  /**
   * Creates a new Vertex
   *
   * @param type the vertex type
   */
  OVertex newVertex(OClass type);

  /**
   * Creates a new Vertex
   *
   * @param type the vertex type (class name)
   */
  OVertex newVertex(String type);

  /**
   * creates a new vertex class (a class that extends V)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws OSchemaException if the class already exists or if V class is not defined (Eg. if it
   *                          was deleted from the schema)
   */
  default OClass createVertexClass(String className) throws OSchemaException {
    return createClass(className, "V");
  }

  /**
   * creates a new edge class (a class that extends E)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws OSchemaException if the class already exists or if E class is not defined (Eg. if it
   *                          was deleted from the schema)
   */
  default OClass createEdgeClass(String className) {
    var edgeClass = createClass(className, "E");

    edgeClass.createProperty(OEdge.DIRECTION_IN, OType.LINK);
    edgeClass.createProperty(OEdge.DIRECTION_OUT, OType.LINK);

    return edgeClass;
  }

  /**
   * Creates a new edge class for lightweight edge (an abstract class that extends E)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws OSchemaException if the class already exists or if E class is not defined (Eg. if it
   *                          was deleted from the schema)
   */
  default OClass createLightweightEdgeClass(String className) {
    return createAbstractClass(className, "E");
  }

  /**
   * If a class with given name already exists, it's just returned, otherwise the method creates a
   * new class and returns it.
   *
   * @param className    the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws OSchemaException if one of the superclasses does not exist in the schema
   */
  default OClass createClassIfNotExist(String className, String... superclasses)
      throws OSchemaException {
    OSchema schema = getMetadata().getSchema();
    schema.reload();

    OClass result = schema.getClass(className);
    if (result == null) {
      result = createClass(className, superclasses);
    }
    schema.reload();
    return result;
  }
}
