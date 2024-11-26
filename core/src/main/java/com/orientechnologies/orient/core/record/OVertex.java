/*
 *
 *
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
 *
 *
 */
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.OEdgeInternal;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * OVertex interface represents a vertex in a graph database. Unlike non-typed record it treats some
 * property names differently, namely properties with names starting with prefixes
 * {@code #DIRECTION_IN_PREFIX} and {@code #DIRECTION_OUT_PREFIX} are considered as booked and
 * should not be used by users directly.
 */
public interface OVertex extends OElement {

  /**
   * The name of the class of the vertex record
   */
  String CLASS_NAME = OClass.VERTEX_CLASS_NAME;

  /**
   * A constant variable representing the prefix used for outbound edges in a graph.
   */
  String DIRECTION_OUT_PREFIX = "out_";

  /**
   * A constant variable representing the prefix used for inbound edges in a graph.
   */
  String DIRECTION_IN_PREFIX = "in_";

  /**
   * Returns the names of defined properties except of properties used to manage edges.
   *
   * @return all the names of defined properties
   */
  Set<String> getPropertyNames();

  /**
   * Gets a property given its name. if the property name starts with {@link #DIRECTION_IN_PREFIX}
   * or {@link #DIRECTION_OUT_PREFIX} method throws {@link IllegalArgumentException}. Those names
   * are used to manage edges.
   *
   * @param name the property name
   * @return Returns the property value
   * @throws IllegalArgumentException if booked property name is used.
   */
  <RET> RET getProperty(String name);

  /**
   * This method similar to {@link #getProperty(String)} bun unlike before mentioned method it does
   * not load link automatically. if the property name starts with {@link #DIRECTION_IN_PREFIX} or
   * {@link #DIRECTION_OUT_PREFIX} method throws {@link IllegalArgumentException}. Those names are
   * used to manage edges.
   *
   * @param name the name of the link property
   * @return the link property value, or null if the property does not exist
   * @throws IllegalArgumentException if booked property name is used or requested property is not a
   *                                  link.
   * @see #getProperty(String)
   */
  @Nullable
  @Override
  OIdentifiable getLinkProperty(String name);

  /**
   * Check if a property exists in the Element. if the property name starts with
   * {@link #DIRECTION_IN_PREFIX} or {@link #DIRECTION_OUT_PREFIX} method throws
   * {@link IllegalArgumentException}. Those names are used to manage edges.
   *
   * @param propertyName Name of the property to check.
   * @return true if exists otherwise false.
   * @throws IllegalArgumentException if booked property name is used.
   */
  boolean hasProperty(final String propertyName);

  /**
   * Sets a property value, if the property name starts with {@link #DIRECTION_IN_PREFIX} or
   * {@link #DIRECTION_OUT_PREFIX} update of such property is aborted. Those names are used to
   *
   * @param name  the property name
   * @param value the property value
   * @throws IllegalArgumentException if booked property name is used.
   */
  void setProperty(String name, Object value);

  /**
   * Sets a property value, if the property name starts with {@link #DIRECTION_IN_PREFIX} or
   * {@link #DIRECTION_OUT_PREFIX} update of such property is aborted. Those names are used to
   * manage edges.
   *
   * @param name      the property name
   * @param value     the property value
   * @param fieldType Forced type (not auto-determined)
   * @throws IllegalArgumentException if booked property name is used.
   */
  void setProperty(String name, Object value, OType... fieldType);

  /**
   * Remove a property, if the property name starts with {@link #DIRECTION_IN_PREFIX} or
   * {@link #DIRECTION_OUT_PREFIX} removal of such property is aborted. Those names are used to
   * manage edges.
   *
   * @param name the property name
   * @throws IllegalArgumentException if booked property name is used.
   */
  <RET> RET removeProperty(String name);

  /**
   * Returns the names of the edges connected to the vertex in both directions. It is identical to
   * the call : {@code getEdgeNames(ODirection.BOTH)}
   *
   * @return a set of strings containing the names of the edges
   */
  Set<String> getEdgeNames();

  /**
   * Returns the names of the edges connected to the vertex, filtered by the given direction.
   *
   * @param direction the direction of the edges to retrieve (IN, OUT, or BOTH)
   * @return a Set of String containing the names of the edges
   */
  Set<String> getEdgeNames(ODirection direction);

  /**
   * Retrieves all edges connected to the vertex in the given direction.
   *
   * @param direction the direction of the edges to retrieve (OUT, IN, or BOTH)
   * @return an iterable collection of edges connected to the vertex
   */
  Iterable<OEdge> getEdges(ODirection direction);

  /**
   * Retrieves all edges connected to the vertex in the given direction and with the specified
   * label(s).
   *
   * @param direction the direction of the edges to retrieve (OUT, IN, or BOTH)
   * @param label     the label(s) of the edges to retrieve
   * @return an iterable collection of edges connected to the vertex
   */
  Iterable<OEdge> getEdges(ODirection direction, String... label);

  /**
   * Retrieves all edges connected to the vertex in the given direction and with the specified
   * label(s).
   *
   * @param direction the direction of the edges to retrieve (OUT, IN, or BOTH)
   * @param label     the label(s) of the edges to retrieve
   * @return an iterable collection of edges connected to the vertex
   */
  Iterable<OEdge> getEdges(ODirection direction, OClass... label);

  /**
   * Retrieves all vertices connected to the current vertex in the specified direction.
   *
   * @param direction the direction of the vertices to retrieve (OUT, IN, or BOTH)
   * @return an iterable collection of vertices connected to the current vertex
   */
  Iterable<OVertex> getVertices(ODirection direction);

  /**
   * Returns the vertices connected to the current vertex in the specified direction and with the
   * specified label(s).
   *
   * @param direction the direction of the vertices to retrieve (OUT, IN, or BOTH)
   * @param label     the label(s) of the vertices to retrieve
   * @return an iterable collection of vertices connected to the current vertex
   */
  Iterable<OVertex> getVertices(ODirection direction, String... label);

  /**
   * Retrieves all vertices connected to the current vertex in the specified direction and with the
   * specified label(s).
   *
   * @param direction the direction of the vertices to retrieve (OUT, IN, or BOTH)
   * @param label     the label(s) of the vertices to retrieve
   * @return an iterable collection of vertices connected to the current vertex
   */
  Iterable<OVertex> getVertices(ODirection direction, OClass... label);

  /**
   * Adds an edge between the current vertex and the specified vertex. Edge will be created without
   * any specific label. It is recommended to use labeled edges instead.
   *
   * @param to the vertex to which the edge is connected
   * @return the created edge
   */
  OEdge addEdge(OVertex to);

  /**
   * Adds a lightweight edge (one that does not require associated record) between the current
   * vertex and the specified vertex. Edge will be created without any specific label. It is
   * recommended to use labeled edges instead.
   *
   * @param to the vertex to which the edge is connected
   * @return the created edge
   */
  OEdge addLightWeightEdge(OVertex to);

  /**
   * Adds an edge between the current vertex and the specified vertex.
   *
   * @param to    the vertex to which the edge is connected
   * @param label the label of the edge (optional)
   * @return the created edge
   */
  OEdge addEdge(OVertex to, String label);

  /**
   * Adds a lightweight edge (one that does not require associated record) between the current
   * vertex and the specified vertex.
   *
   * @param to    the vertex to which the edge is connected
   * @param label the label of the edge (optional)
   * @return the created edge
   */
  OEdge addLightWeightEdge(OVertex to, String label);

  /**
   * Adds an edge between the current vertex and the specified vertex with the given label.
   *
   * @param to    the vertex to which the edge is connected
   * @param label the label of the edge
   * @return the created edge
   */
  OEdge addEdge(OVertex to, OClass label);

  /**
   * Adds a lightweight edge (one that does not require associated record) between the current
   * vertex and the specified vertex with the given label.
   *
   * @param to    the vertex to which the edge is connected
   * @param label the label of the edge
   * @return the created edge
   */
  OEdge addLightWeightEdge(OVertex to, OClass label);

  /**
   * Moves the vertex to the specified class and cluster.
   *
   * @param className   the name of the class to move the vertex to
   * @param clusterName the name of the cluster to move the vertex to
   * @return the new RecordID of the moved vertex
   */
  ORID moveTo(final String className, final String clusterName);

  /**
   * Removes all edges connected to the vertex in the given direction.
   *
   * @param direction the direction of the edges to remove (OUT, IN, or BOTH)
   * @param labels    the labels of the edges to remove
   */
  default void removeEdges(ODirection direction, OClass... labels) {
    var edges = getEdges(direction, labels);
    for (var edge : edges) {
      edge.delete();
    }
  }

  /**
   * Removes all edges connected to the vertex in the given direction.
   *
   * @param direction the direction of the edges to remove (OUT, IN, or BOTH)
   * @param labels    the labels of the edges to remove
   */
  default void removeEdges(ODirection direction, String... labels) {
    var edges = getEdges(direction, labels);
    for (var edge : edges) {
      edge.delete();
    }
  }

  /**
   * Deletes the current vertex.
   */
  void delete();

  /**
   * Creates a copy of the current vertex.
   *
   * @return a copy of the current vertex
   */
  OVertex copy();

  /**
   * @return reference to current instance.
   */
  @Override
  default OVertex toVertex() {
    return this;
  }

  /**
   * Returns the name of the field used to store the link to the edge record.
   *
   * @param direction the direction of the edge
   * @param className the name of the edge class
   * @return the name of the field used to store the direct link to the edge
   */
  static String getEdgeLinkFieldName(final ODirection direction, final String className) {
    if (direction == null || direction == ODirection.BOTH) {
      throw new IllegalArgumentException("Direction not valid");
    }

    // PREFIX "out_" or "in_" TO THE FIELD NAME
    final String prefix = direction == ODirection.OUT ? DIRECTION_OUT_PREFIX : DIRECTION_IN_PREFIX;
    if (className == null || className.isEmpty() || className.equals(OEdgeInternal.CLASS_NAME)) {
      return prefix;
    }

    return prefix + className;
  }
}
