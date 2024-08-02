/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * @author Luigi Dell'Aquila
 */
public interface OEdge extends OElement {

  /** The name of the class of the edge record */
  String CLASS_NAME = OClass.EDGE_CLASS_NAME;

  /**
   * The name of the property that represents link to the vertex from which the edge is going out.
   * This property is used in the internal representation of the edge and can not be updated
   * directly.
   *
   * <p>You can apply composite unique index to this and {@link #DIRECTION_IN} properties to ensure
   * one-to-one relationship between vertices.
   */
  String DIRECTION_OUT = "out";

  /**
   * The name of the property that represents link to the vertex to which the edge is going in. This
   * property is used in the internal representation of the edge and can not be updated directly.
   *
   * <p>You can apply composite unique index to this and {@link #DIRECTION_OUT} properties to ensure
   * one-to-one relationship between vertices.
   */
  String DIRECTION_IN = "in";

  /**
   * Retrieves the vertex that this edge originates from.
   *
   * @return the vertex that this edge originates from
   */
  OVertex getFrom();

  /**
   * Retrieves the identifiable object from where this edge originates.
   *
   * @return the identifiable object from where this edge originates
   */
  OIdentifiable getFromIdentifiable();

  /**
   * Retrieves the vertex that this edge connects to.
   *
   * @return the vertex that this edge connects to
   */
  OVertex getTo();

  /**
   * Retrieves the identifiable object from where the edge connects to.
   *
   * @return the identifiable object from where the edge connects to
   */
  OIdentifiable getToIdentifiable();

  /**
   * Checks if the edge is lightweight.
   *
   * @return true if the edge is lightweight, false otherwise.
   */
  boolean isLightweight();

  /**
   * Retrieves the vertex connected to this edge in the specified direction.
   *
   * @param dir the direction of the edge (IN or OUT)
   * @return the vertex connected to this edge in the specified direction, or null if no vertex is
   *     connected
   */
  default OVertex getVertex(ODirection dir) {
    if (dir == ODirection.IN) {
      return getTo();
    } else if (dir == ODirection.OUT) {
      return getFrom();
    }
    throw new IllegalArgumentException("Direction not supported: " + dir);
  }

  /**
   * Retrieves the identifiable object of the vertex connected to this edge in the specified
   * direction.
   *
   * @param dir the direction of the edge (IN or OUT)
   * @return the identifiable object of the vertex connected to this edge in the specified
   *     direction, or null if no vertex is connected
   */
  default OIdentifiable getVertexLink(ODirection dir) {
    if (dir == ODirection.IN) {
      return getToIdentifiable();
    } else if (dir == ODirection.OUT) {
      return getFromIdentifiable();
    }
    throw new IllegalArgumentException("Direction not supported: " + dir);
  }

  /**
   * Check if the given labels match the labels of the edge.
   *
   * @param labels the labels to check
   * @return true if the labels match, false otherwise
   */
  default boolean isLabeled(String[] labels) {
    if (labels == null) {
      return true;
    }
    if (labels.length == 0) {
      return true;
    }
    Set<String> types = new HashSet<>();

    Optional<OClass> typeClass = getSchemaType();
    if (typeClass.isPresent()) {
      types.add(typeClass.get().getName());
      typeClass.get().getAllSuperClasses().stream().map(OClass::getName).forEach(types::add);
    } else {
      types.add(CLASS_NAME);
    }
    for (String s : labels) {
      for (String type : types) {
        if (type.equalsIgnoreCase(s)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Returns the names of defined properties except of properties used to manage edges.
   *
   * @return all the names of defined properties
   */
  Set<String> getPropertyNames();

  /**
   * Gets a property given its name. if the property name equals to {@link #DIRECTION_IN} or {@link
   * #DIRECTION_OUT} method throws {@link IllegalArgumentException}. Those names are used to manage
   * edges.
   *
   * @param name the property name
   * @return Returns the property value
   * @throws IllegalArgumentException if booked property name is used.
   */
  <RET> RET getProperty(String name);

  /**
   * This method similar to {@link #getProperty(String)} bun unlike before mentioned method it does
   * not load link automatically. if the property name equals to {@link #DIRECTION_IN} or {@link
   * #DIRECTION_OUT} method throws {@link IllegalArgumentException}. Those names are used to manage
   * edges.
   *
   * @param name the name of the link property
   * @return the link property value, or null if the property does not exist
   * @throws IllegalArgumentException if booked property name is used or requested property is not a
   *     link.
   * @see #getProperty(String)
   */
  @Nullable
  @Override
  OIdentifiable getLinkProperty(String name);

  /**
   * Check if a property exists in the Element. if the property name equals to {@link #DIRECTION_IN}
   * or {@link #DIRECTION_OUT} method throws {@link IllegalArgumentException}. Those names are used
   * to manage edges.
   *
   * @param propertyName Name of the property to check.
   * @return true if exists otherwise false.
   * @throws IllegalArgumentException if booked property name is used.
   */
  boolean hasProperty(final String propertyName);

  /**
   * Sets a property value, if the property name equals to {@link #DIRECTION_IN} or {@link
   * #DIRECTION_OUT} update of such property is aborted. Those names are used to
   *
   * @param name the property name
   * @param value the property value
   * @throws IllegalArgumentException if booked property name is used.
   */
  void setProperty(String name, Object value);

  /**
   * Sets a property value, if the property name equals to {@link #DIRECTION_IN} or {@link
   * #DIRECTION_OUT} update of such property is aborted. Those names are used to manage edges.
   *
   * @param name the property name
   * @param value the property value
   * @param fieldType Forced type (not auto-determined)
   * @throws IllegalArgumentException if booked property name is used.
   */
  void setProperty(String name, Object value, OType... fieldType);

  /**
   * Remove a property, if the property name equals to {@link #DIRECTION_IN} or {@link
   * #DIRECTION_OUT} removal of such property is aborted. Those names are used to manage edges.
   *
   * @param name the property name
   * @throws IllegalArgumentException if booked property name is used.
   */
  <RET> RET removeProperty(String name);
}
