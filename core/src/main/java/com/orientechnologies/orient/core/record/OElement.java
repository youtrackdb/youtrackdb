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
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.OBlob;
import java.util.Collection;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation of a generic element. It's bound to the record and allows to read and write
 * values. It's schema aware.
 *
 * @author Luigi Dell'Aquila
 */
public interface OElement extends ORecord {

  /**
   * Returns all the names of defined properties
   *
   * @return all the names of defined properties
   */
  Collection<String> getPropertyNames();

  /**
   * Gets a property given its name This method loads linked record automatically if you prefer to
   * work with lazy loaded record use {@link #getLinkProperty(String)}
   *
   * @param name  the property name
   * @param <RET> the type of the property
   * @return Returns the property value
   * @see #getLinkProperty(String)
   * @see #getElementProperty(String)
   * @see #getVertexProperty(String)
   * @see #getEdgeProperty(String)
   * @see #getBlobProperty(String)
   */
  @Nullable
  <RET> RET getProperty(String name);

  /**
   * Returns the property value as an OElement. If the property is a link, it will be loaded and
   * returned as an OElement. If the property is an OElement, exception will be thrown.
   *
   * @param name the property name
   * @return the property value as an OElement
   * @throws ODatabaseException if the property is not an OElement
   */
  @Nullable
  default OElement getElementProperty(String name) {
    var property = getProperty(name);
    if (property == null) {
      return null;
    }

    if (property instanceof OElement) {
      return (OElement) property;
    } else if (property instanceof OIdentifiable identifiable) {
      return identifiable.getElement();
    }

    throw new ODatabaseException(
        "Property "
            + name
            + " is not an element property, it is a "
            + property.getClass().getName());
  }

  /**
   * Returns the property value as an OVertex. If the property is a link, it will be loaded and
   * returned as an OVertex. If the property is an OVertex, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an OVertex
   * @throws ODatabaseException if the property is not an OVertex
   */
  @Nullable
  default OVertex getVertexProperty(String propertyName) {
    var element = getElementProperty(propertyName);
    if (element == null) {
      return null;
    }

    return element
        .asVertex()
        .orElseThrow(() -> new ODatabaseException("Property " + propertyName + " is not a vertex"));
  }

  /**
   * Returns the property value as an OEdge. If the property is a link, it will be loaded and
   * returned as an OEdge. If the property is an OEdge, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an OEdge
   * @throws ODatabaseException if the property is not an OEdge
   */
  @Nullable
  default OEdge getEdgeProperty(String propertyName) {
    var element = getElementProperty(propertyName);
    if (element == null) {
      return null;
    }

    return element
        .asEdge()
        .orElseThrow(() -> new ODatabaseException("Property " + propertyName + " is not an edge"));
  }

  /**
   * boolean containsProperty(String name);
   * <p>
   * /** Returns the property value as an OBlob. If the property is a link, it will be loaded and
   * returned as an OBlob. If the property is an OBlob, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an OBlob
   * @throws ODatabaseException if the property is not an OBlob
   */
  @Nullable
  default OBlob getBlobProperty(String propertyName) {
    var property = getProperty(propertyName);
    if (property == null) {
      return null;
    }

    if (property instanceof OBlob) {
      return (OBlob) property;
    } else if (property instanceof OIdentifiable identifiable) {
      return identifiable.getBlob();
    }

    throw new ODatabaseException(
        "Property "
            + propertyName
            + " is not a blob property, it is a "
            + property.getClass().getName());
  }

  /**
   * Gets a property value on time of transaction start. This will work for scalar values, and
   * collections of scalar values. Will throw exception in case of called with name starting with
   * {@code #OVertex.DIRECTION_OUT_PREFIX} or {@code #OVertex.DIRECTION_IN_PREFIX}.
   *
   * @param name  the property name*
   * @param <RET> the type of the property
   * @return Returns property value on time of transaction start.
   * @throws IllegalArgumentException if name starts with {@code #OVertex.DIRECTION_OUT_PREFIX} or
   *                                  {@code #OVertex.DIRECTION_IN_PREFIX}.
   */
  <RET> RET getPropertyOnLoadValue(String name);

  /**
   * This method similar to {@link #getProperty(String)} bun unlike before mentioned method it does
   * not load link automatically.
   *
   * @param name the name of the link property
   * @return the link property value, or null if the property does not exist
   * @throws IllegalArgumentException if requested property is not a link.
   * @see #getProperty(String)
   */
  @Nullable
  OIdentifiable getLinkProperty(String name);

  /**
   * Check if a property exists in the Element
   *
   * @param propertyName the property name
   * @return true if exists otherwise false.
   */
  boolean hasProperty(final String propertyName);

  /**
   * Sets a property value
   *
   * @param name  the property name
   * @param value the property value
   */
  void setProperty(String name, Object value);

  /**
   * Sets a property value
   *
   * @param name      the property name
   * @param value     the property value
   * @param fieldType Forced type (not auto-determined)
   */
  void setProperty(String name, Object value, OType... fieldType);

  /**
   * Remove a property
   *
   * @param name the property name
   */
  <RET> RET removeProperty(String name);

  /**
   * Returns an instance of OVertex representing current element
   *
   * @return An OVertex that represents the current element. An empty optional if the current
   * element is not a vertex
   */
  Optional<OVertex> asVertex();

  /**
   * Converts the current element to an OVertex.
   *
   * @return An OVertex that represents the current element. Returns null if the current element is
   * not a vertex.
   */
  @Nullable
  OVertex toVertex();

  /**
   * Returns an instance of OEdge representing current element
   *
   * @return An OEdge that represents the current element. An empty optional if the current element
   * is not an edge
   */
  Optional<OEdge> asEdge();

  /**
   * Converts the current element to an OEdge.
   *
   * @return an OEdge that represents the current element. If the current element is not an edge,
   * returns null.
   */
  @Nullable
  OEdge toEdge();

  /**
   * return true if the current element is a vertex
   *
   * @return true if the current element is a vertex
   */
  boolean isVertex();

  /**
   * return true if the current element is an edge
   *
   * @return true if the current element is an edge
   */
  boolean isEdge();

  /**
   * Returns the type of current element, ie the class in the schema (if any)
   *
   * @return the type of current element. An empty optional is returned if current element does not
   * have a schema
   */
  Optional<OClass> getSchemaType();

  /**
   * Retrieves the schema class associated with this element.
   *
   * @return the schema class associated with this element, or null if it does not have a schema
   * class
   */
  @Nullable
  OClass getSchemaClass();

  /**
   * Retrieves the class name associated with this element.
   *
   * @return the class name associated with this element, or null if it does not have a schema class
   */
  @Nullable
  default String getClassName() {
    var schemaClass = getSchemaClass();
    if (schemaClass == null) {
      return null;
    }

    return schemaClass.getName();
  }

  /**
   * Creates a copy of the record. All the record contents are copied.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   * in chain.
   */
  @Override
  OElement copy();

  /**
   * Returns true if the current element is embedded
   *
   * @return true if the current element is embedded
   */
  boolean isEmbedded();
}
