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

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation of a generic element. It's bound to the record and allows to read and write
 * values. It's schema aware.
 */
public interface YTEntity extends YTRecord {

  String DEFAULT_CLASS_NAME = "O";

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
   * Returns the property value as an YTEntity. If the property is a link, it will be loaded and
   * returned as an YTEntity. If the property is an YTEntity, exception will be thrown.
   *
   * @param name the property name
   * @return the property value as an YTEntity
   * @throws ODatabaseException if the property is not an YTEntity
   */
  @Nullable
  default YTEntity getElementProperty(String name) {
    var property = getProperty(name);
    if (property == null) {
      return null;
    }

    if (property instanceof YTEntity) {
      return (YTEntity) property;
    } else if (property instanceof YTIdentifiable identifiable) {
      return identifiable.getElement();
    }

    throw new ODatabaseException(
        "Property "
            + name
            + " is not an element property, it is a "
            + property.getClass().getName());
  }

  /**
   * Returns the property value as an YTVertex. If the property is a link, it will be loaded and
   * returned as an YTVertex. If the property is an YTVertex, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an YTVertex
   * @throws ODatabaseException if the property is not an YTVertex
   */
  @Nullable
  default YTVertex getVertexProperty(String propertyName) {
    var element = getElementProperty(propertyName);
    if (element == null) {
      return null;
    }

    return element
        .asVertex()
        .orElseThrow(() -> new ODatabaseException("Property " + propertyName + " is not a vertex"));
  }

  /**
   * Returns the property value as an YTEdge. If the property is a link, it will be loaded and
   * returned as an YTEdge. If the property is an YTEdge, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an YTEdge
   * @throws ODatabaseException if the property is not an YTEdge
   */
  @Nullable
  default YTEdge getEdgeProperty(String propertyName) {
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
   * /** Returns the property value as an YTBlob. If the property is a link, it will be loaded and
   * returned as an YTBlob. If the property is an YTBlob, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an YTBlob
   * @throws ODatabaseException if the property is not an YTBlob
   */
  @Nullable
  default YTBlob getBlobProperty(String propertyName) {
    var property = getProperty(propertyName);
    if (property == null) {
      return null;
    }

    if (property instanceof YTBlob) {
      return (YTBlob) property;
    } else if (property instanceof YTIdentifiable identifiable) {
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
   * {@code #YTVertex.DIRECTION_OUT_PREFIX} or {@code #YTVertex.DIRECTION_IN_PREFIX}.
   *
   * @param name  the property name*
   * @param <RET> the type of the property
   * @return Returns property value on time of transaction start.
   * @throws IllegalArgumentException if name starts with {@code #YTVertex.DIRECTION_OUT_PREFIX} or
   *                                  {@code #YTVertex.DIRECTION_IN_PREFIX}.
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
  YTIdentifiable getLinkProperty(String name);

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
  void setProperty(String name, Object value, YTType... fieldType);

  /**
   * Remove a property
   *
   * @param name the property name
   */
  <RET> RET removeProperty(String name);

  /**
   * Returns an instance of YTVertex representing current element
   *
   * @return An YTVertex that represents the current element. An empty optional if the current
   * element is not a vertex
   */
  Optional<YTVertex> asVertex();

  /**
   * Converts the current element to an YTVertex.
   *
   * @return An YTVertex that represents the current element. Returns null if the current element is
   * not a vertex.
   */
  @Nullable
  YTVertex toVertex();

  /**
   * Returns an instance of YTEdge representing current element
   *
   * @return An YTEdge that represents the current element. An empty optional if the current element
   * is not an edge
   */
  Optional<YTEdge> asEdge();

  /**
   * Converts the current element to an YTEdge.
   *
   * @return an YTEdge that represents the current element. If the current element is not an edge,
   * returns null.
   */
  @Nullable
  YTEdge toEdge();

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
  Optional<YTClass> getSchemaType();

  /**
   * Retrieves the schema class associated with this element.
   *
   * @return the schema class associated with this element, or null if it does not have a schema
   * class
   */
  @Nullable
  YTClass getSchemaClass();

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
   * Fills a document passing the field names/values as a Map String,Object where the keys are the
   * field names and the values are the field values.
   */
  void fromMap(final Map<String, ?> map);

  /**
   * Returns the document as Map String,Object . If the document has identity, then the @rid entry
   * is valued. If the document has a class, then the @class entry is valued.
   */
  Map<String, Object> toMap();

  /**
   * Returns true if the current element is embedded
   *
   * @return true if the current element is embedded
   */
  boolean isEmbedded();
}
