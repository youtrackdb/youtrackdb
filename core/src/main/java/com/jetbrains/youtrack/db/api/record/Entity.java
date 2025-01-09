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
package com.jetbrains.youtrack.db.api.record;

import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation of a generic entity. It's bound to the record and allows to read and write values.
 * It's schema aware.
 */
public interface Entity extends DBRecord {
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
   * Returns the property value as an Entity. If the property is a link, it will be loaded and
   * returned as an Entity. If the property is an Entity, exception will be thrown.
   *
   * @param name the property name
   * @return the property value as an Entity
   * @throws DatabaseException if the property is not an Entity
   */
  @Nullable
  default Entity getElementProperty(String name) {
    var property = getProperty(name);

    return switch (property) {
      case null -> null;
      case Entity entity -> entity;
      case Identifiable identifiable -> identifiable.getEntity();
      default -> throw new DatabaseException(
          "Property "
              + name
              + " is not an entity property, it is a "
              + property.getClass().getName());
    };

  }

  /**
   * Returns the property value as an Vertex. If the property is a link, it will be loaded and
   * returned as an Vertex. If the property is an Vertex, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an Vertex
   * @throws DatabaseException if the property is not an Vertex
   */
  @Nullable
  default Vertex getVertexProperty(String propertyName) {
    var entity = getElementProperty(propertyName);
    if (entity == null) {
      return null;
    }

    return entity
        .asVertex()
        .orElseThrow(
            () -> new DatabaseException("Property " + propertyName + " is not a vertex"));
  }

  /**
   * Returns the property value as an Edge. If the property is a link, it will be loaded and
   * returned as an Edge. If the property is an Edge, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an Edge
   * @throws DatabaseException if the property is not an Edge
   */
  @Nullable
  default Edge getEdgeProperty(String propertyName) {
    var entity = getElementProperty(propertyName);
    if (entity == null) {
      return null;
    }

    return entity
        .asEdge()
        .orElseThrow(() -> new DatabaseException("Property " + propertyName + " is not an edge"));
  }

  /**
   * boolean containsProperty(String name);
   * <p>
   * /** Returns the property value as an Blob. If the property is a link, it will be loaded and
   * returned as an Blob. If the property is an Blob, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an Blob
   * @throws DatabaseException if the property is not an Blob
   */
  @Nullable
  default Blob getBlobProperty(String propertyName) {
    var property = getProperty(propertyName);

    return switch (property) {
      case null -> null;
      case Blob blob -> blob;
      case Identifiable identifiable -> identifiable.getBlob();
      default -> throw new DatabaseException(
          "Property "
              + propertyName
              + " is not a blob property, it is a "
              + property.getClass().getName());
    };
  }

  /**
   * Gets a property value on time of transaction start. This will work for scalar values, and
   * collections of scalar values. Will throw exception in case of called with name starting with
   * {@code #Vertex.DIRECTION_OUT_PREFIX} or {@code #Vertex.DIRECTION_IN_PREFIX}.
   *
   * @param name  the property name*
   * @param <RET> the type of the property
   * @return Returns property value on time of transaction start.
   * @throws IllegalArgumentException if name starts with {@code #Vertex.DIRECTION_OUT_PREFIX} or
   *                                  {@code #Vertex.DIRECTION_IN_PREFIX}.
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
  Identifiable getLinkProperty(String name);

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
   * @param name         the property name
   * @param value        the property value
   * @param propertyType Forced type (not auto-determined)
   */
  void setProperty(String name, Object value, PropertyType... propertyType);

  /**
   * Remove a property
   *
   * @param name the property name
   */
  <RET> RET removeProperty(String name);

  /**
   * Returns an instance of Vertex representing current entity
   *
   * @return An Vertex that represents the current entity. An empty optional if the current entity
   * is not a vertex
   */
  Optional<Vertex> asVertex();

  /**
   * Converts the current entity to an Vertex.
   *
   * @return An Vertex that represents the current entity. Returns null if the current entity is not
   * a vertex.
   */
  @Nullable
  Vertex toVertex();

  /**
   * Returns an instance of Edge representing current entity
   *
   * @return An Edge that represents the current entity. An empty optional if the current entity is
   * not an edge
   */
  Optional<Edge> asEdge();

  /**
   * Converts the current entity to an Edge.
   *
   * @return an Edge that represents the current entity. If the current entity is not an edge,
   * returns null.
   */
  @Nullable
  Edge toEdge();

  /**
   * return true if the current entity is a vertex
   *
   * @return true if the current entity is a vertex
   */
  boolean isVertex();

  /**
   * return true if the current entity is an edge
   *
   * @return true if the current entity is an edge
   */
  boolean isEdge();

  /**
   * Returns the type of current entity, ie the class in the schema (if any)
   *
   * @return the type of current entity. An empty optional is returned if current entity does not
   * have a schema
   */
  Optional<SchemaClass> getSchemaType();

  /**
   * Retrieves the schema class associated with this entity.
   *
   * @return the schema class associated with this entity, or null if it does not have a schema
   * class
   */
  @Nullable
  SchemaClass getSchemaClass();

  /**
   * Retrieves the class name associated with this entity.
   *
   * @return the class name associated with this entity, or null if it does not have a schema class
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
   * Fills a entity passing the property names/values as a Map String,Object where the keys are the
   * property names and the values are the property values.
   */
  void fromMap(final Map<String, ?> map);

  /**
   * Returns the entity as Map String,Object . If the entity has identity, then the @rid entry is
   * valued. If the entity has a class, then the @class entry is valued.
   */
  Map<String, Object> toMap();

  /**
   * Returns true if the current entity is embedded
   *
   * @return true if the current entity is embedded
   */
  boolean isEmbedded();
}
