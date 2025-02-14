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

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Implementation of a generic entity. It's bound to the record and allows to read and write values.
 * It's schema aware.
 */
public interface Entity extends DBRecord, Result {
  String DEFAULT_CLASS_NAME = "O";
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
  <RET> RET getPropertyOnLoadValue(@Nonnull String name);

  /**
   * Sets a property value
   *
   * @param name  the property name
   * @param value the property value
   */
  void setProperty(@Nonnull String name, @Nullable Object value);

  @Nonnull
  <T> List<T> getOrCreateEmbeddedList(@Nonnull String name);

  @Nonnull
  <T> Set<T> getOrCreateEmbeddedSet(@Nonnull String name);

  @Nonnull
  <T> Map<String, T> getOrCreateEmbeddedMap(@Nonnull String name);

  @Nonnull
  List<Identifiable> getOrCreateLinkList(@Nonnull String name);

  @Nonnull
  Set<Identifiable> getOrCreateLinkSet(@Nonnull String name);

  @Nonnull
  Map<String, Identifiable> getOrCreateLinkMap(@Nonnull String name);

  /**
   * Sets a property value
   *
   * @param name         the property name
   * @param value        the property value
   * @param propertyType Forced type (not auto-determined)
   */
  void setProperty(@Nonnull String name, @Nullable Object value,
      @Nonnull PropertyType propertyType);

  /**
   * Remove a property
   *
   * @param name the property name
   */
  <RET> RET removeProperty(@Nonnull String name);

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
  String getSchemaClassName();

  /**
   * Returns true if the current entity is embedded
   *
   * @return true if the current entity is embedded
   */
  boolean isEmbedded();

  /**
   * Fills a entity passing the property names/values as a Map String,Object where the keys are the
   * property names and the values are the property values.
   */
  void updateFromMap(@Nonnull final Map<String, ?> map);

  /**
   * Returns the entity as <code>Map</code>. If specified includes entity metadata:
   *
   * <ol>
   *  <li>If the entity has identity, then the @rid entry is added.</li>
   *  <li>If the entity has a class, then the @class entry is added.</li>
   *  <li>If entity is embedded, then the @embedded entry is added.</li>
   * </ol>
   *
   * @param includeMetadata if true, includes metadata in the map
   */
  @Nonnull
  Map<String, Object> toMap(boolean includeMetadata);
}
