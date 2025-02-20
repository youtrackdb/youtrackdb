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
package com.jetbrains.youtrack.db.internal.core.index;

import com.fasterxml.jackson.core.JsonGenerator;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Presentation of index that is used information and contained in entity {@link SchemaClass} .
 *
 * <p>This object cannot be created directly, use {@link
 * SchemaClass} manipulation method instead.
 */
public interface IndexDefinition extends IndexCallback {

  /**
   * @return Names of fields which given index is used to calculate key value. Order of fields is
   * important.
   */
  List<String> getFields();

  /**
   * @return Names of fields and their index modifiers (like "by value" for fields that hold <code>
   * Map</code> values) which given index is used to calculate key value. Order of fields is
   * important.
   */
  List<String> getFieldsToIndex();

  /**
   * @return Name of the class which this index belongs to.
   */
  String getClassName();

  /**
   * {@inheritDoc}
   */
  boolean equals(Object index);

  /**
   * {@inheritDoc}
   */
  int hashCode();

  /**
   * {@inheritDoc}
   */
  String toString();

  /**
   * Calculates key value by passed in parameters.
   *
   * <p>If it is impossible to calculate key value by given parameters <code>null</code> will be
   * returned.
   *
   * @param session Currently active database session.
   * @param params  Parameters from which index key will be calculated.
   * @return Key value or null if calculation is impossible.
   */
  Object createValue(DatabaseSessionInternal session, List<?> params);

  /**
   * Calculates key value by passed in parameters.
   *
   * <p>If it is impossible to calculate key value by given parameters <code>null</code> will be
   * returned.
   *
   * @param session Currently active database session.
   * @param params  Parameters from which index key will be calculated.
   * @return Key value or null if calculation is impossible.
   */
  Object createValue(DatabaseSessionInternal session, Object... params);

  /**
   * Returns amount of parameters that are used to calculate key value. It does not mean that all
   * parameters should be supplied. It only means that if you provide more parameters they will be
   * ignored and will not participate in index key calculation.
   *
   * @return Amount of that are used to calculate key value. Call result should be equals to
   * {@code getTypes().length}.
   */
  int getParamCount();

  /**
   * Return types of values from which index key consist. In case of index that is built on single
   * entity property value single array that contains property type will be returned. In case of
   * composite indexes result will contain several key types.
   *
   * @return Types of values from which index key consist.
   */
  PropertyType[] getTypes();

  /**
   * Serializes internal index state to map.
   */
  @Nonnull
  Map<String, Object> toMap(DatabaseSessionInternal session);

  void toJson(@Nonnull JsonGenerator jsonGenerator);

  /**
   * Deserialize internal index state from map.
   *
   * @param map Serialized index presentation.
   */
  void fromMap(@Nonnull Map<String, ?> map);

  String toCreateIndexDDL(String indexName, String indexType, String engine);

  boolean isAutomatic();

  Collate getCollate();

  void setCollate(Collate collate);

  boolean isNullValuesIgnored();

  void setNullValuesIgnored(boolean value);
}
