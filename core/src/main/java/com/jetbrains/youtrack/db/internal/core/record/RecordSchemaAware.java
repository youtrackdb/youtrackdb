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
package com.jetbrains.youtrack.db.internal.core.record;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.PropertyType;

/**
 * Generic record representation with a schema definition. The record has multiple fields. Fields
 * are also called properties.
 */
public interface RecordSchemaAware extends Entity {

  /**
   * Returns the value of a field.
   *
   * @param iFieldName Field name
   * @return Field value if exists, otherwise null
   */
  <RET> RET field(String iFieldName);

  /**
   * Sets the value for a field.
   *
   * @param iFieldName  Field name
   * @param iFieldValue Field value to set
   * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods
   * in chain.
   */
  RecordSchemaAware field(String iFieldName, Object iFieldValue);

  /**
   * Sets the value for a field forcing the type.This is useful when you want avoid automatic
   * conversions (for example record id to record) or need expressly a conversion between types.
   *
   * @param iFieldName  Field name
   * @param iFieldValue Field value to set
   * @param iType       Type between the values defined in the
   *                    {@link PropertyType} enum
   */
  RecordSchemaAware field(String iFieldName, Object iFieldValue, PropertyType... iType);

  /**
   * Removes a field. This operation does not set the field value to null but remove the field
   * itself.
   *
   * @param iFieldName Field name
   * @return The old value contained in the remove field
   */
  Object removeField(String iFieldName);

  /**
   * Tells if a field is contained in current record.
   *
   * @param iFieldName Field name
   * @return true if exists, otherwise false
   */
  boolean containsField(String iFieldName);

  /**
   * Returns the number of fields present in memory.
   *
   * @return Fields number
   */
  int fields();

  /**
   * Returns the record's field names. The returned Set object is disconnected by internal
   * representation, so changes don't apply to the record. If the fields are ordered the order is
   * maintained also in the returning collection.
   *
   * @return Set of string containing the field names
   */
  String[] fieldNames();

  /**
   * Returns the record's field values. The returned object array is disconnected by internal
   * representation, so changes don't apply to the record. If the fields are ordered the order is
   * maintained also in the returning collection.
   *
   * @return Object array of the field values
   */
  Object[] fieldValues();
}
