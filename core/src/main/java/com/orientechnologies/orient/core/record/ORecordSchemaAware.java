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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Generic record representation with a schema definition. The record has multiple fields. Fields
 * are also called properties.
 *
 * @deprecated use {@link OElement} instead
 */
public interface ORecordSchemaAware extends OElement {

  /**
   * Returns the value of a field.
   *
   * @param iFieldName Field name
   * @return Field value if exists, otherwise null
   * @deprecated use {@link #getProperty(String)} instead
   */
  @Deprecated
  <RET> RET field(String iFieldName);

  /**
   * Returns the value of a field forcing the return type. This is useful when you want avoid
   * automatic conversions (for example record id to record) or need expressly a conversion between
   * types.
   *
   * @param iFieldName Field name
   * @param iType Type between the values defined in the {@link OType} enum
   * @return Field value if exists, otherwise null
   * @deprecated use {@link #setProperty(String, Object, OType...)} instead
   */
  @Deprecated
  <RET> RET field(String iFieldName, OType iType);

  /**
   * Sets the value for a field.
   *
   * @param iFieldName Field name
   * @param iFieldValue Field value to set
   * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   * @deprecated use {@link #setProperty(String, Object)} instead
   */
  @Deprecated
  ORecordSchemaAware field(String iFieldName, Object iFieldValue);

  /**
   * Sets the value for a field forcing the type.This is useful when you want avoid automatic
   * conversions (for example record id to record) or need expressly a conversion between types.
   *
   * @param iFieldName Field name
   * @param iFieldValue Field value to set
   * @param iType Type between the values defined in the {@link
   *     com.orientechnologies.orient.core.metadata.schema.OType} enum
   * @deprecated use {@link #setProperty(String, Object, OType...)} instead
   */
  @Deprecated
  ORecordSchemaAware field(String iFieldName, Object iFieldValue, OType... iType);

  /**
   * Removes a field. This operation does not set the field value to null but remove the field
   * itself.
   *
   * @param iFieldName Field name
   * @return The old value contained in the remove field
   * @deprecated use {@link #removeProperty(String)} instead
   */
  @Deprecated
  Object removeField(String iFieldName);

  /**
   * Tells if a field is contained in current record.
   *
   * @param iFieldName Field name
   * @return true if exists, otherwise false
   * @deprecated use {@link #hasProperty(String)} (String)} instead
   */
  @Deprecated
  boolean containsField(String iFieldName);

  /**
   * Returns the number of fields present in memory.
   *
   * @return Fields number
   * @deprecated use {@link #getPropertyNames()} instead
   */
  @Deprecated
  int fields();

  /**
   * Returns the record's field names. The returned Set object is disconnected by internal
   * representation, so changes don't apply to the record. If the fields are ordered the order is
   * maintained also in the returning collection.
   *
   * @return Set of string containing the field names
   * @deprecated use {@link #getPropertyNames()} instead
   */
  @Deprecated
  String[] fieldNames();

  /**
   * Returns the record's field values. The returned object array is disconnected by internal
   * representation, so changes don't apply to the record. If the fields are ordered the order is
   * maintained also in the returning collection.
   *
   * @return Object array of the field values
   * @deprecated use {@link #getPropertyNames()} and {@link #getLinkProperty(String)} instead
   */
  @Deprecated
  Object[] fieldValues();
}
