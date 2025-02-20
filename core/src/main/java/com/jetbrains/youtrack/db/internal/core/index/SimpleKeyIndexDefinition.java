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
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class SimpleKeyIndexDefinition extends AbstractIndexDefinition {

  private PropertyType[] keyTypes;

  public SimpleKeyIndexDefinition(final PropertyType... keyTypes) {
    super();

    this.keyTypes = keyTypes;
  }

  public SimpleKeyIndexDefinition() {
  }

  public SimpleKeyIndexDefinition(PropertyType[] keyTypes2, List<Collate> collatesList) {
    super();

    this.keyTypes = Arrays.copyOf(keyTypes2, keyTypes2.length);

    if (keyTypes.length > 1) {
      var collate = new CompositeCollate(this);
      if (collatesList != null) {
        for (var oCollate : collatesList) {
          collate.addCollate(oCollate);
        }
      } else {
        final var typesSize = keyTypes.length;
        final var defCollate = SQLEngine.getCollate(DefaultCollate.NAME);
        for (var i = 0; i < typesSize; i++) {
          collate.addCollate(defCollate);
        }
      }
      this.collate = collate;
    }
  }

  public List<String> getFields() {
    return Collections.emptyList();
  }

  public List<String> getFieldsToIndex() {
    return Collections.emptyList();
  }

  public String getClassName() {
    return null;
  }

  public Object createValue(DatabaseSessionInternal session, final List<?> params) {
    return createValue(session, params != null ? params.toArray() : null);
  }

  public Object createValue(DatabaseSessionInternal session, final Object... params) {
    if (params == null || params.length == 0) {
      return null;
    }

    if (keyTypes.length == 1) {
      return PropertyType.convert(session, refreshRid(session, params[0]),
          keyTypes[0].getDefaultJavaType());
    }

    final var compositeKey = new CompositeKey();

    for (var i = 0; i < params.length; ++i) {
      final var paramValue =
          (Comparable<?>)
              PropertyType.convert(session, refreshRid(session, params[i]),
                  keyTypes[i].getDefaultJavaType());

      if (paramValue == null) {
        return null;
      }
      compositeKey.addKey(paramValue);
    }

    return compositeKey;
  }

  public int getParamCount() {
    return keyTypes.length;
  }

  public PropertyType[] getTypes() {
    return Arrays.copyOf(keyTypes, keyTypes.length);
  }

  @Nonnull
  @Override
  public Map<String, Object> toMap(DatabaseSessionInternal session) {
    var map = new HashMap<String, Object>();
    serializeToMap(map, session);
    return map;
  }

  @Override
  public void toJson(@Nonnull JsonGenerator jsonGenerator) {
    try {
      jsonGenerator.writeStartObject();

      jsonGenerator.writeArrayFieldStart("keyTypes");
      for (var keyType : keyTypes) {
        jsonGenerator.writeString(keyType.toString());
      }
      jsonGenerator.writeEndArray();

      if (collate instanceof CompositeCollate) {
        jsonGenerator.writeArrayFieldStart("collates");
        for (var curCollate : ((CompositeCollate) this.collate).getCollates()) {
          jsonGenerator.writeString(curCollate.getName());
        }
        jsonGenerator.writeEndArray();
      } else {
        jsonGenerator.writeStringField("collate", collate.getName());
      }

      jsonGenerator.writeBooleanField("nullValuesIgnored", isNullValuesIgnored());
    } catch (IOException e) {
      throw BaseException.wrapException(
          new SerializationException("Error serializing index defenition"), e, (String) null);
    }


  }

  @Override
  protected void serializeToMap(@Nonnull Map<String, Object> map, DatabaseSessionInternal session) {
    final List<String> keyTypeNames = new ArrayList<>(keyTypes.length);

    for (final var keyType : keyTypes) {
      keyTypeNames.add(keyType.toString());
    }

    map.put("keyTypes", keyTypeNames);
    if (collate instanceof CompositeCollate) {
      List<String> collatesNames = new ArrayList<>();
      for (var curCollate : ((CompositeCollate) this.collate).getCollates()) {
        collatesNames.add(curCollate.getName());
      }
      map.put("collates", collatesNames);
    } else {
      map.put("collate", collate.getName());
    }

    map.put("nullValuesIgnored", isNullValuesIgnored());
  }

  @Override
  public void fromMap(@Nonnull Map<String, ?> map) {
    serializeFromMap(map);
  }

  @Override
  protected void serializeFromMap(@Nonnull Map<String, ?> map) {
    super.serializeFromMap(map);

    @SuppressWarnings("unchecked") final var keyTypeNames = (List<String>) map.get("keyTypes");
    keyTypes = new PropertyType[keyTypeNames.size()];

    var i = 0;
    for (final var keyTypeName : keyTypeNames) {
      keyTypes[i] = PropertyType.valueOf(keyTypeName);
      i++;
    }

    var collate = (String) map.get("collate");
    if (collate != null) {
      setCollate(collate);
    } else {
      @SuppressWarnings("unchecked") final var collatesNames = (List<String>) map.get("collates");
      if (collatesNames != null) {
        var collates = new CompositeCollate(this);
        for (var collateName : collatesNames) {
          collates.addCollate(SQLEngine.getCollate(collateName));
        }
        this.collate = collates;
      }
    }

    setNullValuesIgnored(!Boolean.FALSE.equals(map.get("nullValuesIgnored")));
  }

  public Object getDocumentValueToIndex(
      DatabaseSessionInternal session, final EntityImpl entity) {
    throw new IndexException(session.getDatabaseName(),
        "This method is not supported in given index definition.");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final var that = (SimpleKeyIndexDefinition) o;
    return Arrays.equals(keyTypes, that.keyTypes);
  }

  @Override
  public int hashCode() {
    var result = super.hashCode();
    result = 31 * result + (keyTypes != null ? Arrays.hashCode(keyTypes) : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SimpleKeyIndexDefinition{"
        + "keyTypes="
        + (keyTypes == null ? null : Arrays.asList(keyTypes))
        + '}';
  }

  /**
   * {@inheritDoc}
   *
   * @param indexName
   * @param indexType
   */
  public String toCreateIndexDDL(
      final String indexName, final String indexType, final String engine) {
    final var ddl = new StringBuilder("create index `");
    ddl.append(indexName).append("` ").append(indexType).append(' ');

    if (keyTypes != null && keyTypes.length > 0) {
      ddl.append(keyTypes[0].toString());
      for (var i = 1; i < keyTypes.length; i++) {
        ddl.append(", ").append(keyTypes[i].toString());
      }
    }
    return ddl.toString();
  }

  @Override
  public boolean isAutomatic() {
    return false;
  }
}
