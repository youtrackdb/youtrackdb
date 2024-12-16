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

import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
      CompositeCollate collate = new CompositeCollate(this);
      if (collatesList != null) {
        for (Collate oCollate : collatesList) {
          collate.addCollate(oCollate);
        }
      } else {
        final int typesSize = keyTypes.length;
        final Collate defCollate = SQLEngine.getCollate(DefaultCollate.NAME);
        for (int i = 0; i < typesSize; i++) {
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

    final CompositeKey compositeKey = new CompositeKey();

    for (int i = 0; i < params.length; ++i) {
      final Comparable<?> paramValue =
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

  @Override
  public @Nonnull EntityImpl toStream(@Nonnull EntityImpl entity) {
    serializeToStream(entity);
    return entity;
  }

  @Override
  protected void serializeToStream(EntityImpl entity) {
    super.serializeToStream(entity);

    final List<String> keyTypeNames = new ArrayList<>(keyTypes.length);

    for (final PropertyType keyType : keyTypes) {
      keyTypeNames.add(keyType.toString());
    }

    entity.field("keyTypes", keyTypeNames, PropertyType.EMBEDDEDLIST);
    if (collate instanceof CompositeCollate) {
      List<String> collatesNames = new ArrayList<>();
      for (Collate curCollate : ((CompositeCollate) this.collate).getCollates()) {
        collatesNames.add(curCollate.getName());
      }
      entity.field("collates", collatesNames, PropertyType.EMBEDDEDLIST);
    } else {
      entity.field("collate", collate.getName());
    }

    entity.field("nullValuesIgnored", isNullValuesIgnored());
  }

  @Override
  public void fromStream(@Nonnull EntityImpl entity) {
    serializeFromStream(entity);
  }

  @Override
  protected void serializeFromStream(EntityImpl entity) {
    super.serializeFromStream(entity);

    final List<String> keyTypeNames = entity.field("keyTypes");
    keyTypes = new PropertyType[keyTypeNames.size()];

    int i = 0;
    for (final String keyTypeName : keyTypeNames) {
      keyTypes[i] = PropertyType.valueOf(keyTypeName);
      i++;
    }
    String collate = entity.field("collate");
    if (collate != null) {
      setCollate(collate);
    } else {
      final List<String> collatesNames = entity.field("collates");
      if (collatesNames != null) {
        CompositeCollate collates = new CompositeCollate(this);
        for (String collateName : collatesNames) {
          collates.addCollate(SQLEngine.getCollate(collateName));
        }
        this.collate = collates;
      }
    }

    setNullValuesIgnored(!Boolean.FALSE.equals(entity.<Boolean>field("nullValuesIgnored")));
  }

  public Object getDocumentValueToIndex(
      DatabaseSessionInternal session, final EntityImpl entity) {
    throw new IndexException("This method is not supported in given index definition.");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SimpleKeyIndexDefinition that = (SimpleKeyIndexDefinition) o;
    return Arrays.equals(keyTypes, that.keyTypes);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
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
    final StringBuilder ddl = new StringBuilder("create index `");
    ddl.append(indexName).append("` ").append(indexType).append(' ');

    if (keyTypes != null && keyTypes.length > 0) {
      ddl.append(keyTypes[0].toString());
      for (int i = 1; i < keyTypes.length; i++) {
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
