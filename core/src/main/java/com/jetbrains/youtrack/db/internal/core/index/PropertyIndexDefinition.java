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

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLCreateIndex;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Index implementation bound to one schema class property.
 */
public class PropertyIndexDefinition extends AbstractIndexDefinition {

  protected String className;
  protected String field;
  protected PropertyType keyType;

  public PropertyIndexDefinition(final String iClassName, final String iField,
      final PropertyType iType) {
    super();
    className = iClassName;
    field = iField;
    keyType = iType;
  }

  /**
   * Constructor used for index unmarshalling.
   */
  public PropertyIndexDefinition() {
  }

  public String getClassName() {
    return className;
  }

  public List<String> getFields() {
    return Collections.singletonList(field);
  }

  public List<String> getFieldsToIndex() {
    if (collate == null || collate.getName().equals(DefaultCollate.NAME)) {
      return Collections.singletonList(field);
    }

    return Collections.singletonList(field + " collate " + collate.getName());
  }

  public Object getDocumentValueToIndex(
      DatabaseSessionInternal session, final EntityImpl entity) {
    if (PropertyType.LINK.equals(keyType)) {
      final Identifiable identifiable = entity.field(field);
      if (identifiable != null) {
        return createValue(session, identifiable.getIdentity());
      } else {
        return null;
      }
    }
    return createValue(session, entity.<Object>field(field));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    final PropertyIndexDefinition that = (PropertyIndexDefinition) o;

    if (!className.equals(that.className)) {
      return false;
    }
    if (!field.equals(that.field)) {
      return false;
    }
    return keyType == that.keyType;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + className.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + keyType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "PropertyIndexDefinition{"
        + "className='"
        + className
        + '\''
        + ", field='"
        + field
        + '\''
        + ", keyType="
        + keyType
        + ", collate="
        + collate
        + ", null values ignored = "
        + isNullValuesIgnored()
        + '}';
  }

  public Object createValue(DatabaseSessionInternal session, final List<?> params) {
    return PropertyType.convert(session, params.get(0), keyType.getDefaultJavaType());
  }

  /**
   * {@inheritDoc}
   */
  public Object createValue(DatabaseSessionInternal session, final Object... params) {
    return PropertyType.convert(session, refreshRid(session, params[0]),
        keyType.getDefaultJavaType());
  }

  public int getParamCount() {
    return 1;
  }

  public PropertyType[] getTypes() {
    return new PropertyType[]{keyType};
  }

  public void fromStream(@Nonnull EntityImpl entity) {
    serializeFromStream(entity);
  }

  @Override
  public final @Nonnull EntityImpl toStream(DatabaseSessionInternal db,
      @Nonnull EntityImpl entity) {
    serializeToStream(db, entity);
    return entity;
  }

  protected void serializeToStream(DatabaseSessionInternal db, EntityImpl entity) {
    super.serializeToStream(db, entity);

    entity.setPropertyInternal("className", className);
    entity.setPropertyInternal("field", field);
    entity.setPropertyInternal("keyType", keyType.toString());
    entity.setPropertyInternal("collate", collate.getName());
    entity.setPropertyInternal("nullValuesIgnored", isNullValuesIgnored());
  }

  protected void serializeFromStream(EntityImpl entity) {
    super.serializeFromStream(entity);

    className = entity.field("className");
    field = entity.field("field");

    final String keyTypeStr = entity.field("keyType");
    keyType = PropertyType.valueOf(keyTypeStr);

    setCollate((String) entity.field("collate"));
    setNullValuesIgnored(!Boolean.FALSE.equals(entity.<Boolean>field("nullValuesIgnored")));
  }

  /**
   * {@inheritDoc}
   *
   * @param indexName
   * @param indexType
   */
  public String toCreateIndexDDL(
      final String indexName, final String indexType, final String engine) {
    return createIndexDDLWithFieldType(indexName, indexType, engine).toString();
  }

  protected StringBuilder createIndexDDLWithFieldType(
      String indexName, String indexType, String engine) {
    final StringBuilder ddl = createIndexDDLWithoutFieldType(indexName, indexType, engine);
    ddl.append(' ').append(keyType.name());
    return ddl;
  }

  protected StringBuilder createIndexDDLWithoutFieldType(
      final String indexName, final String indexType, final String engine) {
    final StringBuilder ddl = new StringBuilder("create index `");

    ddl.append(indexName).append("` on `");
    ddl.append(className).append("` ( `").append(field).append("`");

    if (!collate.getName().equals(DefaultCollate.NAME)) {
      ddl.append(" collate ").append(collate.getName());
    }

    ddl.append(" ) ");
    ddl.append(indexType);

    if (engine != null) {
      ddl.append(' ').append(CommandExecutorSQLCreateIndex.KEYWORD_ENGINE + " ").append(engine);
    }
    return ddl;
  }

  protected void processAdd(
      final Object value,
      final Object2IntMap<Object> keysToAdd,
      final Object2IntMap<Object> keysToRemove) {
    if (value == null) {
      return;
    }

    final int removeCount = keysToRemove.getInt(value);
    if (removeCount > 0) {
      int newRemoveCount = removeCount - 1;
      if (newRemoveCount > 0) {
        keysToRemove.put(value, newRemoveCount);
      } else {
        keysToRemove.removeInt(value);
      }
    } else {
      final int addCount = keysToAdd.getInt(value);
      if (addCount > 0) {
        keysToAdd.put(value, addCount + 1);
      } else {
        keysToAdd.put(value, 1);
      }
    }
  }

  protected void processRemoval(
      final Object value,
      final Object2IntMap<Object> keysToAdd,
      final Object2IntMap<Object> keysToRemove) {
    if (value == null) {
      return;
    }

    final int addCount = keysToAdd.getInt(value);
    if (addCount > 0) {
      int newAddCount = addCount - 1;
      if (newAddCount > 0) {
        keysToAdd.put(value, newAddCount);
      } else {
        keysToAdd.removeInt(value);
      }
    } else {
      final int removeCount = keysToRemove.getInt(value);
      if (removeCount > 0) {
        keysToRemove.put(value, removeCount + 1);
      } else {
        keysToRemove.put(value, 1);
      }
    }
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }
}
