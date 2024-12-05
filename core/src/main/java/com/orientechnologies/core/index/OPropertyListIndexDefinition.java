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
package com.orientechnologies.core.index;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Index implementation bound to one schema class property that presents
 * {@link YTType#EMBEDDEDLIST},
 * {@link YTType#LINKLIST},
 * {@link YTType#LINKSET} or
 * {@link YTType#EMBEDDEDSET} properties.
 */
public class OPropertyListIndexDefinition extends OPropertyIndexDefinition
    implements OIndexDefinitionMultiValue {

  @SuppressWarnings("unused")
  public OPropertyListIndexDefinition() {
  }

  public OPropertyListIndexDefinition(
      final String iClassName, final String iField, final YTType iType) {
    super(iClassName, iField, iType);
  }

  @Override
  public Object getDocumentValueToIndex(YTDatabaseSessionInternal session, YTEntityImpl iDocument) {
    return createValue(session, iDocument.<Object>field(field));
  }

  @Override
  public Object createValue(YTDatabaseSessionInternal session, List<?> params) {
    if (!(params.get(0) instanceof Collection)) {
      params = Collections.singletonList(params);
    }

    final Collection<?> multiValueCollection = (Collection<?>) params.get(0);
    final List<Object> values = new ArrayList<>(multiValueCollection.size());
    for (final Object item : multiValueCollection) {
      values.add(createSingleValue(session, item));
    }
    return values;
  }

  @Override
  public Object createValue(YTDatabaseSessionInternal session, final Object... params) {
    Object param = params[0];
    if (!(param instanceof Collection<?>)) {
      try {
        return YTType.convert(session, param, keyType.getDefaultJavaType());
      } catch (Exception e) {
        return null;
      }
    }

    @SuppressWarnings("PatternVariableCanBeUsed")
    var multiValueCollection = (Collection<?>) param;
    final List<Object> values = new ArrayList<>(multiValueCollection.size());
    for (final Object item : multiValueCollection) {
      values.add(createSingleValue(session, item));
    }
    return values;
  }

  public Object createSingleValue(YTDatabaseSessionInternal session, final Object... param) {
    try {
      var value = refreshRid(session, param[0]);
      return YTType.convert(session, value, keyType.getDefaultJavaType());
    } catch (Exception e) {
      throw YTException.wrapException(
          new YTIndexException(
              "Invalid key for index: " + param[0] + " cannot be converted to " + keyType),
          e);
    }
  }

  public void processChangeEvent(
      YTDatabaseSessionInternal session,
      final OMultiValueChangeEvent<?, ?> changeEvent,
      final Object2IntMap<Object> keysToAdd,
      final Object2IntMap<Object> keysToRemove) {
    switch (changeEvent.getChangeType()) {
      case ADD: {
        processAdd(createSingleValue(session, changeEvent.getValue()), keysToAdd, keysToRemove);
        break;
      }
      case REMOVE: {
        processRemoval(
            createSingleValue(session, changeEvent.getOldValue()), keysToAdd, keysToRemove);
        break;
      }
      case UPDATE: {
        processRemoval(
            createSingleValue(session, changeEvent.getOldValue()), keysToAdd, keysToRemove);
        processAdd(createSingleValue(session, changeEvent.getValue()), keysToAdd, keysToRemove);
        break;
      }
      default:
        throw new IllegalArgumentException("Invalid change type : " + changeEvent.getChangeType());
    }
  }

  @Override
  public String toCreateIndexDDL(String indexName, String indexType, String engine) {
    return createIndexDDLWithoutFieldType(indexName, indexType, engine).toString();
  }
}
