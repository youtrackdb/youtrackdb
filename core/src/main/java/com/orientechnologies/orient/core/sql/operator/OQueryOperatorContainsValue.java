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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * CONTAINS KEY operator.
 */
public class OQueryOperatorContainsValue extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorContainsValue() {
    super("CONTAINSVALUE", 5, false);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (!(iRight instanceof OSQLFilterCondition) && !(iLeft instanceof OSQLFilterCondition)) {
      return OIndexReuseType.INDEX_METHOD;
    }

    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal internalIndex = index.getInternal();
    Stream<ORawPair<Object, ORID>> stream;
    if (!internalIndex.canBeUsedInEqualityOperators()) {
      return null;
    }

    if (indexDefinition.getParamCount() == 1) {
      if (!((indexDefinition instanceof OPropertyMapIndexDefinition)
          && ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy()
          == OPropertyMapIndexDefinition.INDEX_BY.VALUE)) {
        return null;
      }

      final Object key =
          ((OIndexDefinitionMultiValue) indexDefinition)
              .createSingleValue(iContext.getDatabase(), keyParams.get(0));

      if (key == null) {
        return null;
      }

      stream = index.getInternal().getRids(iContext.getDatabase(), key)
          .map((rid) -> new ORawPair<>(key, rid));
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.
      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      if (!((compositeIndexDefinition.getMultiValueDefinition()
          instanceof OPropertyMapIndexDefinition)
          && ((OPropertyMapIndexDefinition) compositeIndexDefinition.getMultiValueDefinition())
          .getIndexBy()
          == OPropertyMapIndexDefinition.INDEX_BY.VALUE)) {
        return null;
      }

      final Object keyOne =
          compositeIndexDefinition.createSingleValue(iContext.getDatabase(), keyParams);

      if (keyOne == null) {
        return null;
      }

      if (internalIndex.hasRangeQuerySupport()) {
        final Object keyTwo =
            compositeIndexDefinition.createSingleValue(iContext.getDatabase(), keyParams);

        stream = index.getInternal()
            .streamEntriesBetween(iContext.getDatabase(), keyOne, true, keyTwo, true,
                ascSortOrder);
      } else {
        if (indexDefinition.getParamCount() == keyParams.size()) {
          stream = index.getInternal().getRids(iContext.getDatabase(), keyOne)
              .map((rid) -> new ORawPair<>(keyOne, rid));
        } else {
          return null;
        }
      }
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return stream;
  }

  @Override
  public ORID getBeginRidRange(ODatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(ODatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      Object iRight,
      OCommandContext iContext) {
    final OSQLFilterCondition condition;
    if (iCondition.getLeft() instanceof OSQLFilterCondition) {
      condition = (OSQLFilterCondition) iCondition.getLeft();
    } else if (iCondition.getRight() instanceof OSQLFilterCondition) {
      condition = (OSQLFilterCondition) iCondition.getRight();
    } else {
      condition = null;
    }

    OType type = null;
    if (iCondition.getLeft() instanceof OSQLFilterItemField
        && ((OSQLFilterItemField) iCondition.getLeft()).isFieldChain()
        && ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemCount() == 1) {
      String fieldName =
          ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemName(0);
      if (fieldName != null) {
        Object record = iRecord.getRecord();
        if (record instanceof ODocument) {
          OProperty property =
              ODocumentInternal.getImmutableSchemaClass(((ODocument) record))
                  .getProperty(fieldName);
          if (property != null && property.getType().isMultiValue()) {
            type = property.getLinkedType();
          }
        }
      }
    }

    if (type != null) {
      iRight = OType.convert(iContext.getDatabase(), iRight, type.getDefaultJavaType());
    }

    if (iLeft instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iLeft;

      if (condition != null) {
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          if ((Boolean) condition.evaluate((ODocument) o, null, iContext)) {
            return true;
          }
        }
      } else {
        for (Object val : map.values()) {
          Object convertedRight = iRight;
          if (val instanceof ODocument && iRight instanceof Map) {
            val = ((ODocument) val).toMap();
          }
          if (val instanceof Map && iRight instanceof ODocument) {
            convertedRight = ((ODocument) iRight).toMap();
          }
          if (OQueryOperatorEquals.equals(iContext.getDatabase(), val, convertedRight)) {
            return true;
          }
        }
        return false;
      }

    } else if (iRight instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iRight;

      if (condition != null)
      // CHECK AGAINST A CONDITION
      {
        for (Object o : map.values()) {
          if ((Boolean) condition.evaluate((ODocument) o, null, iContext)) {
            return true;
          } else {
            return map.containsValue(iLeft);
          }
        }
      }
    }
    return false;
  }
}
