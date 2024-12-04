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
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * CONTAINS operator.
 */
public class OQueryOperatorContains extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorContains() {
    super("CONTAINS", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    var database = iContext.getDatabase();
    final OSQLFilterCondition condition;
    if (iCondition.getLeft() instanceof OSQLFilterCondition) {
      condition = (OSQLFilterCondition) iCondition.getLeft();
    } else if (iCondition.getRight() instanceof OSQLFilterCondition) {
      condition = (OSQLFilterCondition) iCondition.getRight();
    } else {
      condition = null;
    }

    if (iLeft instanceof Iterable<?>) {

      final Iterable<Object> iterable = (Iterable<Object>) iLeft;

      if (condition != null) {
        // CHECK AGAINST A CONDITION
        for (final Object o : iterable) {
          final YTIdentifiable id;
          if (o instanceof YTIdentifiable) {
            id = (YTIdentifiable) o;
          } else if (o instanceof Map<?, ?>) {
            final Iterator<Object> iter = ((Map<?, Object>) o).values().iterator();
            final Object v = iter.hasNext() ? iter.next() : null;
            if (v instanceof YTIdentifiable) {
              id = (YTIdentifiable) v;
            } else
            // TRANSFORM THE ENTIRE MAP IN A DOCUMENT. PROBABLY HAS BEEN IMPORTED FROM JSON
            {
              id = new YTDocument((Map) o);
            }

          } else if (o instanceof Iterable<?>) {
            final Iterator<YTIdentifiable> iter = ((Iterable<YTIdentifiable>) o).iterator();
            id = iter.hasNext() ? iter.next() : null;
          } else {
            continue;
          }

          if (condition.evaluate(id, null, iContext) == Boolean.TRUE) {
            return true;
          }
        }
      } else {
        // CHECK AGAINST A SINGLE VALUE
        YTType type = null;

        if (iCondition.getLeft() instanceof OSQLFilterItemField
            && ((OSQLFilterItemField) iCondition.getLeft()).isFieldChain()
            && ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemCount() == 1) {
          String fieldName =
              ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemName(0);
          if (fieldName != null) {
            Object record = iRecord.getRecord();
            if (record instanceof YTDocument) {
              YTProperty property =
                  ODocumentInternal.getImmutableSchemaClass(((YTDocument) record))
                      .getProperty(fieldName);
              if (property != null && property.getType().isMultiValue()) {
                type = property.getLinkedType();
              }
            }
          }
        }
        for (final Object o : iterable) {
          if (OQueryOperatorEquals.equals(database, iRight, o, type)) {
            return true;
          }
        }
      }
    } else if (iRight instanceof Iterable<?>) {

      // CHECK AGAINST A CONDITION
      final Iterable<YTIdentifiable> iterable = (Iterable<YTIdentifiable>) iRight;

      if (condition != null) {
        for (final YTIdentifiable o : iterable) {
          if (condition.evaluate(o, null, iContext) == Boolean.TRUE) {
            return true;
          }
        }
      } else {
        // CHECK AGAINST A SINGLE VALUE
        for (final Object o : iterable) {
          if (OQueryOperatorEquals.equals(database, iLeft, o)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (!(iLeft instanceof OSQLFilterCondition) && !(iRight instanceof OSQLFilterCondition)) {
      return OIndexReuseType.INDEX_METHOD;
    }

    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    var database = iContext.getDatabase();
    final OIndexDefinition indexDefinition = index.getDefinition();

    Stream<ORawPair<Object, YTRID>> stream;
    final OIndexInternal internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators()) {
      return null;
    }

    if (indexDefinition.getParamCount() == 1) {
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue) {
        key =
            ((OIndexDefinitionMultiValue) indexDefinition)
                .createSingleValue(database, keyParams.get(0));
      } else {
        key = indexDefinition.createValue(database, keyParams);
      }

      if (key == null) {
        return null;
      }

      stream = index.getInternal().getRids(database, key).map((rid) -> new ORawPair<>(key, rid));
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.

      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne = compositeIndexDefinition.createSingleValue(database, keyParams);

      if (keyOne == null) {
        return null;
      }

      final Object keyTwo = compositeIndexDefinition.createSingleValue(database, keyParams);
      if (internalIndex.hasRangeQuerySupport()) {
        stream = index.getInternal().streamEntriesBetween(database, keyOne, true, keyTwo, true,
            ascSortOrder);
      } else {
        int indexParamCount = indexDefinition.getParamCount();
        if (indexParamCount == keyParams.size()) {
          stream = index.getInternal().getRids(database, keyOne)
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
  public YTRID getBeginRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }
}
