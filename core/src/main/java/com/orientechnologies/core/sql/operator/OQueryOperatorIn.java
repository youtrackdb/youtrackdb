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
package com.orientechnologies.core.sql.operator;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OCompositeIndexDefinition;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexDefinition;
import com.orientechnologies.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.core.index.OIndexInternal;
import com.orientechnologies.core.record.impl.ODocumentHelper;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.OSQLHelper;
import com.orientechnologies.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.core.sql.filter.OSQLFilterItemParameter;
import com.orientechnologies.core.sql.query.OLegacyResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * IN operator.
 */
public class OQueryOperatorIn extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorIn() {
    super("IN", 5, false);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal internalIndex = index.getInternal();
    Stream<ORawPair<Object, YTRID>> stream;
    if (!internalIndex.canBeUsedInEqualityOperators()) {
      return null;
    }

    if (indexDefinition.getParamCount() == 1) {
      final Object inKeyValue = keyParams.get(0);
      Collection<Object> inParams;
      if (inKeyValue instanceof List<?>) {
        inParams = (Collection<Object>) inKeyValue;
      } else if (inKeyValue instanceof OSQLFilterItem) {
        inParams =
            (Collection<Object>) ((OSQLFilterItem) inKeyValue).getValue(null, null, iContext);
      } else {
        inParams = Collections.singleton(inKeyValue);
      }

      if (inParams instanceof OLegacyResultSet) { // manage IN (subquery)
        Set newInParams = new HashSet();
        for (Object o : inParams) {
          if (o instanceof YTEntityImpl doc && doc.getIdentity().getClusterId() < -1) {
            String[] fieldNames = doc.fieldNames();
            if (fieldNames.length == 1) {
              newInParams.add(doc.field(fieldNames[0]));
            } else {
              newInParams.add(o);
            }
          } else {
            newInParams.add(o);
          }
        }
        inParams = newInParams;
      }
      final List<Object> inKeys = new ArrayList<Object>();

      boolean containsNotCompatibleKey = false;
      for (final Object keyValue : inParams) {
        final Object key;
        if (indexDefinition instanceof OIndexDefinitionMultiValue) {
          key =
              ((OIndexDefinitionMultiValue) indexDefinition)
                  .createSingleValue(iContext.getDatabase(), OSQLHelper.getValue(keyValue));
        } else {
          key = indexDefinition.createValue(iContext.getDatabase(), OSQLHelper.getValue(keyValue));
        }

        if (key == null) {
          containsNotCompatibleKey = true;
          break;
        }

        inKeys.add(key);
      }
      if (containsNotCompatibleKey) {
        return null;
      }

      stream = index.getInternal().streamEntries(iContext.getDatabase(), inKeys, ascSortOrder);
    } else {
      final List<Object> partialKey = new ArrayList<Object>();
      partialKey.addAll(keyParams);
      partialKey.remove(keyParams.size() - 1);

      final Object inKeyValue = keyParams.get(keyParams.size() - 1);

      final Collection<Object> inParams;
      if (inKeyValue instanceof List<?>) {
        inParams = (Collection<Object>) inKeyValue;
      } else if (inKeyValue instanceof OSQLFilterItem) {
        inParams =
            (Collection<Object>) ((OSQLFilterItem) inKeyValue).getValue(null, null, iContext);
      } else {
        throw new IllegalArgumentException("Key '" + inKeyValue + "' is not valid");
      }

      final List<Object> inKeys = new ArrayList<Object>();

      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      boolean containsNotCompatibleKey = false;
      for (final Object keyValue : inParams) {
        List<Object> fullKey = new ArrayList<Object>();
        fullKey.addAll(partialKey);
        fullKey.add(keyValue);
        final Object key =
            compositeIndexDefinition.createSingleValue(iContext.getDatabase(), fullKey);
        if (key == null) {
          containsNotCompatibleKey = true;
          break;
        }

        inKeys.add(key);
      }
      if (containsNotCompatibleKey) {
        return null;
      }

      if (indexDefinition.getParamCount() == keyParams.size()) {
        stream = index.getInternal().streamEntries(iContext.getDatabase(), inKeys, ascSortOrder);
      } else {
        return null;
      }
    }

    updateProfiler(iContext, internalIndex, keyParams, indexDefinition);
    return stream;
  }

  @Override
  public YTRID getBeginRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    final Iterable<?> ridCollection;
    final int ridSize;
    if (iRight instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot(session))) {
      if (iLeft instanceof OSQLFilterItem) {
        iLeft = ((OSQLFilterItem) iLeft).getValue(null, null, null);
      }

      ridCollection = OMultiValue.getMultiValueIterable(iLeft);
      ridSize = OMultiValue.getSize(iLeft);
    } else if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot(session))) {
      if (iRight instanceof OSQLFilterItem) {
        iRight = ((OSQLFilterItem) iRight).getValue(null, null, null);
      }
      ridCollection = OMultiValue.getMultiValueIterable(iRight);
      ridSize = OMultiValue.getSize(iRight);
    } else {
      return null;
    }

    final List<YTRID> rids = addRangeResults(ridCollection, ridSize);

    return rids == null ? null : Collections.min(rids);
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    final Iterable<?> ridCollection;
    final int ridSize;
    if (iRight instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot(session))) {
      if (iLeft instanceof OSQLFilterItem) {
        iLeft = ((OSQLFilterItem) iLeft).getValue(null, null, null);
      }

      ridCollection = OMultiValue.getMultiValueIterable(iLeft);
      ridSize = OMultiValue.getSize(iLeft);
    } else if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot(session))) {
      if (iRight instanceof OSQLFilterItem) {
        iRight = ((OSQLFilterItem) iRight).getValue(null, null, null);
      }

      ridCollection = OMultiValue.getMultiValueIterable(iRight);
      ridSize = OMultiValue.getSize(iRight);
    } else {
      return null;
    }

    final List<YTRID> rids = addRangeResults(ridCollection, ridSize);

    return rids == null ? null : Collections.max(rids);
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
    if (OMultiValue.isMultiValue(iLeft)) {
      if (iRight instanceof Collection<?>) {
        // AGAINST COLLECTION OF ITEMS
        final Collection<Object> collectionToMatch = (Collection<Object>) iRight;

        boolean found = false;
        for (final Object o1 : OMultiValue.getMultiValueIterable(iLeft)) {
          for (final Object o2 : collectionToMatch) {
            if (OQueryOperatorEquals.equals(database, o1, o2)) {
              found = true;
              break;
            }
          }
        }
        return found;
      } else {
        // AGAINST SINGLE ITEM
        if (iLeft instanceof Set<?>) {
          return ((Set) iLeft).contains(iRight);
        }

        for (final Object o : OMultiValue.getMultiValueIterable(iLeft)) {
          if (OQueryOperatorEquals.equals(database, iRight, o)) {
            return true;
          }
        }
      }
    } else if (OMultiValue.isMultiValue(iRight)) {

      if (iRight instanceof Set<?>) {
        return ((Set) iRight).contains(iLeft);
      }

      for (final Object o : OMultiValue.getMultiValueIterable(iRight)) {
        if (OQueryOperatorEquals.equals(database, iLeft, o)) {
          return true;
        }
      }
    } else if (iLeft.getClass().isArray()) {

      for (final Object o : (Object[]) iLeft) {
        if (OQueryOperatorEquals.equals(database, iRight, o)) {
          return true;
        }
      }
    } else if (iRight.getClass().isArray()) {

      for (final Object o : (Object[]) iRight) {
        if (OQueryOperatorEquals.equals(database, iLeft, o)) {
          return true;
        }
      }
    }

    return iLeft.equals(iRight);
  }

  protected List<YTRID> addRangeResults(final Iterable<?> ridCollection, final int ridSize) {
    if (ridCollection == null) {
      return null;
    }

    List<YTRID> rids = null;
    for (Object rid : ridCollection) {
      if (rid instanceof OSQLFilterItemParameter) {
        rid = ((OSQLFilterItemParameter) rid).getValue(null, null, null);
      }

      if (rid instanceof YTIdentifiable) {
        final YTRID r = ((YTIdentifiable) rid).getIdentity();
        if (r.isPersistent()) {
          if (rids == null)
          // LAZY CREATE IT
          {
            rids = new ArrayList<YTRID>(ridSize);
          }
          rids.add(r);
        }
      }
    }
    return rids;
  }
}
