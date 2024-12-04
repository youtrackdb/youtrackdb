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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;
import java.util.List;
import java.util.stream.Stream;

/**
 * MAJOR EQUALS operator.
 */
public class OQueryOperatorMajorEquals extends OQueryOperatorEqualityNotNulls {

  private boolean binaryEvaluate = false;

  public OQueryOperatorMajorEquals() {
    super(">=", 5, false);
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      binaryEvaluate = db.getSerializer().getSupportBinaryEvaluate();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    final Object right = YTType.convert(iContext.getDatabase(), iRight, iLeft.getClass());
    if (right == null) {
      return false;
    }
    return ((Comparable<Object>) iLeft).compareTo(right) >= 0;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iRight == null || iLeft == null) {
      return OIndexReuseType.NO_INDEX;
    }
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    Stream<ORawPair<Object, YTRID>> stream;
    final OIndexInternal internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators() || !internalIndex.hasRangeQuerySupport()) {
      return null;
    }

    if (indexDefinition.getParamCount() == 1) {
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue) {
        key =
            ((OIndexDefinitionMultiValue) indexDefinition)
                .createSingleValue(iContext.getDatabase(), keyParams.get(0));
      } else {
        key = indexDefinition.createValue(iContext.getDatabase(), keyParams);
      }

      if (key == null) {
        return null;
      }

      stream = index.getInternal()
          .streamEntriesMajor(iContext.getDatabase(), key, true, ascSortOrder);
    } else {
      // if we have situation like "field1 = 1 AND field2 >= 2"
      // then we fetch collection which left included boundary is the smallest composite key in the
      // index that contains keys with values field1=1 and field2=2 and which right included
      // boundary
      // is the biggest composite key in the index that contains key with value field1=1.

      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne =
          compositeIndexDefinition.createSingleValue(iContext.getDatabase(), keyParams);

      if (keyOne == null) {
        return null;
      }

      final Object keyTwo =
          compositeIndexDefinition.createSingleValue(
              iContext.getDatabase(), keyParams.subList(0, keyParams.size() - 1));

      if (keyTwo == null) {
        return null;
      }

      stream = index.getInternal()
          .streamEntriesBetween(iContext.getDatabase(), keyOne, true, keyTwo, true, ascSortOrder);
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return stream;
  }

  @Override
  public YTRID getBeginRidRange(YTDatabaseSession session, final Object iLeft,
      final Object iRight) {
    if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot(session))) {
      if (iRight instanceof YTRID) {
        return (YTRID) iRight;
      } else {
        if (iRight instanceof OSQLFilterItemParameter
            && ((OSQLFilterItemParameter) iRight).getValue(null, null, null) instanceof YTRID) {
          return (YTRID) ((OSQLFilterItemParameter) iRight).getValue(null, null, null);
        }
      }
    }

    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    return serializer.getComparator().compare(iFirstField, iSecondField) >= 0;
  }

  @Override
  public boolean isSupportingBinaryEvaluate() {
    return binaryEvaluate;
  }
}
