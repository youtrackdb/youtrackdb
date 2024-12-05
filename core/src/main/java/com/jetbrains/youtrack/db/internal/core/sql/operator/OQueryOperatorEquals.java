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
package com.jetbrains.youtrack.db.internal.core.sql.operator;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OCompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinitionMultiValue;
import com.jetbrains.youtrack.db.internal.core.index.OIndexInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.OBinaryField;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItemField;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItemParameter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * EQUALS operator.
 */
public class OQueryOperatorEquals extends OQueryOperatorEqualityNotNulls {

  private boolean binaryEvaluate = false;

  public OQueryOperatorEquals() {
    super("=", 5, false);
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      binaryEvaluate = db.getSerializer().getSupportBinaryEvaluate();
    }
  }

  public static boolean equals(YTDatabaseSession session, final Object iLeft, final Object iRight,
      YTType type) {
    if (type == null) {
      return equals(session, iLeft, iRight);
    }
    Object left = YTType.convert(session, iLeft, type.getDefaultJavaType());
    Object right = YTType.convert(session, iRight, type.getDefaultJavaType());
    return equals(session, left, right);
  }

  public static boolean equals(@Nullable YTDatabaseSession session, Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null) {
      return false;
    }

    if (iLeft == iRight) {
      return true;
    }

    // RECORD & YTRID
    /*from this is only legacy query engine */
    if (iLeft instanceof Record) {
      return comparesValues(iRight, (Record) iLeft, true);
    } else if (iRight instanceof Record) {
      return comparesValues(iLeft, (Record) iRight, true);
    }
    /*till this is only legacy query engine */
    else if (iRight instanceof YTResult) {
      return comparesValues(iLeft, (YTResult) iRight, true);
    } else if (iRight instanceof YTResult) {
      return comparesValues(iLeft, (YTResult) iRight, true);
    }

    // NUMBERS
    if (iLeft instanceof Number && iRight instanceof Number) {
      Number[] couple = YTType.castComparableNumber((Number) iLeft, (Number) iRight);
      return couple[0].equals(couple[1]);
    }

    // ALL OTHER CASES
    try {
      final Object right = YTType.convert(session, iRight, iLeft.getClass());

      if (right == null) {
        return false;
      }
      if (iLeft instanceof byte[] && iRight instanceof byte[]) {
        return Arrays.equals((byte[]) iLeft, (byte[]) iRight);
      }
      return iLeft.equals(right);
    } catch (Exception ignore) {
      return false;
    }
  }

  protected static boolean comparesValues(
      final Object iValue, final Record iRecord, final boolean iConsiderIn) {
    // YTRID && RECORD
    final YTRID other = iRecord.getIdentity();

    if (!other.isPersistent() && iRecord instanceof EntityImpl) {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final Set<String> firstFieldName = ((EntityImpl) iRecord).getPropertyNames();
      if (firstFieldName.size() > 0) {
        Object fieldValue = ((EntityImpl) iRecord).getProperty(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue)) {
              if (o != null && o.equals(iValue)) {
                return true;
              }
            }
          }

          return fieldValue.equals(iValue);
        }
      }
      return false;
    }
    return other.equals(iValue);
  }

  protected static boolean comparesValues(
      final Object iValue, final YTResult iRecord, final boolean iConsiderIn) {
    if (iRecord.getIdentity().isPresent() && iRecord.getIdentity().get().isPersistent()) {
      return iRecord.getIdentity().get().equals(iValue);
    } else {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      var firstFieldName = iRecord.getPropertyNames();
      if (firstFieldName.size() == 1) {
        Object fieldValue = iRecord.getProperty(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue)) {
              if (o != null && o.equals(iValue)) {
                return true;
              }
            }
          }

          return fieldValue.equals(iValue);
        }
      }

      return false;
    }
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iLeft instanceof YTIdentifiable && iRight instanceof YTIdentifiable) {
      return OIndexReuseType.NO_INDEX;
    }
    if (iRight == null || iLeft == null) {
      return OIndexReuseType.NO_INDEX;
    }

    return OIndexReuseType.INDEX_METHOD;
  }

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

      stream = index.getInternal().getRids(iContext.getDatabase(), key)
          .map((rid) -> new ORawPair<>(key, rid));
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.

      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne =
          compositeIndexDefinition.createSingleValue(iContext.getDatabase(), keyParams);

      if (keyOne == null) {
        return null;
      }

      final Object keyTwo =
          compositeIndexDefinition.createSingleValue(iContext.getDatabase(), keyParams);

      if (internalIndex.hasRangeQuerySupport()) {
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

    if (iRight instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot(session))) {
      if (iLeft instanceof YTRID) {
        return (YTRID) iLeft;
      } else {
        if (iLeft instanceof OSQLFilterItemParameter
            && ((OSQLFilterItemParameter) iLeft).getValue(null, null, null) instanceof YTRID) {
          return (YTRID) ((OSQLFilterItemParameter) iLeft).getValue(null, null, null);
        }
      }
    }

    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, final Object iLeft, final Object iRight) {
    return getBeginRidRange(session, iLeft, iRight);
  }

  @Override
  protected boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    return equals(iContext.getDatabase(), iLeft, iRight);
  }

  @Override
  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    return serializer.getComparator().isEqual(iFirstField, iSecondField);
  }

  @Override
  public boolean isSupportingBinaryEvaluate() {
    return binaryEvaluate;
  }
}
