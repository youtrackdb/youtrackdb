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
import com.orientechnologies.core.index.OIndexInternal;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.ODocumentHelper;
import com.orientechnologies.core.sql.OSQLHelper;
import com.orientechnologies.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.core.sql.filter.OSQLFilterItemField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * BETWEEN operator.
 */
public class OQueryOperatorBetween extends OQueryOperatorEqualityNotNulls {

  private boolean leftInclusive = true;
  private boolean rightInclusive = true;

  public OQueryOperatorBetween() {
    super("BETWEEN", 5, false, 3);
  }

  public boolean isLeftInclusive() {
    return leftInclusive;
  }

  public void setLeftInclusive(boolean leftInclusive) {
    this.leftInclusive = leftInclusive;
  }

  public boolean isRightInclusive() {
    return rightInclusive;
  }

  public void setRightInclusive(boolean rightInclusive) {
    this.rightInclusive = rightInclusive;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition condition,
      final Object left,
      final Object right,
      OCommandContext iContext) {
    validate(right);

    final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(right);

    var database = iContext.getDatabase();
    Object right1 = valueIterator.next();
    valueIterator.next();
    Object right2 = valueIterator.next();
    final Object right1c = YTType.convert(database, right1, left.getClass());
    if (right1c == null) {
      return false;
    }

    final Object right2c = YTType.convert(database, right2, left.getClass());
    if (right2c == null) {
      return false;
    }

    final int leftResult;
    if (left instanceof Number && right1 instanceof Number) {
      Number[] conv = YTType.castComparableNumber((Number) left, (Number) right1);
      leftResult = ((Comparable) conv[0]).compareTo(conv[1]);
    } else {
      leftResult = ((Comparable<Object>) left).compareTo(right1c);
    }
    final int rightResult;
    if (left instanceof Number && right2 instanceof Number) {
      Number[] conv = YTType.castComparableNumber((Number) left, (Number) right2);
      rightResult = ((Comparable) conv[0]).compareTo(conv[1]);
    } else {
      rightResult = ((Comparable<Object>) left).compareTo(right2c);
    }

    return (leftInclusive ? leftResult >= 0 : leftResult > 0)
        && (rightInclusive ? rightResult <= 0 : rightResult < 0);
  }

  private void validate(Object iRight) {
    if (!OMultiValue.isMultiValue(iRight.getClass())) {
      throw new IllegalArgumentException(
          "Found '" + iRight + "' while was expected: " + getSyntax());
    }

    if (OMultiValue.getSize(iRight) != 3) {
      throw new IllegalArgumentException(
          "Found '" + OMultiValue.toString(iRight) + "' while was expected: " + getSyntax());
    }
  }

  @Override
  public String getSyntax() {
    return "<left> " + keyword + " <minRange> AND <maxRange>";
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    var database = iContext.getDatabase();
    Stream<ORawPair<Object, YTRID>> stream;
    final OIndexInternal internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators() || !internalIndex.hasRangeQuerySupport()) {
      return null;
    }

    if (indexDefinition.getParamCount() == 1) {
      final Object[] betweenKeys = (Object[]) keyParams.get(0);

      final Object keyOne =
          indexDefinition.createValue(
              database, Collections.singletonList(OSQLHelper.getValue(betweenKeys[0])));
      final Object keyTwo =
          indexDefinition.createValue(
              database, Collections.singletonList(OSQLHelper.getValue(betweenKeys[2])));

      if (keyOne == null || keyTwo == null) {
        return null;
      }

      stream =
          index
              .getInternal()
              .streamEntriesBetween(database, keyOne, leftInclusive, keyTwo, rightInclusive,
                  ascSortOrder);
    } else {
      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      final Object[] betweenKeys = (Object[]) keyParams.get(keyParams.size() - 1);

      final Object betweenKeyOne = OSQLHelper.getValue(betweenKeys[0]);

      if (betweenKeyOne == null) {
        return null;
      }

      final Object betweenKeyTwo = OSQLHelper.getValue(betweenKeys[2]);

      if (betweenKeyTwo == null) {
        return null;
      }

      final List<Object> betweenKeyOneParams = new ArrayList<>(keyParams.size());
      betweenKeyOneParams.addAll(keyParams.subList(0, keyParams.size() - 1));
      betweenKeyOneParams.add(betweenKeyOne);

      final List<Object> betweenKeyTwoParams = new ArrayList<>(keyParams.size());
      betweenKeyTwoParams.addAll(keyParams.subList(0, keyParams.size() - 1));
      betweenKeyTwoParams.add(betweenKeyTwo);

      final Object keyOne =
          compositeIndexDefinition.createSingleValue(database, betweenKeyOneParams);

      if (keyOne == null) {
        return null;
      }

      final Object keyTwo =
          compositeIndexDefinition.createSingleValue(database, betweenKeyTwoParams);

      if (keyTwo == null) {
        return null;
      }

      stream =
          index
              .getInternal()
              .streamEntriesBetween(database, keyOne, leftInclusive, keyTwo, rightInclusive,
                  ascSortOrder);
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return stream;
  }

  @Override
  public YTRID getBeginRidRange(YTDatabaseSession session, final Object iLeft,
      final Object iRight) {
    validate(iRight);

    if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot(session))) {
      final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

      final Object right1 = valueIterator.next();
      if (right1 != null) {
        return (YTRID) right1;
      }

      valueIterator.next();

      return (YTRID) valueIterator.next();
    }

    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, final Object iLeft, final Object iRight) {
    validate(iRight);

    validate(iRight);

    if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot(session))) {
      final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

      final Object right1 = valueIterator.next();

      valueIterator.next();

      final Object right2 = valueIterator.next();

      if (right2 == null) {
        return (YTRID) right1;
      }

      return (YTRID) right2;
    }

    return null;
  }
}
