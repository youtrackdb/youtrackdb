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
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.query.OQueryRuntimeValueMulti;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItemFieldAny;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TRAVERSE operator.
 */
public class OQueryOperatorTraverse extends OQueryOperatorEqualityNotNulls {

  private int startDeepLevel = 0; // FIRST
  private int endDeepLevel = -1; // INFINITE
  private String[] cfgFields;

  public OQueryOperatorTraverse() {
    super("TRAVERSE", 5, false, 1, true);
  }

  public OQueryOperatorTraverse(
      final int startDeepLevel, final int endDeepLevel, final String[] iFieldList) {
    this();
    this.startDeepLevel = startDeepLevel;
    this.endDeepLevel = endDeepLevel;
    this.cfgFields = iFieldList;
  }

  @Override
  public String getSyntax() {
    return "<left> TRAVERSE[(<begin-deep-level> [,<maximum-deep-level> [,<fields>]] )] ("
        + " <conditions> )";
  }

  @Override
  protected boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      final OCommandContext iContext) {
    final OSQLFilterCondition condition;
    final Object target;

    if (iCondition.getLeft() instanceof OSQLFilterCondition) {
      condition = (OSQLFilterCondition) iCondition.getLeft();
      target = iRight;
    } else {
      condition = (OSQLFilterCondition) iCondition.getRight();
      target = iLeft;
    }

    final Set<YTRID> evaluatedRecords = new HashSet<YTRID>();
    return traverse(target, condition, 0, evaluatedRecords, iContext);
  }

  @SuppressWarnings("unchecked")
  private boolean traverse(
      Object iTarget,
      final OSQLFilterCondition iCondition,
      final int iLevel,
      final Set<YTRID> iEvaluatedRecords,
      final OCommandContext iContext) {
    if (endDeepLevel > -1 && iLevel > endDeepLevel) {
      return false;
    }

    if (iTarget instanceof YTIdentifiable) {
      if (iEvaluatedRecords.contains(((YTIdentifiable) iTarget).getIdentity()))
      // ALREADY EVALUATED
      {
        return false;
      }

      // TRANSFORM THE YTRID IN ODOCUMENT
      iTarget = ((YTIdentifiable) iTarget).getRecord();
    }

    if (iTarget instanceof EntityImpl target) {

      iEvaluatedRecords.add(target.getIdentity());

      var db = iContext.getDatabase();
      if (target.isNotBound(db)) {
        try {
          target = db.bindToSession(target);
        } catch (final YTRecordNotFoundException ignore) {
          // INVALID RID
          return false;
        }
      }

      if (iLevel >= startDeepLevel && iCondition.evaluate(target, null, iContext) == Boolean.TRUE) {
        return true;
      }

      // TRAVERSE THE DOCUMENT ITSELF
      if (cfgFields != null) {
        for (final String cfgField : cfgFields) {
          if (cfgField.equalsIgnoreCase(OSQLFilterItemFieldAny.FULL_NAME)) {
            // ANY
            for (final String fieldName : target.fieldNames()) {
              if (traverse(
                  target.rawField(fieldName),
                  iCondition,
                  iLevel + 1,
                  iEvaluatedRecords,
                  iContext)) {
                return true;
              }
            }
          } else if (cfgField.equalsIgnoreCase(OSQLFilterItemFieldAny.FULL_NAME)) {
            // ALL
            for (final String fieldName : target.fieldNames()) {
              if (!traverse(
                  target.rawField(fieldName),
                  iCondition,
                  iLevel + 1,
                  iEvaluatedRecords,
                  iContext)) {
                return false;
              }
            }
            return true;
          } else {
            if (traverse(
                target.rawField(cfgField), iCondition, iLevel + 1, iEvaluatedRecords, iContext)) {
              return true;
            }
          }
        }
      }

    } else if (iTarget instanceof OQueryRuntimeValueMulti multi) {

      for (final Object o : multi.getValues()) {
        if (traverse(o, iCondition, iLevel + 1, iEvaluatedRecords, iContext) == Boolean.TRUE) {
          return true;
        }
      }
    } else if (iTarget instanceof Map<?, ?>) {

      final Map<Object, Object> map = (Map<Object, Object>) iTarget;
      for (final Object o : map.values()) {
        if (traverse(o, iCondition, iLevel + 1, iEvaluatedRecords, iContext) == Boolean.TRUE) {
          return true;
        }
      }
    } else if (OMultiValue.isMultiValue(iTarget)) {
      final Iterable<Object> collection = OMultiValue.getMultiValueIterable(iTarget);
      for (final Object o : collection) {
        if (traverse(o, iCondition, iLevel + 1, iEvaluatedRecords, iContext) == Boolean.TRUE) {
          return true;
        }
      }
    } else if (iTarget instanceof Iterator iterator) {
      while (iterator.hasNext()) {
        if (traverse(iterator.next(), iCondition, iLevel + 1, iEvaluatedRecords, iContext)
            == Boolean.TRUE) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public OQueryOperator configure(final List<String> iParams) {
    if (iParams == null) {
      return this;
    }

    final int start = !iParams.isEmpty() ? Integer.parseInt(iParams.get(0)) : startDeepLevel;
    final int end = iParams.size() > 1 ? Integer.parseInt(iParams.get(1)) : endDeepLevel;

    String[] fields = new String[]{"any()"};
    if (iParams.size() > 2) {
      String f = iParams.get(2);
      if (f.startsWith("'") || f.startsWith("\"")) {
        f = f.substring(1, f.length() - 1);
      }
      fields = f.split(",");
    }

    return new OQueryOperatorTraverse(start, end, fields);
  }

  public int getStartDeepLevel() {
    return startDeepLevel;
  }

  public int getEndDeepLevel() {
    return endDeepLevel;
  }

  public String[] getCfgFields() {
    return cfgFields;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public String toString() {
    return String.format(
        "%s(%d,%d,%s)", keyword, startDeepLevel, endDeepLevel, Arrays.toString(cfgFields));
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
