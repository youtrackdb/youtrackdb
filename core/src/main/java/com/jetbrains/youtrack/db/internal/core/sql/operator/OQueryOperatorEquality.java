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

import com.jetbrains.youtrack.db.internal.core.collate.OCollate;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.query.OQueryRuntimeValueMulti;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.OBinaryField;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItemFieldAll;

/**
 * Base equality operator. It's an abstract class able to compare the equality between two values.
 */
public abstract class OQueryOperatorEquality extends OQueryOperator {

  protected OQueryOperatorEquality(
      final String iKeyword, final int iPrecedence, final boolean iLogical) {
    super(iKeyword, iPrecedence, false);
  }

  protected OQueryOperatorEquality(
      final String iKeyword,
      final int iPrecedence,
      final boolean iLogical,
      final int iExpectedRightWords) {
    super(iKeyword, iPrecedence, false, iExpectedRightWords);
  }

  protected OQueryOperatorEquality(
      final String iKeyword,
      final int iPrecedence,
      final boolean iLogical,
      final int iExpectedRightWords,
      final boolean iExpectsParameters) {
    super(iKeyword, iPrecedence, iLogical, iExpectedRightWords, iExpectsParameters);
  }

  protected abstract boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      CommandContext iContext);

  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      final CommandContext iContext,
      final ODocumentSerializer serializer) {
    final Object left = serializer.deserializeValue(iContext.getDatabase(), iFirstField.bytes,
        iFirstField.type, null);
    final Object right = serializer.deserializeValue(iContext.getDatabase(), iSecondField.bytes,
        iFirstField.type, null);

    return evaluateExpression(null, null, left, right, iContext);
  }

  @Override
  public Object evaluateRecord(
      final YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      CommandContext iContext,
      final ODocumentSerializer serializer) {

    if (iLeft instanceof OBinaryField && iRight instanceof OBinaryField)
    // BINARY COMPARISON
    {
      return evaluate((OBinaryField) iLeft, (OBinaryField) iRight, iContext, serializer);
    } else if (iLeft instanceof OQueryRuntimeValueMulti left) {
      // LEFT = MULTI

      if (left.getValues().length == 0) {
        return false;
      }

      if (left.getDefinition().getRoot(iContext.getDatabase())
          .startsWith(OSQLFilterItemFieldAll.NAME)) {
        // ALL VALUES
        for (int i = 0; i < left.getValues().length; ++i) {
          Object v = left.getValues()[i];
          Object r = iRight;

          final OCollate collate = left.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            r = collate.transform(iRight);
          }

          if (v == null || !evaluateExpression(iRecord, iCondition, v, r, iContext)) {
            return false;
          }
        }
        return true;
      } else {
        // ANY VALUES
        for (int i = 0; i < left.getValues().length; ++i) {
          Object v = left.getValues()[i];
          Object r = iRight;

          final OCollate collate = left.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            r = collate.transform(iRight);
          }

          if (v != null && evaluateExpression(iRecord, iCondition, v, r, iContext)) {
            return true;
          }
        }
        return false;
      }

    } else if (iRight instanceof OQueryRuntimeValueMulti right) {
      // RIGHT = MULTI

      if (right.getValues().length == 0) {
        return false;
      }

      if (right.getDefinition().getRoot(iContext.getDatabase())
          .startsWith(OSQLFilterItemFieldAll.NAME)) {
        // ALL VALUES
        for (int i = 0; i < right.getValues().length; ++i) {
          Object v = right.getValues()[i];
          Object l = iLeft;

          final OCollate collate = right.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            l = collate.transform(iLeft);
          }

          if (v == null || !evaluateExpression(iRecord, iCondition, l, v, iContext)) {
            return false;
          }
        }
        return true;
      } else {
        // ANY VALUES
        for (int i = 0; i < right.getValues().length; ++i) {
          Object v = right.getValues()[i];
          Object l = iLeft;

          final OCollate collate = right.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            l = collate.transform(iLeft);
          }

          if (v != null && evaluateExpression(iRecord, iCondition, l, v, iContext)) {
            return true;
          }
        }
        return false;
      }
    } else {
      // SINGLE SIMPLE ITEM
      return evaluateExpression(iRecord, iCondition, iLeft, iRight, iContext);
    }
  }
}
