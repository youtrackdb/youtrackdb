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

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;

/**
 * Base equality operator that not admit NULL in the LEFT and in the RIGHT operands. Abstract
 * class.
 */
public abstract class OQueryOperatorEqualityNotNulls extends OQueryOperatorEquality {

  protected OQueryOperatorEqualityNotNulls(
      final String iKeyword, final int iPrecedence, final boolean iLogical) {
    super(iKeyword, iPrecedence, iLogical);
  }

  protected OQueryOperatorEqualityNotNulls(
      final String iKeyword,
      final int iPrecedence,
      final boolean iLogical,
      final int iExpectedRightWords) {
    super(iKeyword, iPrecedence, iLogical, iExpectedRightWords);
  }

  protected OQueryOperatorEqualityNotNulls(
      final String iKeyword,
      final int iPrecedence,
      final boolean iUnary,
      final int iExpectedRightWords,
      final boolean iExpectsParameters) {
    super(iKeyword, iPrecedence, iUnary, iExpectedRightWords, iExpectsParameters);
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
    if (iLeft == null || iRight == null) {
      return false;
    }

    return super.evaluateRecord(
        iRecord, iCurrentResult, iCondition, iLeft, iRight, iContext, serializer);
  }
}
