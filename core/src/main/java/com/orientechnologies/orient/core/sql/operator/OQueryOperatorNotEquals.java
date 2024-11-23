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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * NOT EQUALS operator.
 */
public class OQueryOperatorNotEquals extends OQueryOperatorEqualityNotNulls {

  private boolean binaryEvaluate = false;

  public OQueryOperatorNotEquals() {
    super("<>", 5, false);
    ODatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      binaryEvaluate = db.getSerializer().getSupportBinaryEvaluate();
    }
  }

  @Override
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    return !OQueryOperatorEquals.equals(iContext.getDatabase(), iLeft, iRight);
  }

  @Override
  public boolean isSupportingBinaryEvaluate() {
    return binaryEvaluate;
  }

  @Override
  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    return !serializer.getComparator().isEqual(iFirstField, iSecondField);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public ORID getBeginRidRange(ODatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(ODatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }
}
