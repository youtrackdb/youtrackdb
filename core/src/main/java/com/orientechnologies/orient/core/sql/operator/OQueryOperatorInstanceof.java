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
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * EQUALS operator.
 */
public class OQueryOperatorInstanceof extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorInstanceof() {
    super("INSTANCEOF", 5, false);
  }

  @Override
  protected boolean evaluateExpression(
      final YTIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {

    final YTSchema schema =
        ODatabaseRecordThreadLocal.instance().get().getMetadata().getImmutableSchemaSnapshot();

    final String baseClassName = iRight.toString();
    final YTClass baseClass = schema.getClass(baseClassName);
    if (baseClass == null) {
      throw new OCommandExecutionException(
          "Class '" + baseClassName + "' is not defined in database schema");
    }

    YTClass cls = null;
    if (iLeft instanceof YTIdentifiable) {
      // GET THE RECORD'S CLASS
      final YTRecord record = ((YTIdentifiable) iLeft).getRecord();
      if (record instanceof YTDocument) {
        cls = ODocumentInternal.getImmutableSchemaClass(((YTDocument) record));
      }
    } else if (iLeft instanceof String)
    // GET THE CLASS BY NAME
    {
      cls = schema.getClass((String) iLeft);
    }

    return cls != null && cls.isSubClassOf(baseClass);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.NO_INDEX;
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
