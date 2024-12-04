/*
 *
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class OSQLMethodField extends OAbstractSQLMethod {

  public static final String NAME = "field";

  public OSQLMethodField() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      final OCommandContext iContext,
      Object ioResult,
      final Object[] iParams) {
    if (iParams[0] == null) {
      return null;
    }

    var db = iContext.getDatabase();
    final String paramAsString = iParams[0].toString();

    if (ioResult != null) {
      if (ioResult instanceof OResult) {
        ioResult = ((OResult) ioResult).toElement();
      }
      if (ioResult instanceof Iterable && !(ioResult instanceof YTDocument)) {
        ioResult = ((Iterable) ioResult).iterator();
      }
      if (ioResult instanceof String) {
        try {
          ioResult = new YTRecordId((String) ioResult).getRecord();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on reading rid with value '%s'", e, ioResult);
          ioResult = null;
        }
      } else if (ioResult instanceof YTIdentifiable) {
        try {
          ioResult = ((YTIdentifiable) ioResult).getRecord();
        } catch (YTRecordNotFoundException rnf) {
          OLogManager.instance()
              .error(this, "Error on reading rid with value '%s'", null, ioResult);
          ioResult = null;
        }
      } else if (ioResult instanceof Collection<?>
          || ioResult instanceof Iterator<?>
          || ioResult.getClass().isArray()) {
        final List<Object> result = new ArrayList<Object>(OMultiValue.getSize(ioResult));
        for (Object o : OMultiValue.getMultiValueIterable(ioResult)) {
          Object newlyAdded = ODocumentHelper.getFieldValue(db, o, paramAsString);
          if (OMultiValue.isMultiValue(newlyAdded)) {
            if (newlyAdded instanceof Map || newlyAdded instanceof YTIdentifiable) {
              result.add(newlyAdded);
            } else {
              for (Object item : OMultiValue.getMultiValueIterable(newlyAdded)) {
                result.add(item);
              }
            }
          } else {
            result.add(newlyAdded);
          }
        }
        return result;
      }
    }

    if (!"*".equals(paramAsString) && ioResult != null) {
      if (ioResult instanceof OCommandContext) {
        ioResult = ((OCommandContext) ioResult).getVariable(paramAsString);
      } else {
        ioResult = ODocumentHelper.getFieldValue(db, ioResult, paramAsString, iContext);
      }
    }

    return ioResult;
  }

  @Override
  public boolean evaluateParameters() {
    return false;
  }
}
