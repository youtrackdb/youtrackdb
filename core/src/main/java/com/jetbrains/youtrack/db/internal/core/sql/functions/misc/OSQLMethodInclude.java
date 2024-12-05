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
package com.jetbrains.youtrack.db.internal.core.sql.functions.misc;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OAbstractSQLMethod;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Filter the content by including only some fields. If the content is a document, then creates a
 * copy with only the included fields. If it's a collection of documents it acts against on each
 * single entry.
 *
 * <p>
 *
 * <p>Syntax:
 *
 * <blockquote>
 *
 * <p>
 *
 * <pre>
 * include(&lt;field|value|expression&gt; [,&lt;field-name&gt;]* )
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 *
 * <p>
 *
 * <p>Examples:
 *
 * <blockquote>
 *
 * <p>
 *
 * <pre>
 * SELECT <b>include(roles, 'name')</b> FROM OUser
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 */
public class OSQLMethodInclude extends OAbstractSQLMethod {

  public static final String NAME = "include";

  public OSQLMethodInclude() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: include([<field-name>][,]*)";
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {

    if (iParams[0] != null) {
      if (iThis instanceof YTIdentifiable) {
        try {
          iThis = ((YTIdentifiable) iThis).getRecord();
        } catch (YTRecordNotFoundException rnf) {
          return null;
        }
      } else if (iThis instanceof YTResult result) {
        iThis = result.asEntity();
      }
      if (iThis instanceof EntityImpl) {
        // ACT ON SINGLE DOCUMENT
        return copy((EntityImpl) iThis, iParams);
      } else if (iThis instanceof Map) {
        // ACT ON MAP
        return copy((Map) iThis, iParams);
      } else if (OMultiValue.isMultiValue(iThis)) {
        // ACT ON MULTIPLE DOCUMENTS
        final List<Object> result = new ArrayList<Object>(OMultiValue.getSize(iThis));
        for (Object o : OMultiValue.getMultiValueIterable(iThis)) {
          if (o instanceof YTIdentifiable) {
            try {
              var record = ((YTIdentifiable) o).getRecord();
              result.add(copy((EntityImpl) record, iParams));
            } catch (YTRecordNotFoundException rnf) {
              // IGNORE IT
            }
          }
        }
        return result;
      }
    }

    // INVALID, RETURN NULL
    return null;
  }

  private Object copy(final EntityImpl document, final Object[] iFieldNames) {
    final EntityImpl doc = new EntityImpl();
    for (int i = 0; i < iFieldNames.length; ++i) {
      if (iFieldNames[i] != null) {

        final String fieldName = iFieldNames[i].toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toInclude = new ArrayList<String>();
          for (String f : document.fieldNames()) {
            if (f.startsWith(fieldPart)) {
              toInclude.add(f);
            }
          }

          for (String f : toInclude) {
            doc.field(fieldName, document.<Object>field(f));
          }

        } else {
          doc.field(fieldName, document.<Object>field(fieldName));
        }
      }
    }
    return doc;
  }

  private Object copy(final Map map, final Object[] iFieldNames) {
    final EntityImpl doc = new EntityImpl();
    for (int i = 0; i < iFieldNames.length; ++i) {
      if (iFieldNames[i] != null) {
        final String fieldName = iFieldNames[i].toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toInclude = new ArrayList<String>();
          for (Object f : map.keySet()) {
            if (f.toString().startsWith(fieldPart)) {
              toInclude.add(f.toString());
            }
          }

          for (String f : toInclude) {
            doc.field(fieldName, map.get(f));
          }

        } else {
          doc.field(fieldName, map.get(fieldName));
        }
      }
    }
    return doc;
  }
}
