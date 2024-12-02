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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Filter the content by excluding only some fields. If the content is a document, then creates a
 * copy without the excluded fields. If it's a collection of documents it acts against on each
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
 * exclude(&lt;field|value|expression&gt; [,&lt;field-name&gt;]* )
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
 * SELECT <b>exclude(roles, 'permissions')</b> FROM OUser
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 */
public class OSQLMethodExclude extends OAbstractSQLMethod {

  public static final String NAME = "exclude";

  public OSQLMethodExclude() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: exclude([<field-name>][,]*)";
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    var db = iContext.getDatabase();
    if (iThis != null) {
      if (iThis instanceof ORecordId) {
        try {
          iThis = ((ORecordId) iThis).getRecord();
        } catch (ORecordNotFoundException rnf) {
          return null;
        }
      } else {
        if (iThis instanceof OResult result) {
          iThis = result.asElement();
        }
      }
      if (iThis instanceof ODocument) {
        // ACT ON SINGLE DOCUMENT
        return copy(db, (ODocument) iThis, iParams);
      } else {
        if (iThis instanceof Map) {
          // ACT ON SINGLE MAP
          return copy(db, (Map) iThis, iParams);
        } else {
          if (OMultiValue.isMultiValue(iThis)) {
            // ACT ON MULTIPLE DOCUMENTS
            final List<Object> result = new ArrayList<Object>(OMultiValue.getSize(iThis));
            for (Object o : OMultiValue.getMultiValueIterable(iThis)) {
              if (o instanceof OIdentifiable) {
                try {
                  var rec = ((OIdentifiable) o).getRecord();
                  result.add(copy(db, (ODocument) rec, iParams));
                } catch (ORecordNotFoundException rnf) {
                  // IGNORE IT
                }
              }
            }
            return result;
          }
        }
      }
    }

    // INVALID, RETURN NULL
    return null;
  }

  private static Object copy(ODatabaseSessionInternal db, final ODocument document,
      final Object[] iFieldNames) {
    var result = new OResultInternal(db);

    var propertyNames = new HashSet<>(document.getPropertyNames());
    for (Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();
        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);

          for (String propertyName : document.getPropertyNames()) {
            if (propertyName.startsWith(fieldPart)) {
              propertyNames.remove(propertyName);
            }
          }
        } else {
          propertyNames.remove(fieldName);
        }
      }
    }

    for (String propertyName : propertyNames) {
      result.setProperty(propertyName, document.getProperty(propertyName));
    }

    return result;
  }

  private OResult copy(ODatabaseSessionInternal database, final Map<String, ?> map,
      final Object[] iFieldNames) {
    var result = new OResultInternal(database);

    var propertyNames = new HashSet<>(map.keySet());

    for (Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();
        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);

          for (String propertyName : map.keySet()) {
            if (propertyName.startsWith(fieldPart)) {
              propertyNames.remove(propertyName);
            }
          }
        } else {
          propertyNames.remove(fieldName);
        }
      }
    }

    for (String propertyName : propertyNames) {
      result.setProperty(propertyName, map.get(propertyName));
    }

    return result;
  }
}
