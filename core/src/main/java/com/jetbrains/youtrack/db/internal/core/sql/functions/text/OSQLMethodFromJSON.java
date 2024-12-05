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
package com.jetbrains.youtrack.db.internal.core.sql.functions.text;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OAbstractSQLMethod;

/**
 * Converts a document in JSON string.
 */
public class OSQLMethodFromJSON extends OAbstractSQLMethod {

  public static final String NAME = "fromjson";

  public OSQLMethodFromJSON() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "fromJSON([<options>])";
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis instanceof String) {
      if (iParams.length > 0) {
        try {
          final EntityImpl doc = new EntityImpl().fromJSON(iThis.toString(),
              iParams[0].toString());
          if (iParams[0].toString().contains("embedded")) {
            ODocumentInternal.addOwner(doc, iCurrentRecord.getRecord());
          }

          return doc;
        } catch (YTRecordNotFoundException e) {
          return null;
        }
      }

      var doc = new EntityImpl();
      doc.fromJSON(iThis.toString());
      return doc;
    }

    return null;
  }
}
