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
package com.orientechnologies.orient.core.sql.functions.text;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

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
          final YTEntityImpl doc = new YTEntityImpl().fromJSON(iThis.toString(),
              iParams[0].toString());
          if (iParams[0].toString().contains("embedded")) {
            ODocumentInternal.addOwner(doc, iCurrentRecord.getRecord());
          }

          return doc;
        } catch (YTRecordNotFoundException e) {
          return null;
        }
      }

      var doc = new YTEntityImpl();
      doc.fromJSON(iThis.toString());
      return doc;
    }

    return null;
  }
}
