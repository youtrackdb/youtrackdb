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
package com.orientechnologies.orient.core.sql.method.sequence;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence;
import com.orientechnologies.orient.core.sql.YTCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

/**
 * Returns the current number of a sequence.
 */
public class OSQLMethodCurrent extends OAbstractSQLMethod {

  public static final String NAME = "current";

  public OSQLMethodCurrent() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "current()";
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis == null) {
      throw new YTCommandSQLParsingException(
          "Method 'current()' can be invoked only on OSequence instances, while NULL was found");
    }

    if (!(iThis instanceof YTSequence)) {
      throw new YTCommandSQLParsingException(
          "Method 'current()' can be invoked only on OSequence instances, while '"
              + iThis.getClass()
              + "' was found");
    }

    try {
      return ((YTSequence) iThis).current();
    } catch (YTDatabaseException exc) {
      String message = "Unable to execute command: " + exc.getMessage();
      OLogManager.instance().error(this, message, exc, (Object) null);
      throw new YTCommandExecutionException(message);
    }
  }
}
