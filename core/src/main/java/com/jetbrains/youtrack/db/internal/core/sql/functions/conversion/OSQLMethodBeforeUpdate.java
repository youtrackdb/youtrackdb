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
package com.jetbrains.youtrack.db.internal.core.sql.functions.conversion;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.LiveQueryListenerImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OAbstractSQLMethod;

/**
 * ONLY FOR LIVE QUERY. Returns the value of current record (as an YTResult) before it was updated.
 * Null if the record is new <br> eg. on update, get only records whose "name" attribute was update
 * <code> db.live("select from Person where @this.beforeUpdate().name != name
 * </code>
 */
public class OSQLMethodBeforeUpdate extends OAbstractSQLMethod {

  public static final String NAME = "beforeUpdate";

  public OSQLMethodBeforeUpdate() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "beforeUpdate()";
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis instanceof YTResult) {
      return ((YTResult) iThis).getMetadata(LiveQueryListenerImpl.BEFORE_METADATA_KEY);
    }
    return null;
  }
}
