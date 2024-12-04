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
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Keeps items only once removing duplicates
 */
public class OSQLFunctionDistinct extends OSQLFunctionAbstract {

  public static final String NAME = "distinct";

  private final Set<Object> context = new LinkedHashSet<Object>();

  public OSQLFunctionDistinct() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    final Object value = iParams[0];

    if (value != null && !context.contains(value)) {
      context.add(value);
      return value;
    }

    return null;
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  public String getSyntax(YTDatabaseSession session) {
    return "distinct(<field>)";
  }
}
