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
package com.jetbrains.youtrack.db.internal.core.sql.filter;

import com.jetbrains.youtrack.db.internal.common.parser.OBaseParser;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;

/**
 * Represent one or more object fields as value in the query condition.
 */
public class OSQLFilterItemFieldAny extends OSQLFilterItemFieldMultiAbstract {

  public static final String NAME = "ANY";
  public static final String FULL_NAME = "ANY()";

  public OSQLFilterItemFieldAny(
      YTDatabaseSessionInternal session, final OSQLPredicate iQueryCompiled, final String iName,
      final YTClass iClass) {
    super(session, iQueryCompiled, iName, iClass, OStringSerializerHelper.getParameters(iName));
  }

  @Override
  public String getRoot(YTDatabaseSession session) {
    return FULL_NAME;
  }

  @Override
  protected void setRoot(YTDatabaseSessionInternal session, final OBaseParser iQueryToParse,
      final String iRoot) {
  }
}
