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
package com.orientechnologies.core.sql.functions.misc;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.sql.functions.OSQLFunctionAbstract;
import java.util.stream.Stream;

/**
 * returns the number of keys for an index
 */
public class OSQLFunctionIndexKeySize extends OSQLFunctionAbstract {

  public static final String NAME = "indexKeySize";

  public OSQLFunctionIndexKeySize() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      OCommandContext context) {
    final Object value = iParams[0];

    String indexName = String.valueOf(value);
    final YTDatabaseSessionInternal database = context.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      return null;
    }
    try (Stream<ORawPair<Object, YTRID>> stream = index.getInternal()
        .stream(context.getDatabase())) {
      try (Stream<YTRID> rids = index.getInternal().getRids(context.getDatabase(), null)) {
        return stream.map((pair) -> pair.first).distinct().count() + rids.count();
      }
    }
  }

  public String getSyntax(YTDatabaseSession session) {
    return "indexKeySize(<indexName-string>)";
  }
}
