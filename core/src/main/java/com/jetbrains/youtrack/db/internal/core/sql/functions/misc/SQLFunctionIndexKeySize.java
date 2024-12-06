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

import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionAbstract;
import java.util.stream.Stream;

/**
 * returns the number of keys for an index
 */
public class SQLFunctionIndexKeySize extends SQLFunctionAbstract {

  public static final String NAME = "indexKeySize";

  public SQLFunctionIndexKeySize() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final Identifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      CommandContext context) {
    final Object value = iParams[0];

    String indexName = String.valueOf(value);
    final DatabaseSessionInternal database = context.getDatabase();
    Index index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      return null;
    }
    try (Stream<RawPair<Object, RID>> stream = index.getInternal()
        .stream(context.getDatabase())) {
      try (Stream<RID> rids = index.getInternal().getRids(context.getDatabase(), null)) {
        return stream.map((pair) -> pair.first).distinct().count() + rids.count();
      }
    }
  }

  public String getSyntax(DatabaseSession session) {
    return "indexKeySize(<indexName-string>)";
  }
}
