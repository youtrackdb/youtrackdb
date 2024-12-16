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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.Set;

/**
 * Proxied single value index.
 */
public class IndexRemoteOneValue extends IndexRemote {

  private static final String QUERY_GET = "select rid from index:`%s` where key = ?";

  public IndexRemoteOneValue(
      final String iName,
      final String iWrappedType,
      final String algorithm,
      final RID iRid,
      final IndexDefinition iIndexDefinition,
      final EntityImpl iConfiguration,
      final Set<String> clustersToIndex,
      String database) {
    super(
        iName,
        iWrappedType,
        algorithm,
        iRid,
        iIndexDefinition,
        iConfiguration,
        clustersToIndex,
        database);
  }

  @Deprecated
  public Identifiable get(DatabaseSessionInternal session, final Object key) {
    try (ResultSet result =
        getDatabase().indexQuery(getName(), String.format(QUERY_GET, name), key)) {
      if (result != null && result.hasNext()) {
        return result.next().getProperty("rid");
      }
      return null;
    }
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }
}
