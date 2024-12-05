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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Proxied index.
 */
public class OIndexRemoteMultiValue extends OIndexRemote {

  private static final String QUERY_GET = "select expand( rid ) from index:`%s` where key = ?";

  public OIndexRemoteMultiValue(
      final String iName,
      final String iWrappedType,
      final String algorithm,
      final YTRID iRid,
      final OIndexDefinition iIndexDefinition,
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
  public Collection<YTIdentifiable> get(YTDatabaseSessionInternal session, final Object key) {
    try (YTResultSet result =
        getDatabase().indexQuery(getName(), String.format(QUERY_GET, name), key)) {
      //noinspection resource
      return result.stream()
          .map((res) -> res.getIdentity().orElse(null))
          .collect(Collectors.toSet());
    }
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }
}
