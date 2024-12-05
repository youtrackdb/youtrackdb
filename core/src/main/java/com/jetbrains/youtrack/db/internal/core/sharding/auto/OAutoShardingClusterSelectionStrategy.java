/*
 * Copyright 2010-2014 YouTrackDB LTD (info(-at-)orientdb.com)
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
package com.jetbrains.youtrack.db.internal.core.sharding.auto;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.exception.OInvalidIndexEngineIdException;
import com.jetbrains.youtrack.db.internal.core.exception.YTConfigurationException;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexInternal;
import com.jetbrains.youtrack.db.internal.core.index.engine.OIndexEngine;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.List;

/**
 * Returns the cluster selecting through the hash function.
 *
 * @since 3.0
 */
public class OAutoShardingClusterSelectionStrategy implements OClusterSelectionStrategy {

  public static final String NAME = "auto-sharding";
  private final OIndex index;
  private final OIndexEngine indexEngine;
  private final List<String> indexedFields;
  private final int[] clusters;

  public OAutoShardingClusterSelectionStrategy(final YTClass clazz,
      final OIndex autoShardingIndex) {
    index = autoShardingIndex;
    if (index == null) {
      throw new YTConfigurationException(
          "Cannot use auto-sharding cluster strategy because class '"
              + clazz
              + "' has no auto-sharding index defined");
    }

    indexedFields = index.getDefinition().getFields();
    if (indexedFields.size() != 1) {
      throw new YTConfigurationException(
          "Cannot use auto-sharding cluster strategy because class '"
              + clazz
              + "' has an auto-sharding index defined with multiple fields");
    }

    final Storage stg = ODatabaseRecordThreadLocal.instance().get().getStorage();
    if (!(stg instanceof AbstractPaginatedStorage)) {
      throw new YTConfigurationException(
          "Cannot use auto-sharding cluster strategy because storage is not embedded");
    }

    try {
      indexEngine =
          (OIndexEngine)
              ((AbstractPaginatedStorage) stg)
                  .getIndexEngine(((OIndexInternal) index).getIndexId());
    } catch (OInvalidIndexEngineIdException e) {
      throw YTException.wrapException(
          new YTConfigurationException(
              "Cannot use auto-sharding cluster strategy because the underlying index has not"
                  + " found"),
          e);
    }

    if (indexEngine == null) {
      throw new YTConfigurationException(
          "Cannot use auto-sharding cluster strategy because the underlying index has not found");
    }

    clusters = clazz.getClusterIds();
  }

  public int getCluster(final YTClass iClass, int[] clusters, final EntityImpl doc) {
    // Ignore the subselection.
    return getCluster(iClass, doc);
  }

  public int getCluster(final YTClass clazz, final EntityImpl doc) {
    final Object fieldValue = doc.field(indexedFields.get(0));

    return clusters[
        ((OAutoShardingIndexEngine) indexEngine)
            .getStrategy()
            .getPartitionsId(fieldValue, clusters.length)];
  }

  @Override
  public String getName() {
    return NAME;
  }
}
