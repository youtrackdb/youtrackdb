/*
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
package com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Returns the cluster selecting by round robin algorithm.
 */
public class RoundRobinClusterSelectionStrategy implements ClusterSelectionStrategy {

  public static final String NAME = "round-robin";
  private final AtomicLong pointer = new AtomicLong(0);

  public int getCluster(final SchemaClass iClass, final EntityImpl doc) {
    return getCluster(iClass, iClass.getClusterIds(), doc);
  }

  public int getCluster(final SchemaClass clazz, final int[] clusters, final EntityImpl doc) {
    if (clusters.length == 1)
    // ONLY ONE: RETURN THE FIRST ONE
    {
      return clusters[0];
    }

    return clusters[(int) (pointer.getAndIncrement() % clusters.length)];
  }

  @Override
  public String getName() {
    return NAME;
  }
}
