/*
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
 */

package com.jetbrains.youtrack.db.api.record;

/**
 * Record id interface that represents a record id in database. record id is made of 2 numbers:
 * cluster id (cluster number) and cluster position (absolute position inside the cluster). Loading
 * a record by its record id allows O(1) performance, no matter the database size.
 */
public interface RID extends Identifiable {
  char PREFIX = '#';
  char SEPARATOR = ':';
  int CLUSTER_MAX = 32767;
  int CLUSTER_ID_INVALID = -1;
  long CLUSTER_POS_INVALID = -1;

  int getClusterId();

  long getClusterPosition();

  boolean isPersistent();

  boolean isNew();

  boolean isTemporary();
}
