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
package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaView;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 * Listener Interface to receive callbacks on database usage.
 */
public interface DatabaseLifecycleListener {

  enum PRIORITY {
    FIRST,
    EARLY,
    REGULAR,
    LATE,
    LAST
  }

  default PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  void onCreate(DatabaseSessionInternal iDatabase);

  void onOpen(DatabaseSessionInternal iDatabase);

  void onClose(DatabaseSessionInternal iDatabase);

  void onDrop(DatabaseSessionInternal iDatabase);

  @Deprecated
  default void onCreateClass(DatabaseSessionInternal iDatabase, SchemaClass iClass) {
  }

  @Deprecated
  default void onDropClass(DatabaseSessionInternal iDatabase, SchemaClass iClass) {
  }

  default void onCreateView(DatabaseSessionInternal database, SchemaView view) {
  }

  default void onDropView(DatabaseSessionInternal database, SchemaView cls) {
  }

  /**
   * Event called during the retrieving of distributed configuration, usually at startup and when
   * the cluster shape changes. You can use this event to enrich the EntityImpl sent to the client
   * with custom properties.
   *
   * @param iConfiguration
   */
  void onLocalNodeConfigurationRequest(EntityImpl iConfiguration);
}
