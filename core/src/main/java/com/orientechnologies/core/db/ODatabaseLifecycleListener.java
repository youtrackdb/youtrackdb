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
package com.orientechnologies.core.db;

import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTView;
import com.orientechnologies.core.record.impl.YTEntityImpl;

/**
 * Listener Interface to receive callbacks on database usage.
 */
public interface ODatabaseLifecycleListener {

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

  void onCreate(YTDatabaseSessionInternal iDatabase);

  void onOpen(YTDatabaseSessionInternal iDatabase);

  void onClose(YTDatabaseSessionInternal iDatabase);

  void onDrop(YTDatabaseSessionInternal iDatabase);

  @Deprecated
  default void onCreateClass(YTDatabaseSessionInternal iDatabase, YTClass iClass) {
  }

  @Deprecated
  default void onDropClass(YTDatabaseSessionInternal iDatabase, YTClass iClass) {
  }

  default void onCreateView(YTDatabaseSessionInternal database, YTView view) {
  }

  default void onDropView(YTDatabaseSessionInternal database, YTView cls) {
  }

  /**
   * Event called during the retrieving of distributed configuration, usually at startup and when
   * the cluster shape changes. You can use this event to enrich the YTEntityImpl sent to the client
   * with custom properties.
   *
   * @param iConfiguration
   */
  void onLocalNodeConfigurationRequest(YTEntityImpl iConfiguration);
}
