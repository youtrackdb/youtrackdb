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

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record;

import com.jetbrains.youtrack.db.internal.core.OOrientListenerAbstract;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.record.Record;

/**
 * Thread local to store last document to save. This is used by Auto Merge Conflict Strategy o get
 * he pending record (not visible at storage level).
 */
public class ORecordSaveThreadLocal extends ThreadLocal<Record> {

  public static ORecordSaveThreadLocal INSTANCE = new ORecordSaveThreadLocal();

  static {
    YouTrackDBManager.instance()
        .registerListener(
            new OOrientListenerAbstract() {
              @Override
              public void onStartup() {
                if (INSTANCE == null) {
                  INSTANCE = new ORecordSaveThreadLocal();
                }
              }

              @Override
              public void onShutdown() {
                INSTANCE = null;
              }
            });
  }

  public static Record getLast() {
    return INSTANCE.get();
  }

  public static void setLast(final Record document) {
    INSTANCE.set(document);
  }

  public static void removeLast() {
    INSTANCE.set(null);
  }
}
