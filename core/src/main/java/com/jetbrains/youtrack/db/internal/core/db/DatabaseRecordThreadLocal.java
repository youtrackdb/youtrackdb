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

import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBListenerAbstract;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatabaseRecordThreadLocal extends ThreadLocal<DatabaseSessionInternal> {

  private static final AtomicReference<DatabaseRecordThreadLocal> INSTANCE =
      new AtomicReference<>();

  @Nullable
  public static DatabaseRecordThreadLocal getInstanceIfDefined() {
    return INSTANCE.get();
  }

  @Nonnull
  public static DatabaseRecordThreadLocal instance() {
    final var dbInst = INSTANCE.get();
    if (dbInst != null) {
      return dbInst;
    }
    registerCleanUpHandler();
    return INSTANCE.get();
  }

  private static void registerCleanUpHandler() {
    // we can do that to avoid thread local memory leaks in containers
    if (INSTANCE.get() == null) {
      final var inst = YouTrackDBEnginesManager.instance();
      if (inst == null) {
        throw new DatabaseException("YouTrackDB API is not active.");
      }
      inst.registerListener(
          new YouTrackDBListenerAbstract() {
            @Override
            public void onStartup() {
            }

            @Override
            public void onShutdown() {
              INSTANCE.set(null);
            }
          });

      INSTANCE.compareAndSet(null, new DatabaseRecordThreadLocal());
    }
  }

  @Override
  public DatabaseSessionInternal get() {
    var db = super.get();
    if (db == null) {
      if (YouTrackDBEnginesManager.instance().getDatabaseThreadFactory() == null) {
        throw new DatabaseException(
            "The database instance is not set in the current thread. Be sure to set it with:"
                + " DatabaseRecordThreadLocal.instance().set(db);");
      } else {
        db = YouTrackDBEnginesManager.instance().getDatabaseThreadFactory().getThreadDatabase();
        if (db == null) {
          throw new DatabaseException(
              "The database instance is not set in the current thread. Be sure to set it with:"
                  + " DatabaseRecordThreadLocal.instance().set(db);");
        } else {
          set(db);
        }
      }
    }
    return db;
  }

  @Override
  public void remove() {
    super.remove();
  }

  @Override
  public void set(final DatabaseSessionInternal value) {
    super.set(value);
  }

  public DatabaseSessionInternal getIfDefined() {
    return super.get();
  }

  public boolean isDefined() {
    return super.get() != null;
  }
}
