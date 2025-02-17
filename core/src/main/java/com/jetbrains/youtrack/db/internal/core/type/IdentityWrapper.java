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
package com.jetbrains.youtrack.db.internal.core.type;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import javax.annotation.Nonnull;

/**
 * Base abstract class to wrap a entity.
 */
public abstract class IdentityWrapper implements Identifiable {

  private final @Nonnull RID rid;

  public IdentityWrapper(DatabaseSessionInternal sessionInternal, final String iClassName) {
    var entity = sessionInternal.newEntity(iClassName);
    rid = entity.getIdentity();
  }

  public IdentityWrapper(EntityImpl entity) {
    rid = entity.getIdentity();
  }

  protected abstract void toEntity(@Nonnull DatabaseSessionInternal db, @Nonnull EntityImpl entity);

  @Nonnull
  @Override
  public <T extends DBRecord> T getRecord(@Nonnull DatabaseSession session) {
    return rid.getRecord(session);
  }

  @Override
  public int compareTo(@Nonnull Identifiable o) {
    return 0;
  }

  @Override
  public int compare(Identifiable o1, Identifiable o2) {
    return 0;
  }

  @Nonnull
  public RID getIdentity() {
    return rid;
  }

  public void save(DatabaseSessionInternal db) {
    var entity = db.loadEntity(rid);
    toEntity(db, (EntityImpl) entity);
  }

  public void delete(DatabaseSessionInternal db) {
    try {
      var entity = db.loadEntity(rid);
      db.delete(entity);
    } catch (RecordNotFoundException e) {
      // Ignore
    }
  }


  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (IdentityWrapper) o;
    return rid.equals(that.rid);
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }
}
