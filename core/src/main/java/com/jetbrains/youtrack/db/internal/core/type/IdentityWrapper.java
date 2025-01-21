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

import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base abstract class to wrap a entity.
 */
@SuppressWarnings("unchecked")
public class IdentityWrapper implements Serializable {

  private final ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>();
  private final AtomicBoolean dirty = new AtomicBoolean(false);

  private final RID rid;
  private final AtomicInteger version = new AtomicInteger();

  public IdentityWrapper(DatabaseSessionInternal sessionInternal, final String iClassName) {
    var entity = sessionInternal.newEntity(iClassName);

    rid = entity.getIdentity();
    version.set(entity.getVersion());
  }

  public IdentityWrapper(DatabaseSessionInternal db, EntityImpl entity) {
    rid = entity.getIdentity();
    version.set(entity.getVersion());

    var map = entity.toMap();
    for (var entry : map.entrySet()) {
      var propName = entry.getKey();
      data.put(propName, deserializeProperty(db, propName, entry.getValue()));
    }
  }

  protected Object deserializeProperty(DatabaseSessionInternal db, String propertyName,
      Object value) {
    return value;
  }

  public RID getIdentity() {
    return rid;
  }

  protected <T> T getProperty(String name) {
    return (T) data.get(name);
  }

  protected <T> void setProperty(String name, T value) {
    dirty.set(true);
    data.put(name, value);
  }

  public void save(DatabaseSessionInternal db) {
    if (dirty.getAndSet(false)) {
      var entity = db.loadEntity(rid);
      checkVersion(entity, RecordOperation.UPDATED);

      entity.fromMap(data);
      version.incrementAndGet();
    }
  }

  public void delete(DatabaseSessionInternal db) {
    var entity = db.loadEntity(rid);
    checkVersion(entity, RecordOperation.DELETED);

    db.delete(entity);
    data.clear();
    dirty.set(false);
  }

  private void checkVersion(Entity entity, byte operation) {
    var rememberedVersion = version.get();
    if (entity.getVersion() != rememberedVersion) {
      throw new ConcurrentModificationException(rid, entity.getVersion(), rememberedVersion,
          operation);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IdentityWrapper that = (IdentityWrapper) o;
    return data.equals(that.data) && rid.equals(that.rid);
  }

  @Override
  public int hashCode() {
    int result = data.hashCode();
    result = 31 * result + rid.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " {" +
        "rid=" + rid +
        ", data=" + data +
        '}';
  }
}
