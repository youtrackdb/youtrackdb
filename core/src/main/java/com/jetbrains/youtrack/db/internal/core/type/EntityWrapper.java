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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.Serializable;

/**
 * Base abstract class to wrap a entity.
 */
@SuppressWarnings("unchecked")
public class EntityWrapper implements Serializable {

  private EntityImpl entity;

  public EntityWrapper() {
  }

  public EntityWrapper(final String iClassName) {
    this(new EntityImpl(iClassName));
  }

  public EntityWrapper(final EntityImpl entity) {
    this.entity = entity;
  }

  public void fromStream(DatabaseSessionInternal session, final EntityImpl entity) {
    this.entity = entity;
  }

  public EntityImpl toStream(DatabaseSession session) {
    return getDocument(session);
  }

  public <RET extends EntityWrapper> RET save(DatabaseSessionInternal session) {
    entity.save();
    return (RET) this;
  }

  public <RET extends EntityWrapper> RET save(final String iClusterName) {
    entity.save(iClusterName);
    return (RET) this;
  }

  public EntityImpl getDocument(DatabaseSession session) {
    if (entity != null && entity.isNotBound(session)) {
      entity = session.bindToSession(entity);
    }

    return entity;
  }

  public void setDocument(DatabaseSessionInternal session, EntityImpl entity) {
    if (entity != null && entity.isNotBound(session)) {
      entity = session.bindToSession(entity);
    }

    this.entity = entity;
  }

  public RID getIdentity() {
    return entity.getIdentity();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((entity == null) ? 0 : entity.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final EntityWrapper other = (EntityWrapper) obj;
    if (entity == null) {
      return other.entity == null;
    } else {
      return entity.equals(other.entity);
    }
  }

  @Override
  public String toString() {
    return entity != null ? entity.toString() : "?";
  }
}
