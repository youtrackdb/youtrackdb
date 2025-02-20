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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Abstract index definition implementation.
 */
public abstract class AbstractIndexDefinition implements IndexDefinition {

  protected Collate collate = new DefaultCollate();
  private boolean nullValuesIgnored = true;

  protected AbstractIndexDefinition() {
  }

  public Collate getCollate() {
    return collate;
  }

  public void setCollate(final Collate collate) {
    if (collate == null) {
      throw new IllegalArgumentException("COLLATE cannot be null");
    }
    this.collate = collate;
  }

  public void setCollate(String iCollate) {
    if (iCollate == null) {
      iCollate = DefaultCollate.NAME;
    }

    setCollate(SQLEngine.getCollate(iCollate));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (AbstractIndexDefinition) o;

    if (!collate.equals(that.collate)) {
      return false;
    }

    return nullValuesIgnored == that.nullValuesIgnored;
  }

  @Override
  public int hashCode() {
    var result = collate.hashCode();
    result = 31 * result + (nullValuesIgnored ? 1 : 0);
    return result;
  }

  @Override
  public boolean isNullValuesIgnored() {
    return nullValuesIgnored;
  }

  @Override
  public void setNullValuesIgnored(boolean value) {
    nullValuesIgnored = value;
  }

  protected void serializeToMap(@Nonnull Map<String, Object> map, DatabaseSessionInternal session) {
  }


  protected void serializeFromMap(@Nonnull Map<String, ?> map) {
  }

  protected static <T> T refreshRid(DatabaseSessionInternal session, T value) {
    if (value instanceof RID rid) {
      if (rid.isNew()) {
        try {
          var record = session.load(rid);
          //noinspection unchecked
          value = (T) record.getIdentity();
        } catch (RecordNotFoundException rnf) {
          return value;
        }
      }
    }
    return value;
  }
}
