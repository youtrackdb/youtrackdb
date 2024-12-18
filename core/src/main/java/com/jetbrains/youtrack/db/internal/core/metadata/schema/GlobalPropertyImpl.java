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

package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

public class GlobalPropertyImpl implements GlobalProperty {

  private String name;
  private PropertyType type;
  private Integer id;

  public GlobalPropertyImpl() {
  }

  public GlobalPropertyImpl(final String name, final PropertyType type, final Integer id) {
    this.name = name;
    this.type = type;
    this.id = id;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public PropertyType getType() {
    return type;
  }

  public void fromEntity(final EntityImpl entity) {
    this.name = entity.field("name");
    this.type = PropertyType.valueOf(entity.field("type"));
    this.id = entity.field("id");
  }

  public EntityImpl toEntity(DatabaseSessionInternal db) {
    final EntityImpl entity = new EntityImpl(db);
    entity.field("name", name);
    entity.field("type", type.name());
    entity.field("id", id);
    return entity;
  }
}
