/*
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
package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SystemRole extends Role {

  public static final String DB_FILTER = "dbFilter";

  private List<String> dbFilter;

  public List<String> getDbFilter() {
    return dbFilter;
  }

  /**
   * Constructor used in unmarshalling.
   */
  public SystemRole() {
  }

  public SystemRole(
      DatabaseSessionInternal session, final String iName,
      final Role iParent,
      final ALLOW_MODES iAllowMode,
      Map<String, SecurityPolicy> policies) {
    super(session, iName, iParent, iAllowMode, policies);
  }

  /**
   * Create the role by reading the source entity.
   */
  public SystemRole(DatabaseSessionInternal session, final EntityImpl iSource) {
    super(session, iSource);
  }

  @Override
  public void fromStream(DatabaseSessionInternal session, final EntityImpl iSource) {
    super.fromStream(session, iSource);

    var entity = getDocument(session);
    if (entity != null
        && entity.containsField(DB_FILTER)
        && entity.fieldType(DB_FILTER) == PropertyType.EMBEDDEDLIST) {
      dbFilter = entity.field(DB_FILTER, PropertyType.EMBEDDEDLIST);
    }
  }
}
