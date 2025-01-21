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

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;

/**
 *
 */
public class SecuritySystemUserImpl extends SecurityUserImpl {
  private String databaseName;
  private String userType;

  protected String getDatabaseName() {
    return databaseName;
  }

  public SecuritySystemUserImpl(DatabaseSessionInternal session, final String iName) {
    super(session, iName);
  }

  public SecuritySystemUserImpl(DatabaseSessionInternal session, String iUserName,
      final String iUserPassword) {
    super(session, iUserName, iUserPassword);
  }

  public SecuritySystemUserImpl(DatabaseSessionInternal session, String iUserName,
      final String iUserPassword,
      String userType) {
    super(session, iUserName, iUserPassword);
    this.userType = userType;
  }

  /**
   * Create the user by reading the source document.
   */
  public SecuritySystemUserImpl(DatabaseSessionInternal session, final EntityImpl iSource) {
    super(session, iSource);
  }

  /**
   * dbName is the name of the source database and is used for filtering roles.
   */
  public SecuritySystemUserImpl(DatabaseSessionInternal session, final EntityImpl iSource,
      final String dbName) {
    databaseName = dbName;
  }

  /**
   * Derived classes can override createRole() to return an extended Role implementation.
   */
  protected Role createRole(DatabaseSessionInternal session, final EntityImpl roleEntity) {
    Role role = null;

    // If databaseName is set, then only allow roles with the same databaseName.
    if (databaseName != null && !databaseName.isEmpty()) {
      if (roleEntity != null
          && roleEntity.containsField(SystemRole.DB_FILTER)
          && roleEntity.fieldType(SystemRole.DB_FILTER) == PropertyType.EMBEDDEDLIST) {

        List<String> dbNames = roleEntity.field(SystemRole.DB_FILTER, PropertyType.EMBEDDEDLIST);

        for (String dbName : dbNames) {
          if (dbName != null
              && !dbName.isEmpty()
              && (dbName.equalsIgnoreCase(databaseName) || dbName.equals("*"))) {
            role = new SystemRole(session, roleEntity);
            break;
          }
        }
      }
    } else {
      // If databaseName is not set, only return roles without a SystemRole.DB_FILTER property or
      // if set to "*".
      if (roleEntity != null) {
        if (!roleEntity.containsField(SystemRole.DB_FILTER)) {
          role = new SystemRole(session, roleEntity);
        } else { // It does use the dbFilter property.
          if (roleEntity.fieldType(SystemRole.DB_FILTER) == PropertyType.EMBEDDEDLIST) {
            List<String> dbNames = roleEntity.field(SystemRole.DB_FILTER,
                PropertyType.EMBEDDEDLIST);

            for (String dbName : dbNames) {
              if (dbName != null && !dbName.isEmpty() && dbName.equals("*")) {
                role = new SystemRole(session, roleEntity);
                break;
              }
            }
          }
        }
      }
    }

    return role;
  }

  @Override
  public String getUserType() {
    return userType;
  }
}
