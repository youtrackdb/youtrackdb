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
package com.jetbrains.youtrack.db.internal.core.metadata.security;


import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule.ResourceGeneric;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.security.SecuritySystem;
import com.jetbrains.youtrack.db.internal.core.type.IdentityWrapper;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains the user settings about security and permissions. Each user has one or more roles
 * associated. Roles contains the permission rules that define what the user can access and what he
 * cannot.
 *
 * @see Role
 */
public class SecurityUserImpl extends IdentityWrapper implements SecurityUser {

  public static final String ADMIN = "admin";
  public static final String CLASS_NAME = "OUser";
  public static final String PASSWORD_FIELD = "password";

  public SecurityUserImpl(DatabaseSessionInternal db, final String iName) {
    super(db, CLASS_NAME);

    setProperty("name", iName);
    setAccountStatus(db, STATUSES.ACTIVE);
  }

  public SecurityUserImpl(DatabaseSessionInternal db, String iUserName,
      final String iUserPassword) {
    super(db, "OUser");
    setProperty("name", iUserName);
    setPassword(db, iUserPassword);
    setAccountStatus(db, STATUSES.ACTIVE);
  }

  /**
   * Create the user by reading the source entity.
   */
  public SecurityUserImpl(DatabaseSessionInternal session, final EntityImpl iSource) {
    super(session, iSource);
  }

  @Override
  protected Object deserializeProperty(DatabaseSessionInternal db, String propertyName,
      Object value) {
    if (propertyName.equals("roles")) {
      final Set<Role> roles = new HashSet<>();
      if (value != null) {
        //noinspection rawtypes
        for (final Object o : (Collection) value) {
          roles.add(createRole(db, (EntityImpl) ((RID) o).getEntity(db)));
        }
      }

      return roles;
    }

    return super.deserializeProperty(db, propertyName, value);
  }

  public static String encryptPassword(final String password) {
    return SecurityManager.createHash(
        password,
        GlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM.getValueAsString(),
        true);
  }

  public static boolean encodePassword(
      DatabaseSessionInternal session, final EntityImpl entity) {
    final String name = entity.field("name");
    if (name == null) {
      throw new SecurityException("User name not found");
    }

    final String password = entity.field("password");

    if (password == null) {
      throw new SecurityException("User '" + entity.field("name") + "' has no password");
    }
    SecuritySystem security = session.getSharedContext().getYouTrackDB().getSecuritySystem();
    security.validatePassword(name, password);

    if (!(!password.isEmpty() && password.charAt(0) == '{')) {
      entity.field("password", encryptPassword(password));
      return true;
    }

    return false;
  }

  /**
   * Derived classes can override createRole() to return an extended Role implementation or null if
   * the role should not be added.
   */
  protected Role createRole(DatabaseSessionInternal session, final EntityImpl roleEntity) {
    return new Role(session, roleEntity);
  }

  /**
   * Checks if the user has the permission to access to the requested resource for the requested
   * operation.
   *
   * @param iOperation Requested operation
   * @return The role that has granted the permission if any, otherwise a SecurityAccessException
   * exception is raised
   */
  public Role allow(
      DatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation) {
    var roles = getRoles();
    if (roles == null || roles.isEmpty()) {
      throw new SecurityAccessException(
          session.getName(),
          "User '" + getProperty("name") + "' has no role defined");
    }

    final Role role = checkIfAllowed(session, resourceGeneric, resourceSpecific, iOperation);
    if (role == null) {
      throw new SecurityAccessException(
          session.getName(),
          "User '"
              + getProperty("name")
              + "' does not have permission to execute the operation '"
              + Role.permissionToString(iOperation)
              + "' against the resource: "
              + resourceGeneric
              + "."
              + resourceSpecific);
    }

    return role;
  }

  /**
   * Checks if the user has the permission to access to the requested resource for the requested
   * operation.
   *
   * @param operation Requested operation
   * @return The role that has granted the permission if any, otherwise null
   */
  public Role checkIfAllowed(
      DatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int operation) {
    var roles = getRoles();

    for (Role r : roles) {
      if (r == null) {
        LogManager.instance()
            .warn(
                this,
                "User '%s' has a null role, ignoring it. Consider fixing this user's roles before"
                    + " continuing",
                getName(session));
      } else if (r.allow(resourceGeneric, resourceSpecific, operation)) {
        return r;
      }
    }

    return null;
  }

  @Override
  @Deprecated
  public SecurityRole allow(DatabaseSessionInternal session, String iResource, int iOperation) {
    final String resourceSpecific = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return allow(session, resourceGeneric, null, iOperation);
    }

    return allow(session, resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public SecurityRole checkIfAllowed(DatabaseSessionInternal session, String iResource,
      int iOperation) {
    final String resourceSpecific = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return checkIfAllowed(session, resourceGeneric, null, iOperation);
    }

    return checkIfAllowed(session, resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public boolean isRuleDefined(DatabaseSessionInternal session, String iResource) {
    final String resourceSpecific = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return isRuleDefined(session, resourceGeneric, null);
    }

    return isRuleDefined(session, resourceGeneric, resourceSpecific);
  }

  /**
   * Checks if a rule was defined for the user.
   *
   * @return True is a rule is defined, otherwise false
   */
  public boolean isRuleDefined(
      DatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific) {
    var roles = getRoles();
    for (Role r : roles) {
      if (r == null) {
        LogManager.instance()
            .warn(
                this,
                "User '%s' has a null role, bypass it. Consider to fix this user roles before to"
                    + " continue",
                getName(session));
      } else if (r.hasRule(resourceGeneric, resourceSpecific)) {
        return true;
      }
    }

    return false;
  }

  public boolean checkPassword(DatabaseSessionInternal session, final String iPassword) {
    return SecurityManager.checkPassword(iPassword, getProperty(PASSWORD_FIELD));
  }

  public String getName(DatabaseSessionInternal session) {
    return getProperty("name");
  }

  public SecurityUserImpl setName(DatabaseSessionInternal session, final String iName) {
    setProperty("name", iName);
    return this;
  }

  public String getPassword(DatabaseSessionInternal session) {
    return getProperty(PASSWORD_FIELD);
  }

  public SecurityUserImpl setPassword(DatabaseSessionInternal session, final String iPassword) {
    setProperty(PASSWORD_FIELD, iPassword);
    return this;
  }

  public STATUSES getAccountStatus(DatabaseSessionInternal session) {
    final String status = getProperty("status");
    if (status == null) {
      throw new SecurityException("User '" + getName(session) + "' has no status");
    }
    return STATUSES.valueOf(status);
  }

  public void setAccountStatus(DatabaseSessionInternal session, STATUSES accountStatus) {
    setProperty("status", accountStatus);
  }

  public Set<Role> getRoles() {
    return getProperty("roles");
  }

  public SecurityUserImpl addRole(DatabaseSessionInternal session, final String iRole) {
    if (iRole != null) {
      addRole(session, session.getMetadata().getSecurity().getRole(iRole));
    }
    return this;
  }

  @Override
  public SecurityUserImpl addRole(DatabaseSessionInternal session, final SecurityRole iRole) {
    if (iRole != null) {
      var roles = getRoles();
      roles.add((Role) iRole);
    }

    return this;
  }

  public boolean removeRole(DatabaseSessionInternal session, final String roleName) {
    boolean removed = false;
    var roles = getRoles();

    roles.removeIf(role -> role.getName(session).equals(roleName));

    return removed;
  }

  public boolean hasRole(DatabaseSessionInternal session, final String iRoleName,
      final boolean iIncludeInherited) {
    var roles = getRoles();

    for (final Role role : roles) {
      if (role.getName(session).equals(iRoleName)) {
        return true;
      }

      if (iIncludeInherited) {
        Role r = role.getParentRole();
        while (r != null) {
          if (r.getName(session).equals(iRoleName)) {
            return true;
          }
          r = r.getParentRole();
        }
      }
    }

    return false;
  }

  @Override
  public String toString() {
    var database = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null) {
      return getName(database);
    }

    return SecurityUserImpl.class.getName();
  }

  @Override
  public String getUserType() {
    return "Database";
  }
}
