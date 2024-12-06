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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule.ResourceGeneric;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.security.SecuritySystem;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Contains the user settings about security and permissions. Each user has one or more roles
 * associated. Roles contains the permission rules that define what the user can access and what he
 * cannot.
 *
 * @see Role
 */
public class SecurityUserIml extends Identity implements SecurityUser {

  public static final String ADMIN = "admin";
  public static final String CLASS_NAME = "OUser";
  public static final String PASSWORD_FIELD = "password";

  private static final long serialVersionUID = 1L;

  // AVOID THE INVOCATION OF SETTER
  protected Set<Role> roles = new HashSet<Role>();

  /**
   * Constructor used in unmarshalling.
   */
  public SecurityUserIml() {
  }

  public SecurityUserIml(DatabaseSessionInternal session, final String iName) {
    super(CLASS_NAME);
    getDocument(session).field("name", iName);
    setAccountStatus(session, STATUSES.ACTIVE);
  }

  public SecurityUserIml(DatabaseSessionInternal session, String iUserName,
      final String iUserPassword) {
    super("OUser");
    getDocument(session).field("name", iUserName);
    setPassword(session, iUserPassword);
    setAccountStatus(session, STATUSES.ACTIVE);
  }

  /**
   * Create the user by reading the source document.
   */
  public SecurityUserIml(DatabaseSession session, final EntityImpl iSource) {
    fromStream((DatabaseSessionInternal) session, iSource);
  }

  public static final String encryptPassword(final String iPassword) {
    return SecurityManager.createHash(
        iPassword,
        GlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM.getValueAsString(),
        true);
  }

  public static boolean encodePassword(
      DatabaseSessionInternal session, final EntityImpl iDocument) {
    final String name = iDocument.field("name");
    if (name == null) {
      throw new SecurityException("User name not found");
    }

    final String password = iDocument.field("password");

    if (password == null) {
      throw new SecurityException("User '" + iDocument.field("name") + "' has no password");
    }
    SecuritySystem security = session.getSharedContext().getYouTrackDB().getSecuritySystem();
    security.validatePassword(name, password);

    if (!password.startsWith("{")) {
      iDocument.field("password", encryptPassword(password));
      return true;
    }

    return false;
  }

  @Override
  public void fromStream(DatabaseSessionInternal session, final EntityImpl iSource) {
    if (getDocument(session) != null) {
      return;
    }

    setDocument(session, iSource);

    roles = new HashSet<>();
    final Collection<Identifiable> loadedRoles = iSource.field("roles");
    if (loadedRoles != null) {
      for (final Identifiable d : loadedRoles) {
        if (d != null) {
          Role role = createRole(session, d.getRecord());
          if (role != null) {
            roles.add(role);
          }
        } else {
          LogManager.instance()
              .warn(
                  this,
                  "User '%s' is declared to have a role that does not exist in the database. "
                      + " Ignoring it.",
                  getName(session));
        }
      }
    }
  }

  /**
   * Derived classes can override createRole() to return an extended Role implementation or null if
   * the role should not be added.
   */
  protected Role createRole(DatabaseSessionInternal session, final EntityImpl roleDoc) {
    return new Role(session, roleDoc);
  }

  /**
   * Checks if the user has the permission to access to the requested resource for the requested
   * operation.
   *
   * @param session
   * @param iOperation Requested operation
   * @return The role that has granted the permission if any, otherwise a SecurityAccessException
   * exception is raised
   * @throws SecurityAccessException
   */
  public Role allow(
      DatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation) {
    var sessionInternal = session;
    var document = getDocument(session);
    if (roles == null || roles.isEmpty()) {
      if (document.field("roles") != null
          && !((Collection<Identifiable>) document.field("roles")).isEmpty()) {
        final EntityImpl doc = document;
        document = null;
        fromStream(sessionInternal, doc);
      } else {
        throw new SecurityAccessException(
            sessionInternal.getName(),
            "User '" + document.field("name") + "' has no role defined");
      }
    }

    final Role role = checkIfAllowed(session, resourceGeneric, resourceSpecific, iOperation);

    if (role == null) {
      throw new SecurityAccessException(
          document.getSession().getName(),
          "User '"
              + document.field("name")
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
   * @param session
   * @param iOperation Requested operation
   * @return The role that has granted the permission if any, otherwise null
   */
  public Role checkIfAllowed(
      DatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation) {
    for (Role r : roles) {
      if (r == null) {
        LogManager.instance()
            .warn(
                this,
                "User '%s' has a null role, ignoring it. Consider fixing this user's roles before"
                    + " continuing",
                getName(session));
      } else if (r.allow(resourceGeneric, resourceSpecific, iOperation)) {
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
    return SecurityManager.checkPassword(iPassword, getDocument(session).field(PASSWORD_FIELD));
  }

  public String getName(DatabaseSessionInternal session) {
    return getDocument(session).field("name");
  }

  public SecurityUserIml setName(DatabaseSessionInternal session, final String iName) {
    getDocument(session).field("name", iName);
    return this;
  }

  public String getPassword(DatabaseSessionInternal session) {
    return getDocument(session).field(PASSWORD_FIELD);
  }

  public SecurityUserIml setPassword(DatabaseSessionInternal session, final String iPassword) {
    getDocument(session).field(PASSWORD_FIELD, iPassword);
    return this;
  }

  public STATUSES getAccountStatus(DatabaseSessionInternal session) {
    final String status = getDocument(session).field("status");
    if (status == null) {
      throw new SecurityException("User '" + getName(session) + "' has no status");
    }
    return STATUSES.valueOf(status);
  }

  public void setAccountStatus(DatabaseSessionInternal session, STATUSES accountStatus) {
    getDocument(session).field("status", accountStatus);
  }

  public Set<Role> getRoles() {
    return roles;
  }

  public SecurityUserIml addRole(DatabaseSessionInternal session, final String iRole) {
    if (iRole != null) {
      addRole(session, session.getMetadata().getSecurity().getRole(iRole));
    }
    return this;
  }

  @Override
  public SecurityUserIml addRole(DatabaseSessionInternal session, final SecurityRole iRole) {
    if (iRole != null) {
      roles.add((Role) iRole);
    }

    final HashSet<EntityImpl> persistentRoles = new HashSet<EntityImpl>();
    for (Role r : roles) {
      persistentRoles.add(r.toStream(session));
    }
    getDocument(session).field("roles", persistentRoles);
    return this;
  }

  public boolean removeRole(DatabaseSessionInternal session, final String iRoleName) {
    boolean removed = false;
    var document = getDocument(session);
    for (Iterator<Role> it = roles.iterator(); it.hasNext(); ) {
      if (it.next().getName(session).equals(iRoleName)) {
        it.remove();
        removed = true;
      }
    }

    if (removed) {
      final HashSet<EntityImpl> persistentRoles = new HashSet<EntityImpl>();
      for (Role r : roles) {
        persistentRoles.add(r.toStream(session));
      }
      document.field("roles", persistentRoles);
    }

    return removed;
  }

  public boolean hasRole(DatabaseSessionInternal session, final String iRoleName,
      final boolean iIncludeInherited) {
    for (Iterator<Role> it = roles.iterator(); it.hasNext(); ) {
      final Role role = it.next();
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
  @SuppressWarnings("unchecked")
  public SecurityUserIml save(DatabaseSessionInternal session) {
    getDocument(session).save();
    return this;
  }

  @Override
  public String toString() {
    var database = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null) {
      return getName(database);
    }

    return SecurityUserIml.class.getName();
  }

  @Override
  public Identifiable getIdentity(DatabaseSessionInternal session) {
    return getDocument(session);
  }

  @Override
  public String getUserType() {
    return "Database";
  }
}
