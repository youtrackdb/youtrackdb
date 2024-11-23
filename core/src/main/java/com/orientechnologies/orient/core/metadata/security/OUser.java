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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Contains the user settings about security and permissions. Each user has one or more roles
 * associated. Roles contains the permission rules that define what the user can access and what he
 * cannot.
 *
 * @see ORole
 */
public class OUser extends OIdentity implements OSecurityUser {

  public static final String ADMIN = "admin";
  public static final String CLASS_NAME = "OUser";
  public static final String PASSWORD_FIELD = "password";

  private static final long serialVersionUID = 1L;

  // AVOID THE INVOCATION OF SETTER
  protected Set<ORole> roles = new HashSet<ORole>();

  /**
   * Constructor used in unmarshalling.
   */
  public OUser() {
  }

  public OUser(ODatabaseSession session, final String iName) {
    super(CLASS_NAME);
    getDocument(session).field("name", iName);
    setAccountStatus(session, STATUSES.ACTIVE);
  }

  public OUser(ODatabaseSession session, String iUserName, final String iUserPassword) {
    super("OUser");
    getDocument(session).field("name", iUserName);
    setPassword(session, iUserPassword);
    setAccountStatus(session, STATUSES.ACTIVE);
  }

  /**
   * Create the user by reading the source document.
   */
  public OUser(ODatabaseSession session, final ODocument iSource) {
    fromStream((ODatabaseSessionInternal) session, iSource);
  }

  public static final String encryptPassword(final String iPassword) {
    return OSecurityManager.createHash(
        iPassword,
        OGlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM.getValueAsString(),
        true);
  }

  public static boolean encodePassword(
      ODatabaseSessionInternal session, final ODocument iDocument) {
    final String name = iDocument.field("name");
    if (name == null) {
      throw new OSecurityException("User name not found");
    }

    final String password = iDocument.field("password");

    if (password == null) {
      throw new OSecurityException("User '" + iDocument.field("name") + "' has no password");
    }
    OSecuritySystem security = session.getSharedContext().getOrientDB().getSecuritySystem();
    security.validatePassword(name, password);

    if (!password.startsWith("{")) {
      iDocument.field("password", encryptPassword(password));
      return true;
    }

    return false;
  }

  @Override
  public void fromStream(ODatabaseSessionInternal session, final ODocument iSource) {
    if (getDocument(session) != null) {
      return;
    }

    setDocument(session, iSource);

    roles = new HashSet<>();
    final Collection<OIdentifiable> loadedRoles = iSource.field("roles");
    if (loadedRoles != null) {
      for (final OIdentifiable d : loadedRoles) {
        if (d != null) {
          ORole role = createRole(session, d.getRecord());
          if (role != null) {
            roles.add(role);
          }
        } else {
          OLogManager.instance()
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
   * Derived classes can override createRole() to return an extended ORole implementation or null if
   * the role should not be added.
   */
  protected ORole createRole(ODatabaseSessionInternal session, final ODocument roleDoc) {
    return new ORole(session, roleDoc);
  }

  /**
   * Checks if the user has the permission to access to the requested resource for the requested
   * operation.
   *
   * @param session
   * @param iOperation Requested operation
   * @return The role that has granted the permission if any, otherwise a OSecurityAccessException
   * exception is raised
   * @throws OSecurityAccessException
   */
  public ORole allow(
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    var document = getDocument(session);
    if (roles == null || roles.isEmpty()) {
      if (document.field("roles") != null
          && !((Collection<OIdentifiable>) document.field("roles")).isEmpty()) {
        final ODocument doc = document;
        document = null;
        fromStream(sessionInternal, doc);
      } else {
        throw new OSecurityAccessException(
            sessionInternal.getName(),
            "User '" + document.field("name") + "' has no role defined");
      }
    }

    final ORole role = checkIfAllowed(session, resourceGeneric, resourceSpecific, iOperation);

    if (role == null) {
      throw new OSecurityAccessException(
          document.getSession().getName(),
          "User '"
              + document.field("name")
              + "' does not have permission to execute the operation '"
              + ORole.permissionToString(iOperation)
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
  public ORole checkIfAllowed(
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    for (ORole r : roles) {
      if (r == null) {
        OLogManager.instance()
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
  public OSecurityRole allow(ODatabaseSession session, String iResource, int iOperation) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return allow(session, resourceGeneric, null, iOperation);
    }

    return allow(session, resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public OSecurityRole checkIfAllowed(ODatabaseSession session, String iResource, int iOperation) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return checkIfAllowed(session, resourceGeneric, null, iOperation);
    }

    return checkIfAllowed(session, resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public boolean isRuleDefined(ODatabaseSession session, String iResource) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

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
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific) {
    for (ORole r : roles) {
      if (r == null) {
        OLogManager.instance()
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

  public boolean checkPassword(ODatabaseSession session, final String iPassword) {
    return OSecurityManager.checkPassword(iPassword, getDocument(session).field(PASSWORD_FIELD));
  }

  public String getName(ODatabaseSession session) {
    return getDocument(session).field("name");
  }

  public OUser setName(ODatabaseSession session, final String iName) {
    getDocument(session).field("name", iName);
    return this;
  }

  public String getPassword(ODatabaseSession session) {
    return getDocument(session).field(PASSWORD_FIELD);
  }

  public OUser setPassword(ODatabaseSession session, final String iPassword) {
    getDocument(session).field(PASSWORD_FIELD, iPassword);
    return this;
  }

  public STATUSES getAccountStatus(ODatabaseSession session) {
    final String status = getDocument(session).field("status");
    if (status == null) {
      throw new OSecurityException("User '" + getName(session) + "' has no status");
    }
    return STATUSES.valueOf(status);
  }

  public void setAccountStatus(ODatabaseSession session, STATUSES accountStatus) {
    getDocument(session).field("status", accountStatus);
  }

  public Set<ORole> getRoles() {
    return roles;
  }

  public OUser addRole(ODatabaseSession session, final String iRole) {
    if (iRole != null) {
      addRole(session, session.getMetadata().getSecurity().getRole(iRole));
    }
    return this;
  }

  public OUser addRole(ODatabaseSession session, final OSecurityRole iRole) {
    if (iRole != null) {
      roles.add((ORole) iRole);
    }

    final HashSet<ODocument> persistentRoles = new HashSet<ODocument>();
    for (ORole r : roles) {
      persistentRoles.add(r.toStream(session));
    }
    getDocument(session).field("roles", persistentRoles);
    return this;
  }

  public boolean removeRole(ODatabaseSession session, final String iRoleName) {
    boolean removed = false;
    var document = getDocument(session);
    for (Iterator<ORole> it = roles.iterator(); it.hasNext(); ) {
      if (it.next().getName(session).equals(iRoleName)) {
        it.remove();
        removed = true;
      }
    }

    if (removed) {
      final HashSet<ODocument> persistentRoles = new HashSet<ODocument>();
      for (ORole r : roles) {
        persistentRoles.add(r.toStream(session));
      }
      document.field("roles", persistentRoles);
    }

    return removed;
  }

  public boolean hasRole(ODatabaseSession session, final String iRoleName,
      final boolean iIncludeInherited) {
    for (Iterator<ORole> it = roles.iterator(); it.hasNext(); ) {
      final ORole role = it.next();
      if (role.getName(session).equals(iRoleName)) {
        return true;
      }

      if (iIncludeInherited) {
        ORole r = role.getParentRole();
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
  public OUser save(ODatabaseSession session) {
    getDocument(session).save(OUser.class.getSimpleName());
    return this;
  }

  @Override
  public String toString() {
    var database = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null) {
      return getName(database);
    }

    return OUser.class.getName();
  }

  @Override
  public OIdentifiable getIdentity(ODatabaseSession session) {
    return getDocument(session);
  }

  @Override
  public String getUserType() {
    return "Database";
  }
}
