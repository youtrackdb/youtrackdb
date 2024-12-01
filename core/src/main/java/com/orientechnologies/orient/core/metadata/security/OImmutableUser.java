package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.orient.core.security.OSecurityManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @since 03/11/14
 */
public class OImmutableUser implements OSecurityUser {

  private final long version;

  private final String name;
  private final String password;

  private final Set<OImmutableRole> roles = new HashSet<OImmutableRole>();

  private final STATUSES status;
  private final ORID rid;
  private final String userType;

  public OImmutableUser(ODatabaseSessionInternal session, long version, OSecurityUser user) {
    this.version = version;
    this.name = user.getName(session);
    this.password = user.getPassword(session);
    this.status = user.getAccountStatus(session);
    this.rid = user.getIdentity(session).getIdentity();
    this.userType = user.getUserType();

    for (OSecurityRole role : user.getRoles()) {
      roles.add(new OImmutableRole(session, role));
    }
  }

  public OImmutableUser(ODatabaseSessionInternal session, String name, String userType) {
    this(session, name, "", userType, null);
  }

  public OImmutableUser(ODatabaseSessionInternal session, String name, String password,
      String userType, OSecurityRole role) {
    this.version = 0;
    this.name = name;
    this.password = password;
    this.status = STATUSES.ACTIVE;
    this.rid = new ORecordId(-1, -1);
    this.userType = userType;
    if (role != null) {
      OImmutableRole immutableRole;
      if (role instanceof OImmutableRole) {
        immutableRole = (OImmutableRole) role;
      } else {
        immutableRole = new OImmutableRole(session, role);
      }
      roles.add(immutableRole);
    }
  }

  public OSecurityRole allow(
      ODatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iOperation) {
    if (roles.isEmpty()) {
      throw new OSecurityAccessException(name, "User '" + name + "' has no role defined");
    }

    final OSecurityRole role = checkIfAllowed(session, resourceGeneric, resourceSpecific,
        iOperation);

    if (role == null) {
      throw new OSecurityAccessException(
          name,
          "User '"
              + name
              + "' does not have permission to execute the operation '"
              + ORole.permissionToString(iOperation)
              + "' against the resource: "
              + resourceGeneric
              + "."
              + resourceSpecific);
    }

    return role;
  }

  public OSecurityRole checkIfAllowed(
      ODatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iOperation) {
    for (OImmutableRole r : roles) {
      if (r == null) {
        OLogManager.instance()
            .warn(
                this,
                "User '%s' has a null role, ignoring it.  Consider fixing this user's roles before"
                    + " continuing",
                name);
      } else if (r.allow(resourceGeneric, resourceSpecific, iOperation)) {
        return r;
      }
    }

    return null;
  }

  public boolean isRuleDefined(
      ODatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific) {
    for (OImmutableRole r : roles) {
      if (r == null) {
        OLogManager.instance()
            .warn(
                this,
                "UseOSecurityAuthenticatorr '%s' has a null role, ignoring it.  Consider fixing"
                    + " this user's roles before continuing",
                name);
      } else if (r.hasRule(resourceGeneric, resourceSpecific)) {
        return true;
      }
    }

    return false;
  }

  @Override
  @Deprecated
  public OSecurityRole allow(ODatabaseSessionInternal session, String iResource, int iOperation) {
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
  public OSecurityRole checkIfAllowed(ODatabaseSessionInternal session, String iResource,
      int iOperation) {
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
  public boolean isRuleDefined(ODatabaseSessionInternal session, String iResource) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return isRuleDefined(session, resourceGeneric, null);
    }

    return isRuleDefined(session, resourceGeneric, resourceSpecific);
  }

  public boolean checkPassword(ODatabaseSessionInternal session, final String iPassword) {
    return OSecurityManager.checkPassword(iPassword, password);
  }

  public String getName(ODatabaseSessionInternal session) {
    return name;
  }

  public OUser setName(ODatabaseSessionInternal session, final String iName) {
    throw new UnsupportedOperationException();
  }

  public String getPassword(ODatabaseSessionInternal session) {
    return password;
  }

  public OUser setPassword(ODatabaseSessionInternal session, final String iPassword) {
    throw new UnsupportedOperationException();
  }

  public STATUSES getAccountStatus(ODatabaseSessionInternal session) {
    return status;
  }

  public void setAccountStatus(ODatabaseSessionInternal session, STATUSES accountStatus) {
    throw new UnsupportedOperationException();
  }

  public Set<OImmutableRole> getRoles() {
    return Collections.unmodifiableSet(roles);
  }

  public OUser addRole(ODatabaseSessionInternal session, final String iRole) {
    throw new UnsupportedOperationException();
  }

  public OUser addRole(ODatabaseSessionInternal session, final OSecurityRole iRole) {
    throw new UnsupportedOperationException();
  }

  public boolean removeRole(ODatabaseSessionInternal session, final String iRoleName) {
    throw new UnsupportedOperationException();
  }

  public boolean hasRole(ODatabaseSessionInternal session, final String iRoleName,
      final boolean iIncludeInherited) {
    for (final OSecurityRole role : roles) {
      if (role.getName(session).equals(iRoleName)) {
        return true;
      }

      if (iIncludeInherited) {
        OSecurityRole r = role.getParentRole();
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
    return name;
  }

  public long getVersion() {
    return version;
  }

  @Override
  public OIdentifiable getIdentity(ODatabaseSessionInternal session) {
    return rid;
  }

  @Override
  public String getUserType() {
    return userType;
  }
}
