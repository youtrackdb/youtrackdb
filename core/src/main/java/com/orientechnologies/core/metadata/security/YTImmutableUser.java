package com.orientechnologies.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTSecurityAccessException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.core.security.OSecurityManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @since 03/11/14
 */
public class YTImmutableUser implements YTSecurityUser {

  private final long version;

  private final String name;
  private final String password;

  private final Set<OImmutableRole> roles = new HashSet<OImmutableRole>();

  private final STATUSES status;
  private final YTRID rid;
  private final String userType;

  public YTImmutableUser(YTDatabaseSessionInternal session, long version, YTSecurityUser user) {
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

  public YTImmutableUser(YTDatabaseSessionInternal session, String name, String userType) {
    this(session, name, "", userType, null);
  }

  public YTImmutableUser(YTDatabaseSessionInternal session, String name, String password,
      String userType, OSecurityRole role) {
    this.version = 0;
    this.name = name;
    this.password = password;
    this.status = STATUSES.ACTIVE;
    this.rid = new YTRecordId(-1, -1);
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
      YTDatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iOperation) {
    if (roles.isEmpty()) {
      throw new YTSecurityAccessException(name, "User '" + name + "' has no role defined");
    }

    final OSecurityRole role = checkIfAllowed(session, resourceGeneric, resourceSpecific,
        iOperation);

    if (role == null) {
      throw new YTSecurityAccessException(
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
      YTDatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
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
      YTDatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
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
  public OSecurityRole allow(YTDatabaseSessionInternal session, String iResource, int iOperation) {
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
  public OSecurityRole checkIfAllowed(YTDatabaseSessionInternal session, String iResource,
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
  public boolean isRuleDefined(YTDatabaseSessionInternal session, String iResource) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      return isRuleDefined(session, resourceGeneric, null);
    }

    return isRuleDefined(session, resourceGeneric, resourceSpecific);
  }

  public boolean checkPassword(YTDatabaseSessionInternal session, final String iPassword) {
    return OSecurityManager.checkPassword(iPassword, password);
  }

  public String getName(YTDatabaseSessionInternal session) {
    return name;
  }

  public YTUser setName(YTDatabaseSessionInternal session, final String iName) {
    throw new UnsupportedOperationException();
  }

  public String getPassword(YTDatabaseSessionInternal session) {
    return password;
  }

  public YTUser setPassword(YTDatabaseSessionInternal session, final String iPassword) {
    throw new UnsupportedOperationException();
  }

  public STATUSES getAccountStatus(YTDatabaseSessionInternal session) {
    return status;
  }

  public void setAccountStatus(YTDatabaseSessionInternal session, STATUSES accountStatus) {
    throw new UnsupportedOperationException();
  }

  public Set<OImmutableRole> getRoles() {
    return Collections.unmodifiableSet(roles);
  }

  public YTUser addRole(YTDatabaseSessionInternal session, final String iRole) {
    throw new UnsupportedOperationException();
  }

  public YTUser addRole(YTDatabaseSessionInternal session, final OSecurityRole iRole) {
    throw new UnsupportedOperationException();
  }

  public boolean removeRole(YTDatabaseSessionInternal session, final String iRoleName) {
    throw new UnsupportedOperationException();
  }

  public boolean hasRole(YTDatabaseSessionInternal session, final String iRoleName,
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
  public YTIdentifiable getIdentity(YTDatabaseSessionInternal session) {
    return rid;
  }

  @Override
  public String getUserType() {
    return userType;
  }
}
