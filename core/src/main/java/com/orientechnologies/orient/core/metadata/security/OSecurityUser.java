package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import java.io.Serializable;
import java.util.Set;

/**
 * @since 03/11/14
 */
public interface OSecurityUser extends Serializable {

  String SERVER_USER_TYPE = "Server";
  String DATABASE_USER_TYPE = "Database";
  String SECURITY_USER_TYPE = "Security";

  enum STATUSES {
    SUSPENDED,
    ACTIVE
  }

  OSecurityRole allow(
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  OSecurityRole checkIfAllowed(
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  boolean isRuleDefined(ODatabaseSession session, final ResourceGeneric resourceGeneric,
      String resourceSpecific);

  @Deprecated
  OSecurityRole allow(ODatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole checkIfAllowed(ODatabaseSession session, final String iResource,
      final int iOperation);

  @Deprecated
  boolean isRuleDefined(ODatabaseSession session, final String iResource);

  boolean checkPassword(ODatabaseSession session, final String iPassword);

  String getName(ODatabaseSession session);

  OSecurityUser setName(ODatabaseSession session, final String iName);

  String getPassword(ODatabaseSession session);

  OSecurityUser setPassword(ODatabaseSession session, final String iPassword);

  OSecurityUser.STATUSES getAccountStatus(ODatabaseSession session);

  void setAccountStatus(ODatabaseSession session, STATUSES accountStatus);

  Set<? extends OSecurityRole> getRoles();

  OSecurityUser addRole(ODatabaseSession session, final String iRole);

  OSecurityUser addRole(ODatabaseSession session, final OSecurityRole iRole);

  boolean removeRole(ODatabaseSession session, final String iRoleName);

  boolean hasRole(ODatabaseSession session, final String iRoleName,
      final boolean iIncludeInherited);

  OIdentifiable getIdentity(ODatabaseSession session);

  String getUserType();
}
