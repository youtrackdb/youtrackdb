package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
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
      ODatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation);

  OSecurityRole checkIfAllowed(
      ODatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation);

  boolean isRuleDefined(ODatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific);

  @Deprecated
  OSecurityRole allow(ODatabaseSessionInternal session, final String iResource,
      final int iOperation);

  @Deprecated
  OSecurityRole checkIfAllowed(ODatabaseSessionInternal session, final String iResource,
      final int iOperation);

  @Deprecated
  boolean isRuleDefined(ODatabaseSessionInternal session, final String iResource);

  boolean checkPassword(ODatabaseSessionInternal session, final String iPassword);

  String getName(ODatabaseSessionInternal session);

  OSecurityUser setName(ODatabaseSessionInternal session, final String iName);

  String getPassword(ODatabaseSessionInternal session);

  OSecurityUser setPassword(ODatabaseSessionInternal session, final String iPassword);

  OSecurityUser.STATUSES getAccountStatus(ODatabaseSessionInternal session);

  void setAccountStatus(ODatabaseSessionInternal session, STATUSES accountStatus);

  Set<? extends OSecurityRole> getRoles();

  OSecurityUser addRole(ODatabaseSessionInternal session, final String iRole);

  OSecurityUser addRole(ODatabaseSessionInternal session, final OSecurityRole iRole);

  boolean removeRole(ODatabaseSessionInternal session, final String iRoleName);

  boolean hasRole(ODatabaseSessionInternal session, final String iRoleName,
      final boolean iIncludeInherited);

  OIdentifiable getIdentity(ODatabaseSessionInternal session);

  String getUserType();
}
