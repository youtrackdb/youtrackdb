package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
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
      YTDatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation);

  OSecurityRole checkIfAllowed(
      YTDatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iOperation);

  boolean isRuleDefined(YTDatabaseSessionInternal session, final ResourceGeneric resourceGeneric,
      String resourceSpecific);

  @Deprecated
  OSecurityRole allow(YTDatabaseSessionInternal session, final String iResource,
      final int iOperation);

  @Deprecated
  OSecurityRole checkIfAllowed(YTDatabaseSessionInternal session, final String iResource,
      final int iOperation);

  @Deprecated
  boolean isRuleDefined(YTDatabaseSessionInternal session, final String iResource);

  boolean checkPassword(YTDatabaseSessionInternal session, final String iPassword);

  String getName(YTDatabaseSessionInternal session);

  OSecurityUser setName(YTDatabaseSessionInternal session, final String iName);

  String getPassword(YTDatabaseSessionInternal session);

  OSecurityUser setPassword(YTDatabaseSessionInternal session, final String iPassword);

  OSecurityUser.STATUSES getAccountStatus(YTDatabaseSessionInternal session);

  void setAccountStatus(YTDatabaseSessionInternal session, STATUSES accountStatus);

  Set<? extends OSecurityRole> getRoles();

  OSecurityUser addRole(YTDatabaseSessionInternal session, final String iRole);

  OSecurityUser addRole(YTDatabaseSessionInternal session, final OSecurityRole iRole);

  boolean removeRole(YTDatabaseSessionInternal session, final String iRoleName);

  boolean hasRole(YTDatabaseSessionInternal session, final String iRoleName,
      final boolean iIncludeInherited);

  YTIdentifiable getIdentity(YTDatabaseSessionInternal session);

  String getUserType();
}
