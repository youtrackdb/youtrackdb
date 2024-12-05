package com.orientechnologies.core.metadata.security;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.metadata.security.ORule.ResourceGeneric;
import java.io.Serializable;
import java.util.Set;

/**
 * @since 03/11/14
 */
public interface YTSecurityUser extends Serializable {

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

  YTSecurityUser setName(YTDatabaseSessionInternal session, final String iName);

  String getPassword(YTDatabaseSessionInternal session);

  YTSecurityUser setPassword(YTDatabaseSessionInternal session, final String iPassword);

  YTSecurityUser.STATUSES getAccountStatus(YTDatabaseSessionInternal session);

  void setAccountStatus(YTDatabaseSessionInternal session, STATUSES accountStatus);

  Set<? extends OSecurityRole> getRoles();

  YTSecurityUser addRole(YTDatabaseSessionInternal session, final String iRole);

  YTSecurityUser addRole(YTDatabaseSessionInternal session, final OSecurityRole iRole);

  boolean removeRole(YTDatabaseSessionInternal session, final String iRoleName);

  boolean hasRole(YTDatabaseSessionInternal session, final String iRoleName,
      final boolean iIncludeInherited);

  YTIdentifiable getIdentity(YTDatabaseSessionInternal session);

  String getUserType();
}
