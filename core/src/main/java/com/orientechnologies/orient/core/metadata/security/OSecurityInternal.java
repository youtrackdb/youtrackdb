package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole.ALLOW_MODES;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface OSecurityInternal {

  boolean isAllowed(
      YTDatabaseSessionInternal session, Set<YTIdentifiable> iAllowAll,
      Set<YTIdentifiable> iAllowOperation);

  @Deprecated
  YTIdentifiable allowUser(
      YTDatabaseSession session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iUserName);

  @Deprecated
  YTIdentifiable allowRole(
      YTDatabaseSession session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName);

  @Deprecated
  YTIdentifiable denyUser(
      YTDatabaseSessionInternal session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iUserName);

  @Deprecated
  YTIdentifiable denyRole(
      YTDatabaseSessionInternal session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName);

  @Deprecated
  YTIdentifiable allowIdentity(
      YTDatabaseSession session, YTEntityImpl iDocument, String iAllowFieldName,
      YTIdentifiable iId);

  @Deprecated
  YTIdentifiable disallowIdentity(
      YTDatabaseSessionInternal session, YTEntityImpl iDocument, String iAllowFieldName,
      YTIdentifiable iId);

  YTUser authenticate(YTDatabaseSessionInternal session, String iUsername, String iUserPassword);

  YTUser createUser(
      YTDatabaseSessionInternal session, String iUserName, String iUserPassword, String[] iRoles);

  YTUser createUser(
      YTDatabaseSessionInternal session, String iUserName, String iUserPassword, ORole[] iRoles);

  YTUser authenticate(YTDatabaseSessionInternal session, OToken authToken);

  ORole createRole(
      YTDatabaseSessionInternal session,
      String iRoleName,
      ORole iParent,
      ALLOW_MODES iAllowMode);

  ORole createRole(
      YTDatabaseSessionInternal session, String iRoleName, ALLOW_MODES iAllowMode);

  YTUser getUser(YTDatabaseSession session, String iUserName);

  YTUser getUser(YTDatabaseSession session, YTRID userId);

  ORole getRole(YTDatabaseSession session, String iRoleName);

  ORole getRole(YTDatabaseSession session, YTIdentifiable iRoleRid);

  List<YTEntityImpl> getAllUsers(YTDatabaseSession session);

  List<YTEntityImpl> getAllRoles(YTDatabaseSession session);

  Map<String, OSecurityPolicy> getSecurityPolicies(YTDatabaseSession session, OSecurityRole role);

  /**
   * Returns the security policy policy assigned to a role for a specific resource (not recursive on
   * superclasses, nor on role hierarchy)
   *
   * @param session  an active DB session
   * @param role     the role
   * @param resource the string representation of the security resource, eg.
   *                 "database.class.Person"
   * @return
   */
  OSecurityPolicy getSecurityPolicy(YTDatabaseSession session, OSecurityRole role, String resource);

  /**
   * Sets a security policy for a specific resource on a role
   *
   * @param session  a valid db session to perform the operation (that has permissions to do it)
   * @param role     The role
   * @param resource the string representation of the security resource, eg.
   *                 "database.class.Person"
   * @param policy   The security policy
   */
  void setSecurityPolicy(
      YTDatabaseSessionInternal session, OSecurityRole role, String resource,
      OSecurityPolicyImpl policy);

  /**
   * creates and saves an empty security policy
   *
   * @param session the session to a DB where the policy has to be created
   * @param name    the policy name
   * @return
   */
  OSecurityPolicyImpl createSecurityPolicy(YTDatabaseSession session, String name);

  OSecurityPolicyImpl getSecurityPolicy(YTDatabaseSession session, String name);

  void saveSecurityPolicy(YTDatabaseSession session, OSecurityPolicyImpl policy);

  void deleteSecurityPolicy(YTDatabaseSession session, String name);

  /**
   * Removes security policy bound to a role for a specific resource
   *
   * @param session  A valid db session to perform the operation
   * @param role     the role
   * @param resource the string representation of the security resource, eg.
   *                 "database.class.Person"
   */
  void removeSecurityPolicy(YTDatabaseSession session, ORole role, String resource);

  boolean dropUser(YTDatabaseSession session, String iUserName);

  boolean dropRole(YTDatabaseSession session, String iRoleName);

  void createClassTrigger(YTDatabaseSessionInternal session);

  long getVersion(YTDatabaseSession session);

  void incrementVersion(YTDatabaseSession session);

  YTUser create(YTDatabaseSessionInternal session);

  void load(YTDatabaseSessionInternal session);

  void close();

  /**
   * For property-level security. Returns the list of the properties that are hidden (ie. not
   * allowed to be read) for current session, regarding a specific document
   *
   * @param session  the db session
   * @param document the document to filter
   * @return the list of the properties that are hidden (ie. not allowed to be read) on current
   * document for current session
   */
  Set<String> getFilteredProperties(YTDatabaseSessionInternal session, YTEntityImpl document);

  /**
   * For property-level security
   *
   * @param session
   * @param document     current document to check for proeprty-level security
   * @param propertyName the property to check for write access
   * @return
   */
  boolean isAllowedWrite(YTDatabaseSessionInternal session, YTEntityImpl document,
      String propertyName);

  boolean canCreate(YTDatabaseSessionInternal session, YTRecord record);

  boolean canRead(YTDatabaseSessionInternal session, YTRecord record);

  boolean canUpdate(YTDatabaseSessionInternal session, YTRecord record);

  boolean canDelete(YTDatabaseSessionInternal session, YTRecord record);

  boolean canExecute(YTDatabaseSessionInternal session, OFunction function);

  /**
   * checks if for current session a resource is restricted by security resources (ie. READ policies
   * exist, with predicate different from "TRUE", to access the given resource
   *
   * @param session  The session to check for the existece of policies
   * @param resource a resource string, eg. "database.class.Person"
   * @return true if a restriction of any type exists for this session and this resource. False
   * otherwise
   */
  boolean isReadRestrictedBySecurityPolicy(YTDatabaseSession session, String resource);

  /**
   * returns the list of all the filtered properties (for any role defined in the db)
   *
   * @param database
   * @return
   */
  Set<OSecurityResourceProperty> getAllFilteredProperties(YTDatabaseSessionInternal database);

  YTSecurityUser securityAuthenticate(YTDatabaseSessionInternal session, String userName,
      String password);

  YTSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, OAuthenticationInfo authenticationInfo);
}
