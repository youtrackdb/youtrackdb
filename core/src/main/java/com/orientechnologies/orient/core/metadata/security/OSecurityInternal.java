package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole.ALLOW_MODES;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface OSecurityInternal {

  boolean isAllowed(
      ODatabaseSessionInternal session, Set<OIdentifiable> iAllowAll,
      Set<OIdentifiable> iAllowOperation);

  @Deprecated
  OIdentifiable allowUser(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iUserName);

  @Deprecated
  OIdentifiable allowRole(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName);

  @Deprecated
  OIdentifiable denyUser(
      ODatabaseSessionInternal session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iUserName);

  @Deprecated
  OIdentifiable denyRole(
      ODatabaseSessionInternal session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName);

  @Deprecated
  OIdentifiable allowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId);

  @Deprecated
  OIdentifiable disallowIdentity(
      ODatabaseSessionInternal session, ODocument iDocument, String iAllowFieldName,
      OIdentifiable iId);

  OUser authenticate(ODatabaseSessionInternal session, String iUsername, String iUserPassword);

  OUser createUser(
      ODatabaseSessionInternal session, String iUserName, String iUserPassword, String[] iRoles);

  OUser createUser(
      ODatabaseSessionInternal session, String iUserName, String iUserPassword, ORole[] iRoles);

  OUser authenticate(ODatabaseSessionInternal session, OToken authToken);

  ORole createRole(
      ODatabaseSessionInternal session,
      String iRoleName,
      ORole iParent,
      ALLOW_MODES iAllowMode);

  ORole createRole(
      ODatabaseSessionInternal session, String iRoleName, ALLOW_MODES iAllowMode);

  OUser getUser(ODatabaseSession session, String iUserName);

  OUser getUser(ODatabaseSession session, ORID userId);

  ORole getRole(ODatabaseSession session, String iRoleName);

  ORole getRole(ODatabaseSession session, OIdentifiable iRoleRid);

  List<ODocument> getAllUsers(ODatabaseSession session);

  List<ODocument> getAllRoles(ODatabaseSession session);

  Map<String, OSecurityPolicy> getSecurityPolicies(ODatabaseSession session, OSecurityRole role);

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
  OSecurityPolicy getSecurityPolicy(ODatabaseSession session, OSecurityRole role, String resource);

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
      ODatabaseSessionInternal session, OSecurityRole role, String resource,
      OSecurityPolicyImpl policy);

  /**
   * creates and saves an empty security policy
   *
   * @param session the session to a DB where the policy has to be created
   * @param name    the policy name
   * @return
   */
  OSecurityPolicyImpl createSecurityPolicy(ODatabaseSession session, String name);

  OSecurityPolicyImpl getSecurityPolicy(ODatabaseSession session, String name);

  void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicyImpl policy);

  void deleteSecurityPolicy(ODatabaseSession session, String name);

  /**
   * Removes security policy bound to a role for a specific resource
   *
   * @param session  A valid db session to perform the operation
   * @param role     the role
   * @param resource the string representation of the security resource, eg.
   *                 "database.class.Person"
   */
  void removeSecurityPolicy(ODatabaseSession session, ORole role, String resource);

  boolean dropUser(ODatabaseSession session, String iUserName);

  boolean dropRole(ODatabaseSession session, String iRoleName);

  void createClassTrigger(ODatabaseSessionInternal session);

  long getVersion(ODatabaseSession session);

  void incrementVersion(ODatabaseSession session);

  OUser create(ODatabaseSessionInternal session);

  void load(ODatabaseSessionInternal session);

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
  Set<String> getFilteredProperties(ODatabaseSessionInternal session, ODocument document);

  /**
   * For property-level security
   *
   * @param session
   * @param document     current document to check for proeprty-level security
   * @param propertyName the property to check for write access
   * @return
   */
  boolean isAllowedWrite(ODatabaseSessionInternal session, ODocument document, String propertyName);

  boolean canCreate(ODatabaseSessionInternal session, ORecord record);

  boolean canRead(ODatabaseSessionInternal session, ORecord record);

  boolean canUpdate(ODatabaseSessionInternal session, ORecord record);

  boolean canDelete(ODatabaseSessionInternal session, ORecord record);

  boolean canExecute(ODatabaseSessionInternal session, OFunction function);

  /**
   * checks if for current session a resource is restricted by security resources (ie. READ policies
   * exist, with predicate different from "TRUE", to access the given resource
   *
   * @param session  The session to check for the existece of policies
   * @param resource a resource string, eg. "database.class.Person"
   * @return true if a restriction of any type exists for this session and this resource. False
   * otherwise
   */
  boolean isReadRestrictedBySecurityPolicy(ODatabaseSession session, String resource);

  /**
   * returns the list of all the filtered properties (for any role defined in the db)
   *
   * @param database
   * @return
   */
  Set<OSecurityResourceProperty> getAllFilteredProperties(ODatabaseSessionInternal database);

  OSecurityUser securityAuthenticate(ODatabaseSessionInternal session, String userName,
      String password);

  OSecurityUser securityAuthenticate(
      ODatabaseSessionInternal session, OAuthenticationInfo authenticationInfo);
}
