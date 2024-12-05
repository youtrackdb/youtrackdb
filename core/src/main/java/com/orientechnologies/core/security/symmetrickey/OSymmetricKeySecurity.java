/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
package com.orientechnologies.core.security.symmetrickey;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTSecurityAccessException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.function.OFunction;
import com.orientechnologies.core.metadata.security.ORestrictedOperation;
import com.orientechnologies.core.metadata.security.ORole;
import com.orientechnologies.core.metadata.security.OSecurityInternal;
import com.orientechnologies.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.core.metadata.security.OSecurityPolicyImpl;
import com.orientechnologies.core.metadata.security.OSecurityResourceProperty;
import com.orientechnologies.core.metadata.security.OSecurityRole;
import com.orientechnologies.core.metadata.security.OSecurityRole.ALLOW_MODES;
import com.orientechnologies.core.metadata.security.OToken;
import com.orientechnologies.core.metadata.security.YTSecurityUser;
import com.orientechnologies.core.metadata.security.YTUser;
import com.orientechnologies.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.security.OSecurityManager;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a symmetric key specific authentication. Implements an OSecurity interface that
 * delegates to the specified OSecurity object.
 *
 * <p>This is used with embedded (non-server) databases, like so:
 * db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSymmetricKeySecurity.class);
 */
public class OSymmetricKeySecurity implements OSecurityInternal {

  private final OSecurityInternal delegate;

  public OSymmetricKeySecurity(final OSecurityInternal iDelegate) {
    this.delegate = iDelegate;
  }

  @Override
  public YTSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, String userName, String password) {
    return authenticate(session, userName, password);
  }

  public YTUser authenticate(
      YTDatabaseSessionInternal session, final String username, final String password) {
    if (delegate == null) {
      throw new YTSecurityAccessException(
          "OSymmetricKeySecurity.authenticate() Delegate is null for username: " + username);
    }

    if (session == null) {
      throw new YTSecurityAccessException(
          "OSymmetricKeySecurity.authenticate() Database is null for username: " + username);
    }

    final String dbName = session.getName();

    YTUser user = delegate.getUser(session, username);

    if (user == null) {
      throw new YTSecurityAccessException(
          dbName,
          "OSymmetricKeySecurity.authenticate() Username or Key is invalid for username: "
              + username);
    }

    if (user.getAccountStatus(session) != YTSecurityUser.STATUSES.ACTIVE) {
      throw new YTSecurityAccessException(
          dbName, "OSymmetricKeySecurity.authenticate() User '" + username + "' is not active");
    }

    try {
      OUserSymmetricKeyConfig userConfig = new OUserSymmetricKeyConfig(user.getDocument(session));

      OSymmetricKey sk = OSymmetricKey.fromConfig(userConfig);

      String decryptedUsername = sk.decryptAsString(password);

      if (OSecurityManager.checkPassword(username, decryptedUsername)) {
        return user;
      }
    } catch (Exception ex) {
      throw YTException.wrapException(
          new YTSecurityAccessException(
              dbName,
              "OSymmetricKeySecurity.authenticate() Exception for session: "
                  + dbName
                  + ", username: "
                  + username
                  + " "
                  + ex.getMessage()),
          ex);
    }

    throw new YTSecurityAccessException(
        dbName,
        "OSymmetricKeySecurity.authenticate() Username or Key is invalid for session: "
            + dbName
            + ", username: "
            + username);
  }

  @Override
  public boolean isAllowed(
      YTDatabaseSessionInternal session,
      final Set<YTIdentifiable> iAllowAll,
      final Set<YTIdentifiable> iAllowOperation) {
    return delegate.isAllowed(session, iAllowAll, iAllowOperation);
  }

  @Override
  public YTIdentifiable allowUser(
      YTDatabaseSession session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iUserName) {
    return delegate.allowUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public YTIdentifiable allowRole(
      YTDatabaseSession session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName) {
    return delegate.allowRole(session, iDocument, iOperationType, iRoleName);
  }

  @Override
  public YTIdentifiable denyUser(
      YTDatabaseSessionInternal session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iUserName) {
    return delegate.denyUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public YTIdentifiable denyRole(
      YTDatabaseSessionInternal session,
      YTEntityImpl iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName) {
    return delegate.denyRole(session, iDocument, iOperationType, iRoleName);
  }

  @Override
  public YTIdentifiable allowIdentity(
      YTDatabaseSession session, YTEntityImpl iDocument, String iAllowFieldName,
      YTIdentifiable iId) {
    return delegate.allowIdentity(session, iDocument, iAllowFieldName, iId);
  }

  @Override
  public YTIdentifiable disallowIdentity(
      YTDatabaseSessionInternal session, YTEntityImpl iDocument, String iAllowFieldName,
      YTIdentifiable iId) {
    return delegate.disallowIdentity(session, iDocument, iAllowFieldName, iId);
  }

  public YTUser create(YTDatabaseSessionInternal session) {
    return delegate.create(session);
  }

  public void load(YTDatabaseSessionInternal session) {
    delegate.load(session);
  }

  public YTUser authenticate(YTDatabaseSessionInternal session, final OToken authToken) {
    return null;
  }

  public YTUser getUser(YTDatabaseSession session, final String iUserName) {
    return delegate.getUser(session, iUserName);
  }

  public YTUser getUser(YTDatabaseSession session, final YTRID iUserId) {
    return delegate.getUser(session, iUserId);
  }

  public YTUser createUser(
      YTDatabaseSessionInternal session,
      final String iUserName,
      final String iUserPassword,
      final String... iRoles) {
    return delegate.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public YTUser createUser(
      YTDatabaseSessionInternal session,
      final String iUserName,
      final String iUserPassword,
      final ORole... iRoles) {
    return delegate.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public ORole getRole(YTDatabaseSession session, final String iRoleName) {
    return delegate.getRole(session, iRoleName);
  }

  public ORole getRole(YTDatabaseSession session, final YTIdentifiable iRole) {
    return delegate.getRole(session, iRole);
  }

  public ORole createRole(
      YTDatabaseSessionInternal session,
      final String iRoleName,
      final ALLOW_MODES iAllowMode) {
    return delegate.createRole(session, iRoleName, iAllowMode);
  }

  public ORole createRole(
      YTDatabaseSessionInternal session,
      final String iRoleName,
      final ORole iParent,
      final ALLOW_MODES iAllowMode) {
    return delegate.createRole(session, iRoleName, iParent, iAllowMode);
  }

  public List<YTEntityImpl> getAllUsers(YTDatabaseSession session) {
    return delegate.getAllUsers(session);
  }

  public List<YTEntityImpl> getAllRoles(YTDatabaseSession session) {
    return delegate.getAllRoles(session);
  }

  @Override
  public Map<String, OSecurityPolicy> getSecurityPolicies(
      YTDatabaseSession session, OSecurityRole role) {
    return delegate.getSecurityPolicies(session, role);
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(
      YTDatabaseSession session, OSecurityRole role, String resource) {
    return delegate.getSecurityPolicy(session, role, resource);
  }

  @Override
  public void setSecurityPolicy(
      YTDatabaseSessionInternal session, OSecurityRole role, String resource,
      OSecurityPolicyImpl policy) {
    delegate.setSecurityPolicy(session, role, resource, policy);
  }

  @Override
  public OSecurityPolicyImpl createSecurityPolicy(YTDatabaseSession session, String name) {
    return delegate.createSecurityPolicy(session, name);
  }

  @Override
  public OSecurityPolicyImpl getSecurityPolicy(YTDatabaseSession session, String name) {
    return delegate.getSecurityPolicy(session, name);
  }

  @Override
  public void saveSecurityPolicy(YTDatabaseSession session, OSecurityPolicyImpl policy) {
    delegate.saveSecurityPolicy(session, policy);
  }

  @Override
  public void deleteSecurityPolicy(YTDatabaseSession session, String name) {
    delegate.deleteSecurityPolicy(session, name);
  }

  @Override
  public void removeSecurityPolicy(YTDatabaseSession session, ORole role, String resource) {
    delegate.removeSecurityPolicy(session, role, resource);
  }

  public String toString() {
    return delegate.toString();
  }

  public boolean dropUser(YTDatabaseSession session, final String iUserName) {
    return delegate.dropUser(session, iUserName);
  }

  public boolean dropRole(YTDatabaseSession session, final String iRoleName) {
    return delegate.dropRole(session, iRoleName);
  }

  public void createClassTrigger(YTDatabaseSessionInternal session) {
    delegate.createClassTrigger(session);
  }

  @Override
  public long getVersion(YTDatabaseSession session) {
    return delegate.getVersion(session);
  }

  @Override
  public void incrementVersion(YTDatabaseSession session) {
    delegate.incrementVersion(session);
  }

  @Override
  public Set<String> getFilteredProperties(YTDatabaseSessionInternal session,
      YTEntityImpl document) {
    return delegate.getFilteredProperties(session, document);
  }

  @Override
  public boolean isAllowedWrite(YTDatabaseSessionInternal session, YTEntityImpl document,
      String propertyName) {
    return delegate.isAllowedWrite(session, document, propertyName);
  }

  @Override
  public boolean canCreate(YTDatabaseSessionInternal session, YTRecord record) {
    return delegate.canCreate(session, record);
  }

  @Override
  public boolean canRead(YTDatabaseSessionInternal session, YTRecord record) {
    return delegate.canRead(session, record);
  }

  @Override
  public boolean canUpdate(YTDatabaseSessionInternal session, YTRecord record) {
    return delegate.canUpdate(session, record);
  }

  @Override
  public boolean canDelete(YTDatabaseSessionInternal session, YTRecord record) {
    return delegate.canDelete(session, record);
  }

  @Override
  public boolean canExecute(YTDatabaseSessionInternal session, OFunction function) {
    return delegate.canExecute(session, function);
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(YTDatabaseSession session, String resource) {
    return delegate.isReadRestrictedBySecurityPolicy(session, resource);
  }

  @Override
  public Set<OSecurityResourceProperty> getAllFilteredProperties(
      YTDatabaseSessionInternal database) {
    return delegate.getAllFilteredProperties(database);
  }

  @Override
  public YTSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, OAuthenticationInfo authenticationInfo) {
    return delegate.securityAuthenticate(session, authenticationInfo);
  }

  @Override
  public void close() {
  }
}
