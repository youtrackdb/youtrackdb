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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.List;
import java.util.Set;

/**
 * Proxy class for user management
 */
public class OSecurityProxy implements OSecurity {

  private final YTDatabaseSessionInternal session;
  private final OSecurityInternal security;

  public OSecurityProxy(OSecurityInternal security, YTDatabaseSessionInternal session) {
    this.security = security;
    this.session = session;
  }

  @Override
  public boolean isAllowed(
      final Set<YTIdentifiable> iAllowAll, final Set<YTIdentifiable> iAllowOperation) {
    return security.isAllowed(session, iAllowAll, iAllowOperation);
  }

  @Override
  public YTIdentifiable allowUser(
      YTEntityImpl iDocument, ORestrictedOperation iOperationType, String iUserName) {
    return security.allowUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public YTIdentifiable allowRole(
      YTEntityImpl iDocument, ORestrictedOperation iOperationType, String iRoleName) {
    return security.allowRole(session, iDocument, iOperationType, iRoleName);
  }

  @Override
  public YTIdentifiable denyUser(
      YTEntityImpl iDocument, ORestrictedOperation iOperationType, String iUserName) {
    return security.denyUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public YTIdentifiable denyRole(
      YTEntityImpl iDocument, ORestrictedOperation iOperationType, String iRoleName) {
    return security.denyRole(session, iDocument, iOperationType, iRoleName);
  }

  public YTUser authenticate(final String iUsername, final String iUserPassword) {
    return security.authenticate(session, iUsername, iUserPassword);
  }

  public YTUser authenticate(final OToken authToken) {
    return security.authenticate(session, authToken);
  }

  public YTUser getUser(final String iUserName) {
    return security.getUser(session, iUserName);
  }

  public YTUser getUser(final YTRID iUserId) {
    return security.getUser(session, iUserId);
  }

  public YTUser createUser(
      final String iUserName, final String iUserPassword, final String... iRoles) {
    return security.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public YTUser createUser(
      final String iUserName, final String iUserPassword, final ORole... iRoles) {
    return security.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public ORole getRole(final String iRoleName) {
    return security.getRole(session, iRoleName);
  }

  public ORole getRole(final YTIdentifiable iRole) {
    return security.getRole(session, iRole);
  }

  public ORole createRole(final String iRoleName, final OSecurityRole.ALLOW_MODES iAllowMode) {
    return security.createRole(session, iRoleName, iAllowMode);
  }

  public ORole createRole(
      final String iRoleName, final ORole iParent, final OSecurityRole.ALLOW_MODES iAllowMode) {
    return security.createRole(session, iRoleName, iParent, iAllowMode);
  }

  public List<YTEntityImpl> getAllUsers() {
    return security.getAllUsers(session);
  }

  public List<YTEntityImpl> getAllRoles() {
    return security.getAllRoles(session);
  }

  public String toString() {
    return security.toString();
  }

  public boolean dropUser(final String iUserName) {
    return security.dropUser(session, iUserName);
  }

  public boolean dropRole(final String iRoleName) {
    return security.dropRole(session, iRoleName);
  }
}
