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

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.List;
import java.util.Set;

/**
 * Manages users and roles.
 */
public interface OSecurity {

  String RESTRICTED_CLASSNAME = "ORestricted";
  @Deprecated
  String IDENTITY_CLASSNAME = OIdentity.CLASS_NAME;
  String ALLOW_ALL_FIELD = "_allow";
  String ALLOW_READ_FIELD = "_allowRead";
  String ALLOW_UPDATE_FIELD = "_allowUpdate";
  String ALLOW_DELETE_FIELD = "_allowDelete";
  String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  String ONCREATE_FIELD = "onCreate.fields";

  @Deprecated
  boolean isAllowed(final Set<YTIdentifiable> iAllowAll, final Set<YTIdentifiable> iAllowOperation);

  /**
   * Record level security: allows a user to access to a record.
   *
   * @param iDocument      YTEntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to allow:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iUserName      User name to provide the access
   * @return The OIdentity instance allowed
   */
  @Deprecated
  YTIdentifiable allowUser(
      final YTEntityImpl iDocument, final ORestrictedOperation iOperationType,
      final String iUserName);

  /**
   * Record level security: allows a role to access to a record.
   *
   * @param iDocument      YTEntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to allow:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iRoleName      Role name to provide the access
   * @return The OIdentity instance allowed
   */
  @Deprecated
  YTIdentifiable allowRole(
      final YTEntityImpl iDocument, final ORestrictedOperation iOperationType,
      final String iRoleName);

  /**
   * Record level security: deny a user to access to a record.
   *
   * @param iDocument      YTEntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to deny:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iUserName      User name to deny the access
   * @return The OIdentity instance denied
   */
  @Deprecated
  YTIdentifiable denyUser(
      final YTEntityImpl iDocument, final ORestrictedOperation iOperationType,
      final String iUserName);

  /**
   * Record level security: deny a role to access to a record.
   *
   * @param iDocument      YTEntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to deny:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iRoleName      Role name to deny the access
   * @return The OIdentity instance denied
   */
  @Deprecated
  YTIdentifiable denyRole(
      final YTEntityImpl iDocument, final ORestrictedOperation iOperationType,
      final String iRoleName);

  @Deprecated
  YTUser authenticate(String iUsername, String iUserPassword);

  @Deprecated
  YTUser authenticate(final OToken authToken);

  YTUser getUser(String iUserName);

  YTUser getUser(final YTRID iUserId);

  YTUser createUser(String iUserName, String iUserPassword, String... iRoles);

  YTUser createUser(String iUserName, String iUserPassword, ORole... iRoles);

  boolean dropUser(String iUserName);

  ORole getRole(String iRoleName);

  ORole getRole(YTIdentifiable role);

  ORole createRole(String iRoleName, ORole.ALLOW_MODES iAllowMode);

  ORole createRole(String iRoleName, ORole iParent, ORole.ALLOW_MODES iAllowMode);

  boolean dropRole(String iRoleName);

  List<YTEntityImpl> getAllUsers();

  List<YTEntityImpl> getAllRoles();
}
