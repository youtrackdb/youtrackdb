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
package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import java.util.Set;

/**
 * Manages users and roles.
 */
public interface Security {

  String RESTRICTED_CLASSNAME = "ORestricted";
  @Deprecated
  String IDENTITY_CLASSNAME = Identity.CLASS_NAME;
  String ALLOW_ALL_FIELD = "_allow";
  String ALLOW_READ_FIELD = "_allowRead";
  String ALLOW_UPDATE_FIELD = "_allowUpdate";
  String ALLOW_DELETE_FIELD = "_allowDelete";
  String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  String ONCREATE_FIELD = "onCreate.fields";

  @Deprecated
  boolean isAllowed(final Set<Identifiable> iAllowAll, final Set<Identifiable> iAllowOperation);

  /**
   * Record level security: allows a user to access to a record.
   *
   * @param entity      EntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to allow:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iUserName      User name to provide the access
   * @return The Identity instance allowed
   */
  @Deprecated
  Identifiable allowUser(
      final EntityImpl entity, final RestrictedOperation iOperationType,
      final String iUserName);

  /**
   * Record level security: allows a role to access to a record.
   *
   * @param entity      EntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to allow:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iRoleName      Role name to provide the access
   * @return The Identity instance allowed
   */
  @Deprecated
  Identifiable allowRole(
      final EntityImpl entity, final RestrictedOperation iOperationType,
      final String iRoleName);

  /**
   * Record level security: deny a user to access to a record.
   *
   * @param entity      EntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to deny:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iUserName      User name to deny the access
   * @return The Identity instance denied
   */
  @Deprecated
  Identifiable denyUser(
      final EntityImpl entity, final RestrictedOperation iOperationType,
      final String iUserName);

  /**
   * Record level security: deny a role to access to a record.
   *
   * @param entity      EntityImpl instance to give access
   * @param iOperationType Operation type to use based on the permission to deny:
   *                       <ul>
   *                         <li>ALLOW_ALL, to provide full access (RUD)
   *                         <li>ALLOW_READ, to provide read access
   *                         <li>ALLOW_UPDATE, to provide update access
   *                         <li>ALLOW_DELETE, to provide delete access
   *                       </ul>
   * @param iRoleName      Role name to deny the access
   * @return The Identity instance denied
   */
  @Deprecated
  Identifiable denyRole(
      final EntityImpl entity, final RestrictedOperation iOperationType,
      final String iRoleName);

  @Deprecated
  SecurityUserIml authenticate(String iUsername, String iUserPassword);

  @Deprecated
  SecurityUserIml authenticate(final Token authToken);

  SecurityUserIml getUser(String iUserName);

  SecurityUserIml getUser(final RID iUserId);

  SecurityUserIml createUser(String iUserName, String iUserPassword, String... iRoles);

  SecurityUserIml createUser(String iUserName, String iUserPassword, Role... iRoles);

  boolean dropUser(String iUserName);

  Role getRole(String iRoleName);

  Role getRole(Identifiable role);

  Role createRole(String iRoleName, Role.ALLOW_MODES iAllowMode);

  Role createRole(String iRoleName, Role iParent, Role.ALLOW_MODES iAllowMode);

  boolean dropRole(String iRoleName);

  List<EntityImpl> getAllUsers();

  List<EntityImpl> getAllRoles();
}
