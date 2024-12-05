/*
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
package com.jetbrains.youtrack.db.internal.core.security;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.OAuthenticationInfo;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a basic interface for a modular security system.
 */
public interface OSecuritySystem {

  void shutdown();

  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  boolean isDefaultAllowed();

  // Returns the actual username if successful, null otherwise.
  // Some token-based authentication (e.g., SPNEGO tokens have the user's name embedded in the
  // service ticket).
  YTSecurityUser authenticate(
      YTDatabaseSessionInternal session, final String username, final String password);

  // Used for generating the appropriate HTTP authentication mechanism. The chain of authenticators
  // is used for this.
  String getAuthenticationHeader(final String databaseName);

  default Map<String, String> getAuthenticationHeaders(final String databaseName) {
    return new HashMap<>();
  }

  EntityImpl getConfig();

  EntityImpl getComponentConfig(final String name);

  /**
   * Returns the "System User" associated with 'username' from the system database. If not found,
   * returns null. dbName is used to filter the assigned roles. It may be null.
   */
  YTSecurityUser getSystemUser(final String username, final String dbName);

  // Walks through the list of Authenticators.
  boolean isAuthorized(YTDatabaseSessionInternal session, final String username,
      final String resource);

  boolean isEnabled();

  // Indicates if passwords should be stored when creating new users.
  boolean arePasswordsStored();

  // Indicates if the primary security mechanism supports single sign-on.
  boolean isSingleSignOnSupported();

  /**
   * Logs to the auditing service, if installed.
   *
   * @param session
   * @param dbName  May be null or empty.
   * @param user    May be null or empty.
   */
  void log(
      YTDatabaseSessionInternal session, final OAuditingOperation operation,
      final String dbName,
      YTSecurityUser user,
      final String message);

  void registerSecurityClass(final Class<?> cls);

  void reload(YTDatabaseSessionInternal session, final EntityImpl jsonConfig);

  void reload(YTDatabaseSessionInternal session, YTSecurityUser user,
      final EntityImpl jsonConfig);

  void reloadComponent(YTDatabaseSessionInternal session, YTSecurityUser user, final String name,
      final EntityImpl jsonConfig);

  void unregisterSecurityClass(final Class<?> cls);

  // If a password validator is registered with the security system, it will be called to validate
  // the specified password. An YTInvalidPasswordException is thrown if the password does not meet
  // the password validator's requirements.
  void validatePassword(final String username, final String password)
      throws YTInvalidPasswordException;

  OAuditingService getAuditing();

  /**
   * Returns the authenticator based on name, if one exists.
   */
  OSecurityAuthenticator getAuthenticator(final String authName);

  /**
   * Returns the first authenticator in the list, which is the primary authenticator.
   */
  OSecurityAuthenticator getPrimaryAuthenticator();

  OSyslog getSyslog();

  /**
   * Some authenticators support maintaining a list of users and associated resources (and sometimes
   * passwords).
   */
  YTSecurityUser getUser(final String username, YTDatabaseSessionInternal session);

  void onAfterDynamicPlugins(YTDatabaseSessionInternal session);

  default void onAfterDynamicPlugins(YTDatabaseSessionInternal session, YTSecurityUser user) {
    onAfterDynamicPlugins(session);
  }

  YTSecurityUser authenticateAndAuthorize(
      YTDatabaseSessionInternal session, String iUserName, String iPassword,
      String iResourceToCheck);

  YTSecurityUser authenticateServerUser(YTDatabaseSessionInternal session, String username,
      String password);

  YTSecurityUser getServerUser(YTDatabaseSessionInternal session, String username);

  boolean isServerUserAuthorized(YTDatabaseSessionInternal session, String username,
      String resource);

  YouTrackDBInternal getContext();

  boolean existsUser(String defaultRootUser);

  void addTemporaryUser(String user, String password, String resources);

  OSecurityInternal newSecurity(String database);

  YTSecurityUser authenticate(YTDatabaseSessionInternal session,
      OAuthenticationInfo authenticationInfo);

  OTokenSign getTokenSign();
}
