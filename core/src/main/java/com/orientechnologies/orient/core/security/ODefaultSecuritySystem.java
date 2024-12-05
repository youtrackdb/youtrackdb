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
package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDBInternal;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.YTImmutableUser;
import com.orientechnologies.orient.core.metadata.security.YTSecurityUser;
import com.orientechnologies.orient.core.metadata.security.YTSystemUser;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.security.authenticator.ODatabaseUserAuthenticator;
import com.orientechnologies.orient.core.security.authenticator.OServerConfigAuthenticator;
import com.orientechnologies.orient.core.security.authenticator.OSystemUserAuthenticator;
import com.orientechnologies.orient.core.security.authenticator.OTemporaryGlobalUser;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Provides an implementation of OServerSecurity.
 */
public class ODefaultSecuritySystem implements OSecuritySystem {

  private boolean enabled = false; // Defaults to not
  // enabled at
  // first.
  private boolean debug = false;

  private boolean storePasswords = true;

  // OServerSecurity (via OSecurityAuthenticator)
  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  private boolean allowDefault = true;

  private final Object passwordValidatorSynch = new Object();
  private OPasswordValidator passwordValidator;

  private final Object importLDAPSynch = new Object();
  private OSecurityComponent importLDAP;

  private final Object auditingSynch = new Object();
  private OAuditingService auditingService;

  private YTDocument configDoc; // Holds the
  // current JSON
  // configuration.
  private OSecurityConfig serverConfig;
  private YouTrackDBInternal context;

  private YTDocument auditingDoc;
  private YTDocument serverDoc;
  private YTDocument authDoc;
  private YTDocument passwdValDoc;
  private YTDocument ldapImportDoc;

  // We use a list because the order indicates priority of method.
  private volatile List<OSecurityAuthenticator> authenticatorsList;
  private volatile List<OSecurityAuthenticator> enabledAuthenticators;

  private final ConcurrentHashMap<String, Class<?>> securityClassMap =
      new ConcurrentHashMap<String, Class<?>>();
  private OTokenSign tokenSign;
  private final Map<String, OTemporaryGlobalUser> ephemeralUsers =
      new ConcurrentHashMap<String, OTemporaryGlobalUser>();

  private final Map<String, OGlobalUser> configUsers = new HashMap<String, OGlobalUser>();

  public ODefaultSecuritySystem() {
  }

  public void activate(final YouTrackDBInternal context,
      final OSecurityConfig serverCfg) {
    this.context = context;
    this.serverConfig = serverCfg;
    if (serverConfig != null) {
      this.load(serverConfig.getConfigurationFile());
    }
    onAfterDynamicPlugins(null);
    tokenSign = new OTokenSignImpl(context.getConfigurations().getConfigurations());
    for (OGlobalUser user : context.getConfigurations().getUsers()) {
      configUsers.put(user.getName(), user);
    }
  }

  public static void createSystemRoles(YTDatabaseSessionInternal session) {
    session.executeInTx(
        () -> {
          OSecurity security = session.getMetadata().getSecurity();
          if (security.getRole("root") == null) {
            ORole root = security.createRole("root", ORole.ALLOW_MODES.DENY_ALL_BUT);
            for (ORule.ResourceGeneric resource : ORule.ResourceGeneric.values()) {
              root.addRule(session, resource, null, ORole.PERMISSION_ALL);
            }
            // Do not allow root to have access to audit log class by default.
            root.addRule(session, ResourceGeneric.CLASS, "OAuditingLog", ORole.PERMISSION_NONE);
            root.addRule(session, ResourceGeneric.CLUSTER, "oauditinglog", ORole.PERMISSION_NONE);
            root.save(session);
          }
          if (security.getRole("guest") == null) {
            ORole guest = security.createRole("guest", ORole.ALLOW_MODES.DENY_ALL_BUT);
            guest.addRule(session, ResourceGeneric.SERVER, "listDatabases", ORole.PERMISSION_ALL);
            guest.save(session);
          }
          // for monitoring/logging purposes, intended to connect from external monitoring systems
          if (security.getRole("monitor") == null) {
            ORole guest = security.createRole("monitor", ORole.ALLOW_MODES.DENY_ALL_BUT);
            guest.addRule(session, ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
            guest.addRule(session, ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
            guest.addRule(session, ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_READ);
            guest.addRule(session, ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
            guest.addRule(session, ResourceGeneric.FUNCTION, null, ORole.PERMISSION_ALL);
            guest.addRule(session, ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
            guest.addRule(session, ResourceGeneric.COMMAND_GREMLIN, null, ORole.PERMISSION_ALL);
            guest.addRule(session, ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
            guest.addRule(session, ResourceGeneric.SERVER, null, ORole.PERMISSION_READ);
            guest.save(session);
          }
          // a separate role for accessing the auditing logs
          if (security.getRole("auditor") == null) {
            ORole auditor = security.createRole("auditor", OSecurityRole.ALLOW_MODES.DENY_ALL_BUT);
            auditor.addRule(session, ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
            auditor.addRule(session, ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
            auditor.addRule(session, ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
            auditor.addRule(session, ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
            auditor.addRule(session, ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
            auditor.addRule(session, ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
            auditor.addRule(session, ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
            auditor.addRule(session, ResourceGeneric.CLASS, "orole", ORole.PERMISSION_NONE);
            auditor.addRule(session, ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
            auditor.addRule(session, ResourceGeneric.CLASS, "OAuditingLog",
                ORole.PERMISSION_CREATE + ORole.PERMISSION_READ + ORole.PERMISSION_UPDATE);
            auditor.addRule(session,
                ResourceGeneric.CLUSTER,
                "oauditinglog",
                ORole.PERMISSION_CREATE + ORole.PERMISSION_READ + ORole.PERMISSION_UPDATE);
            auditor.save(session);
          }
        });
  }

  private void initDefultAuthenticators(YTDatabaseSessionInternal session) {
    OServerConfigAuthenticator serverAuth = new OServerConfigAuthenticator();
    serverAuth.config(session, null, this);

    ODatabaseUserAuthenticator databaseAuth = new ODatabaseUserAuthenticator();
    databaseAuth.config(session, null, this);

    OSystemUserAuthenticator systemAuth = new OSystemUserAuthenticator();
    systemAuth.config(session, null, this);

    List<OSecurityAuthenticator> authenticators = new ArrayList<OSecurityAuthenticator>();
    authenticators.add(serverAuth);
    authenticators.add(systemAuth);
    authenticators.add(databaseAuth);
    setAuthenticatorList(authenticators);
  }

  public void shutdown() {
    close();
  }

  private Class<?> getClass(final YTDocument jsonConfig) {
    Class<?> cls = null;

    try {
      if (jsonConfig.containsField("class")) {
        final String clsName = jsonConfig.field("class");

        if (securityClassMap.containsKey(clsName)) {
          cls = securityClassMap.get(clsName);
        } else {
          cls = Class.forName(clsName);
        }
      }
    } catch (Exception th) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getClass() Throwable: ", th);
    }

    return cls;
  }

  // OSecuritySystem (via OServerSecurity)
  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  public boolean isDefaultAllowed() {
    if (enabled) {
      return allowDefault;
    } else {
      return true; // If the security system is disabled return the original system default.
    }
  }

  @Override
  public YTSecurityUser authenticate(
      YTDatabaseSessionInternal session, OAuthenticationInfo authenticationInfo) {
    try {
      for (OSecurityAuthenticator sa : enabledAuthenticators) {
        YTSecurityUser principal = sa.authenticate(session, authenticationInfo);

        if (principal != null) {
          return principal;
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate()", ex);
    }

    return null; // Indicates authentication failed.
  }

  // OSecuritySystem (via OServerSecurity)
  public YTSecurityUser authenticate(
      YTDatabaseSessionInternal session, final String username, final String password) {
    try {
      // It's possible for the username to be null or an empty string in the case of SPNEGO
      // Kerberos
      // tickets.
      if (username != null && !username.isEmpty()) {
        if (debug) {
          OLogManager.instance()
              .info(
                  this,
                  "ODefaultServerSecurity.authenticate() ** Authenticating username: %s",
                  username);
        }
      }

      for (OSecurityAuthenticator sa : enabledAuthenticators) {
        YTSecurityUser principal = sa.authenticate(session, username, password);

        if (principal != null) {
          return principal;
        }
      }

    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate()", ex);
    }

    return null; // Indicates authentication failed.
  }

  public YTSecurityUser authenticateServerUser(YTDatabaseSessionInternal session,
      final String username,
      final String password) {
    YTSecurityUser user = getServerUser(session, username);

    if (user != null && user.getPassword(session) != null) {
      if (OSecurityManager.checkPassword(password, user.getPassword(session).trim())) {
        return user;
      }
    }
    return null;
  }

  public YouTrackDBInternal getContext() {
    return context;
  }

  // OSecuritySystem (via OServerSecurity)
  // Used for generating the appropriate HTTP authentication mechanism.
  public String getAuthenticationHeader(final String databaseName) {
    String header = null;

    // Default to Basic.
    if (databaseName != null) {
      header = "WWW-Authenticate: Basic realm=\"YouTrackDB db-" + databaseName + "\"";
    } else {
      header = "WWW-Authenticate: Basic realm=\"YouTrackDB Server\"";
    }

    if (enabled) {
      StringBuilder sb = new StringBuilder();

      // Walk through the list of OSecurityAuthenticators.
      for (OSecurityAuthenticator sa : enabledAuthenticators) {
        String sah = sa.getAuthenticationHeader(databaseName);

        if (sah != null && sah.trim().length() > 0) {
          // If we're not the first authenticator, then append "\n".
          if (sb.length() > 0) {
            sb.append("\r\n");
          }
          sb.append(sah);
        }
      }

      if (sb.length() > 0) {
        header = sb.toString();
      }
    }

    return header;
  }

  @Override
  public Map<String, String> getAuthenticationHeaders(String databaseName) {
    Map<String, String> headers = new HashMap<>();

    // Default to Basic.
    if (databaseName != null) {
      headers.put("WWW-Authenticate", "Basic realm=\"YouTrackDB db-" + databaseName + "\"");
    } else {
      headers.put("WWW-Authenticate", "Basic realm=\"YouTrackDB Server\"");
    }

    if (enabled) {

      // Walk through the list of OSecurityAuthenticators.
      for (OSecurityAuthenticator sa : enabledAuthenticators) {
        if (sa.isEnabled()) {
          Map<String, String> currentHeaders = sa.getAuthenticationHeaders(databaseName);
          currentHeaders.entrySet().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        }
      }
    }

    return headers;
  }

  // OSecuritySystem (via OServerSecurity)
  public YTDocument getConfig() {
    YTDocument jsonConfig = new YTDocument();

    try {
      jsonConfig.field("enabled", enabled);
      jsonConfig.field("debug", debug);

      if (serverDoc != null) {
        jsonConfig.field("server", serverDoc, YTType.EMBEDDED);
      }

      if (authDoc != null) {
        jsonConfig.field("authentication", authDoc, YTType.EMBEDDED);
      }

      if (passwdValDoc != null) {
        jsonConfig.field("passwordValidator", passwdValDoc, YTType.EMBEDDED);
      }

      if (ldapImportDoc != null) {
        jsonConfig.field("ldapImporter", ldapImportDoc, YTType.EMBEDDED);
      }

      if (auditingDoc != null) {
        jsonConfig.field("auditing", auditingDoc, YTType.EMBEDDED);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getConfig() Exception: %s", ex);
    }

    return jsonConfig;
  }

  // OSecuritySystem (via OServerSecurity)
  // public YTDocument getComponentConfig(final String name) { return getSection(name); }

  public YTDocument getComponentConfig(final String name) {
    if (name != null) {
      if (name.equalsIgnoreCase("auditing")) {
        return auditingDoc;
      } else if (name.equalsIgnoreCase("authentication")) {
        return authDoc;
      } else if (name.equalsIgnoreCase("ldapImporter")) {
        return ldapImportDoc;
      } else if (name.equalsIgnoreCase("passwordValidator")) {
        return passwdValDoc;
      } else if (name.equalsIgnoreCase("server")) {
        return serverDoc;
      }
    }

    return null;
  }

  // OServerSecurity

  /**
   * Returns the "System User" associated with 'username' from the system database. If not found,
   * returns null. dbName is used to filter the assigned roles. It may be null.
   */
  public YTSecurityUser getSystemUser(final String username, final String dbName) {
    // ** There are cases when we need to retrieve an OUser that is a system user.
    //  if (isEnabled() && !OSystemDatabase.SYSTEM_DB_NAME.equals(dbName)) {
    var systemDb = context.getSystemDatabase();
    if (context.getSystemDatabase().exists()) {
      return systemDb
          .execute(
              (resultset, session) -> {
                var sessionInternal = (YTDatabaseSessionInternal) session;
                if (resultset != null && resultset.hasNext()) {
                  return new YTImmutableUser(sessionInternal,
                      0,
                      new YTSystemUser(sessionInternal,
                          resultset.next().getElement().orElseThrow().getRecord(), dbName));
                }
                return null;
              },
              "select from OUser where name = ? limit 1 fetchplan roles:1",
              username);
    }
    return null;
  }

  // OSecuritySystem (via OServerSecurity)
  // This will first look for a user in the security.json "users" array and then check if a resource
  // matches.
  public boolean isAuthorized(YTDatabaseSessionInternal session, final String username,
      final String resource) {
    if (username == null || resource == null) {
      return false;
    }

    // Walk through the list of OSecurityAuthenticators.
    for (OSecurityAuthenticator sa : enabledAuthenticators) {
      if (sa.isAuthorized(session, username, resource)) {
        return true;
      }
    }
    return false;
  }

  public boolean isServerUserAuthorized(YTDatabaseSessionInternal session, final String username,
      final String resource) {
    final YTSecurityUser user = getServerUser(session, username);

    if (user != null) {
      // TODO: to verify if this logic match previous logic
      return user.checkIfAllowed(session, resource, ORole.PERMISSION_ALL) != null;
      /*
      if (user.getResources().equals("*"))
        // ACCESS TO ALL
        return true;

      String[] resourceParts = user.getResources().split(",");
      for (String r : resourceParts) if (r.equalsIgnoreCase(resource)) return true;
      */
    }
    return false;
  }

  // OSecuritySystem (via OServerSecurity)
  public boolean isEnabled() {
    return enabled;
  }

  // OSecuritySystem (via OServerSecurity)
  // Indicates if passwords should be stored for users.
  public boolean arePasswordsStored() {
    if (enabled) {
      return storePasswords;
    } else {
      return true; // If the security system is disabled return the original system default.
    }
  }

  // OSecuritySystem (via OServerSecurity)
  // Indicates if the primary security mechanism supports single sign-on.
  public boolean isSingleSignOnSupported() {
    if (enabled) {
      OSecurityAuthenticator priAuth = getPrimaryAuthenticator();

      if (priAuth != null) {
        return priAuth.isSingleSignOnSupported();
      }
    }

    return false;
  }

  // OSecuritySystem (via OServerSecurity)
  public void validatePassword(final String username, final String password)
      throws YTInvalidPasswordException {
    if (enabled) {
      synchronized (passwordValidatorSynch) {
        if (passwordValidator != null) {
          passwordValidator.validatePassword(username, password);
        }
      }
    }
  }

  /**
   * OServerSecurity Interface *
   */

  // OServerSecurity
  public OAuditingService getAuditing() {
    return auditingService;
  }

  // OServerSecurity
  public OSecurityAuthenticator getAuthenticator(final String authMethod) {
    for (OSecurityAuthenticator am : authenticatorsList) {
      // If authMethod is null or an empty string, then return the first OSecurityAuthenticator.
      if (authMethod == null || authMethod.isEmpty()) {
        return am;
      }

      if (am.getName() != null && am.getName().equalsIgnoreCase(authMethod)) {
        return am;
      }
    }

    return null;
  }

  // OServerSecurity
  // Returns the first OSecurityAuthenticator in the list.
  public OSecurityAuthenticator getPrimaryAuthenticator() {
    if (enabled) {
      List<OSecurityAuthenticator> auth = authenticatorsList;
      if (auth.size() > 0) {
        return auth.get(0);
      }
    }

    return null;
  }

  // OServerSecurity
  public YTSecurityUser getUser(final String username, YTDatabaseSessionInternal session) {
    YTSecurityUser userCfg = null;

    // Walk through the list of OSecurityAuthenticators.
    for (OSecurityAuthenticator sa : enabledAuthenticators) {
      userCfg = sa.getUser(username, session);
      if (userCfg != null) {
        break;
      }
    }

    return userCfg;
  }

  public YTSecurityUser getServerUser(YTDatabaseSessionInternal session, final String username) {
    YTSecurityUser systemUser = null;
    // This will throw an IllegalArgumentException if iUserName is null or empty.
    // However, a null or empty iUserName is possible with some security implementations.
    if (username != null && !username.isEmpty()) {
      OGlobalUser userCfg = configUsers.get(username);
      if (userCfg == null) {
        for (OTemporaryGlobalUser user : ephemeralUsers.values()) {
          if (username.equalsIgnoreCase(user.getName())) {
            // FOUND
            userCfg = user;
          }
        }
      }
      if (userCfg != null) {
        OSecurityRole role = OSecurityShared.createRole(userCfg);
        systemUser =
            new YTImmutableUser(session,
                username, userCfg.getPassword(), YTSecurityUser.SERVER_USER_TYPE, role);
      }
    }

    return systemUser;
  }

  @Override
  public OSyslog getSyslog() {
    return serverConfig.getSyslog();
  }

  // OSecuritySystem
  public void log(
      YTDatabaseSessionInternal session, final OAuditingOperation operation,
      final String dbName,
      YTSecurityUser user,
      final String message) {
    synchronized (auditingSynch) {
      if (auditingService != null) {
        auditingService.log(session, operation, dbName, user, message);
      }
    }
  }

  // OSecuritySystem
  public void registerSecurityClass(final Class<?> cls) {
    String fullTypeName = getFullTypeName(cls);

    if (fullTypeName != null) {
      securityClassMap.put(fullTypeName, cls);
    }
  }

  // OSecuritySystem
  public void unregisterSecurityClass(final Class<?> cls) {
    String fullTypeName = getFullTypeName(cls);

    if (fullTypeName != null) {
      securityClassMap.remove(fullTypeName);
    }
  }

  // Returns the package plus type name of Class.
  private static String getFullTypeName(Class<?> type) {
    String typeName = null;

    typeName = type.getSimpleName();

    Package pack = type.getPackage();

    if (pack != null) {
      typeName = pack.getName() + "." + typeName;
    }

    return typeName;
  }

  public void load(final String cfgPath) {
    this.configDoc = loadConfig(cfgPath);
  }

  // OSecuritySystem
  public void reload(YTDatabaseSessionInternal session, final YTDocument configDoc) {
    reload(session, null, configDoc);
  }

  @Override
  public void reload(YTDatabaseSessionInternal session, YTSecurityUser user, YTDocument configDoc) {
    if (configDoc != null) {
      close();

      this.configDoc = configDoc;

      onAfterDynamicPlugins(session, user);

      log(session,
          OAuditingOperation.RELOADEDSECURITY,
          null,
          user, "The security configuration file has been reloaded");
    } else {
      OLogManager.instance()
          .warn(
              this,
              "ODefaultServerSecurity.reload(YTDocument) The provided configuration document is"
                  + " null");
      throw new YTSecuritySystemException(
          "ODefaultServerSecurity.reload(YTDocument) The provided configuration document is null");
    }
  }

  public void reloadComponent(YTDatabaseSessionInternal session, YTSecurityUser user,
      final String name, final YTDocument jsonConfig) {
    if (name == null || name.isEmpty()) {
      throw new YTSecuritySystemException(
          "ODefaultServerSecurity.reloadComponent() name is null or empty");
    }
    if (jsonConfig == null) {
      throw new YTSecuritySystemException(
          "ODefaultServerSecurity.reloadComponent() Configuration document is null");
    }

    if (name.equalsIgnoreCase("auditing")) {
      auditingDoc = jsonConfig;
      reloadAuditingService(session);

    } else if (name.equalsIgnoreCase("authentication")) {
      authDoc = jsonConfig;
      reloadAuthMethods(session);

    } else if (name.equalsIgnoreCase("ldapImporter")) {
      ldapImportDoc = jsonConfig;
      reloadImportLDAP(session);
    } else if (name.equalsIgnoreCase("passwordValidator")) {
      passwdValDoc = jsonConfig;
      reloadPasswordValidator(session);
    } else if (name.equalsIgnoreCase("server")) {
      serverDoc = jsonConfig;
      reloadServer();
    }
    setSection(name, jsonConfig);

    log(session,
        OAuditingOperation.RELOADEDSECURITY,
        null,
        user, String.format("The %s security component has been reloaded", name));
  }

  private void loadAuthenticators(YTDatabaseSessionInternal session, final YTDocument authDoc) {
    if (authDoc.containsField("authenticators")) {
      List<OSecurityAuthenticator> autheticators = new ArrayList<OSecurityAuthenticator>();
      List<YTDocument> authMethodsList = authDoc.field("authenticators");

      for (YTDocument authMethodDoc : authMethodsList) {
        try {
          if (authMethodDoc.containsField("name")) {
            final String name = authMethodDoc.field("name");

            // defaults to enabled if "enabled" is missing
            boolean enabled = true;

            if (authMethodDoc.containsField("enabled")) {
              enabled = authMethodDoc.field("enabled");
            }

            if (enabled) {
              Class<?> authClass = getClass(authMethodDoc);

              if (authClass != null) {
                if (OSecurityAuthenticator.class.isAssignableFrom(authClass)) {
                  OSecurityAuthenticator authPlugin =
                      (OSecurityAuthenticator) authClass.newInstance();

                  authPlugin.config(session, authMethodDoc, this);
                  authPlugin.active();

                  autheticators.add(authPlugin);
                } else {
                  OLogManager.instance()
                      .error(
                          this,
                          "ODefaultServerSecurity.loadAuthenticators() class is not an"
                              + " OSecurityAuthenticator",
                          null);
                }
              } else {
                OLogManager.instance()
                    .error(
                        this,
                        "ODefaultServerSecurity.loadAuthenticators() authentication class is null"
                            + " for %s",
                        null,
                        name);
              }
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.loadAuthenticators() authentication object is missing"
                        + " name",
                    null);
          }
        } catch (Exception ex) {
          OLogManager.instance()
              .error(this, "ODefaultServerSecurity.loadAuthenticators() Exception: ", ex);
        }
      }
      if (isDefaultAllowed()) {
        autheticators.add(new ODatabaseUserAuthenticator());
      }
      setAuthenticatorList(autheticators);
    } else {
      initDefultAuthenticators(session);
    }
  }

  // OServerSecurity
  public void onAfterDynamicPlugins(YTDatabaseSessionInternal session) {
    onAfterDynamicPlugins(session, null);
  }

  @Override
  public void onAfterDynamicPlugins(YTDatabaseSessionInternal session, YTSecurityUser user) {
    if (configDoc != null) {
      loadComponents(session);

      if (enabled) {
        log(session, OAuditingOperation.SECURITY, null, user, "The security module is now loaded");
      }
    } else {
      initDefultAuthenticators(session);
      OLogManager.instance().debug(this, "onAfterDynamicPlugins() Configuration document is empty");
    }
  }

  protected void loadComponents(YTDatabaseSessionInternal session) {
    // Loads the top-level configuration properties ("enabled" and "debug").
    loadSecurity();

    if (enabled) {
      // Loads the "auditing" configuration properties.
      auditingDoc = getSection("auditing");
      reloadAuditingService(session);

      // Loads the "server" configuration properties.
      serverDoc = getSection("server");
      reloadServer();

      // Loads the "authentication" configuration properties.
      authDoc = getSection("authentication");
      reloadAuthMethods(session);

      // Loads the "passwordValidator" configuration properties.
      passwdValDoc = getSection("passwordValidator");
      reloadPasswordValidator(session);

      // Loads the "ldapImporter" configuration properties.
      ldapImportDoc = getSection("ldapImporter");
      reloadImportLDAP(session);
    }
  }

  // Returns a section of the JSON document configuration as an YTDocument if section is present.
  private YTDocument getSection(final String section) {
    YTDocument sectionDoc = null;

    try {
      if (configDoc != null) {
        if (configDoc.containsField(section)) {
          sectionDoc = configDoc.field(section);
        }
      } else {
        OLogManager.instance()
            .error(
                this,
                "ODefaultServerSecurity.getSection(%s) Configuration document is null",
                null,
                section);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getSection(%s)", ex, section);
    }

    return sectionDoc;
  }

  // Change the component section and save it to disk
  private void setSection(final String section, YTDocument sectionDoc) {

    YTDocument oldSection = getSection(section);
    try {
      if (configDoc != null) {

        configDoc.field(section, sectionDoc);
        String configFile =
            OSystemVariableResolver.resolveSystemVariables(
                "${YOU_TRACK_DB_HOME}/config/security.json");

        String ssf = YTGlobalConfiguration.SERVER_SECURITY_FILE.getValueAsString();
        if (ssf != null) {
          configFile = ssf;
        }

        File f = new File(configFile);
        OIOUtils.writeFile(f, configDoc.toJSON("prettyPrint"));
      }
    } catch (Exception ex) {
      configDoc.field(section, oldSection);
      OLogManager.instance().error(this, "ODefaultServerSecurity.setSection(%s)", ex, section);
    }
  }

  // "${YOU_TRACK_DB_HOME}/config/security.json"
  private YTDocument loadConfig(final String cfgPath) {
    YTDocument securityDoc = null;

    try {
      if (cfgPath != null) {
        // Default
        String jsonFile = OSystemVariableResolver.resolveSystemVariables(cfgPath);

        File file = new File(jsonFile);

        if (file.exists() && file.canRead()) {
          FileInputStream fis = null;

          try {
            fis = new FileInputStream(file);

            final byte[] buffer = new byte[(int) file.length()];
            fis.read(buffer);

            securityDoc = new YTDocument().fromJSON(new String(buffer), "noMap");
          } finally {
            if (fis != null) {
              fis.close();
            }
          }
        } else {
          if (file.exists()) {
            OLogManager.instance()
                .warn(this, "Could not read the security JSON file: %s", null, jsonFile);
          } else {
            if (file.exists()) {
              OLogManager.instance()
                  .warn(this, "Security JSON file: %s do not exists", null, jsonFile);
            }
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "error loading security config", ex);
    }

    return securityDoc;
  }

  private boolean isEnabled(final YTDocument sectionDoc) {
    boolean enabled = true;

    try {
      if (sectionDoc.containsField("enabled")) {
        enabled = sectionDoc.field("enabled");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.isEnabled()", ex);
    }

    return enabled;
  }

  private void loadSecurity() {
    try {
      enabled = false;

      if (configDoc != null) {
        if (configDoc.containsField("enabled")) {
          enabled = configDoc.field("enabled");
        }

        if (configDoc.containsField("debug")) {
          debug = configDoc.field("debug");
        }
      } else {
        OLogManager.instance()
            .debug(this, "ODefaultServerSecurity.loadSecurity() jsonConfig is null");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity()", ex);
    }
  }

  private void reloadServer() {
    try {
      storePasswords = true;

      if (serverDoc != null) {
        if (serverDoc.containsField("createDefaultUsers")) {
          YTGlobalConfiguration.CREATE_DEFAULT_USERS.setValue(
              serverDoc.field("createDefaultUsers"));
        }

        if (serverDoc.containsField("storePasswords")) {
          storePasswords = serverDoc.field("storePasswords");
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadServer()", ex);
    }
  }

  private void reloadAuthMethods(YTDatabaseSessionInternal session) {
    if (authDoc != null) {
      if (authDoc.containsField("allowDefault")) {
        allowDefault = authDoc.field("allowDefault");
      }

      loadAuthenticators(session, authDoc);
    }
  }

  private void reloadPasswordValidator(YTDatabaseSessionInternal session) {
    try {
      synchronized (passwordValidatorSynch) {
        if (passwdValDoc != null && isEnabled(passwdValDoc)) {

          if (passwordValidator != null) {
            passwordValidator.dispose();
            passwordValidator = null;
          }

          Class<?> cls = getClass(passwdValDoc);

          if (cls != null) {
            if (OPasswordValidator.class.isAssignableFrom(cls)) {
              passwordValidator = (OPasswordValidator) cls.newInstance();
              passwordValidator.config(session, passwdValDoc, this);
              passwordValidator.active();
            } else {
              OLogManager.instance()
                  .error(
                      this,
                      "ODefaultServerSecurity.reloadPasswordValidator() class is not an"
                          + " OPasswordValidator",
                      null);
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.reloadPasswordValidator() PasswordValidator class"
                        + " property is missing",
                    null);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadPasswordValidator()", ex);
    }
  }

  private void reloadImportLDAP(YTDatabaseSessionInternal session) {
    try {
      synchronized (importLDAPSynch) {
        if (importLDAP != null) {
          importLDAP.dispose();
          importLDAP = null;
        }

        if (ldapImportDoc != null && isEnabled(ldapImportDoc)) {
          Class<?> cls = getClass(ldapImportDoc);

          if (cls != null) {
            if (OSecurityComponent.class.isAssignableFrom(cls)) {
              importLDAP = (OSecurityComponent) cls.newInstance();
              importLDAP.config(session, ldapImportDoc, this);
              importLDAP.active();
            } else {
              OLogManager.instance()
                  .error(
                      this,
                      "ODefaultServerSecurity.reloadImportLDAP() class is not an"
                          + " OSecurityComponent",
                      null);
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.reloadImportLDAP() ImportLDAP class property is"
                        + " missing",
                    null);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP()", ex);
    }
  }

  private void reloadAuditingService(YTDatabaseSessionInternal session) {
    try {
      synchronized (auditingSynch) {
        if (auditingService != null) {
          auditingService.dispose();
          auditingService = null;
        }

        if (auditingDoc != null && isEnabled(auditingDoc)) {
          Class<?> cls = getClass(auditingDoc);

          if (cls != null) {
            if (OAuditingService.class.isAssignableFrom(cls)) {
              auditingService = (OAuditingService) cls.newInstance();
              auditingService.config(session, auditingDoc, this);
              auditingService.active();
            } else {
              OLogManager.instance()
                  .error(
                      this,
                      "ODefaultServerSecurity.reloadAuditingService() class is not an"
                          + " OAuditingService",
                      null);
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.reloadAuditingService() Auditing class property is"
                        + " missing",
                    null);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService()", ex);
    }
  }

  public void close() {
    if (enabled) {

      synchronized (importLDAPSynch) {
        if (importLDAP != null) {
          importLDAP.dispose();
          importLDAP = null;
        }
      }

      synchronized (passwordValidatorSynch) {
        if (passwordValidator != null) {
          passwordValidator.dispose();
          passwordValidator = null;
        }
      }

      synchronized (auditingSynch) {
        if (auditingService != null) {
          auditingService.dispose();
          auditingService = null;
        }
      }

      setAuthenticatorList(Collections.emptyList());

      enabled = false;
    }
  }

  @Override
  public YTSecurityUser authenticateAndAuthorize(
      YTDatabaseSessionInternal session, String iUserName, String iPassword,
      String iResourceToCheck) {
    // Returns the authenticated username, if successful, otherwise null.
    YTSecurityUser user = authenticate(null, iUserName, iPassword);

    // Authenticated, now see if the user is authorized.
    if (user != null) {
      if (isAuthorized(session, user.getName(session), iResourceToCheck)) {
        return user;
      }
    }
    return null;
  }

  public boolean existsUser(String user) {
    return configUsers.containsKey(user);
  }

  public void addTemporaryUser(String iName, String iPassword, String iPermissions) {
    OTemporaryGlobalUser userCfg = new OTemporaryGlobalUser(iName, iPassword, iPermissions);
    ephemeralUsers.put(iName, userCfg);
  }

  @Override
  public OSecurityInternal newSecurity(String database) {
    return new OSecurityShared(this);
  }

  public synchronized void setAuthenticatorList(List<OSecurityAuthenticator> authenticators) {
    if (authenticatorsList != null) {
      for (OSecurityAuthenticator sa : authenticatorsList) {
        sa.dispose();
      }
    }
    this.authenticatorsList = Collections.unmodifiableList(authenticators);
    this.enabledAuthenticators =
        Collections.unmodifiableList(
            authenticators.stream().filter((x) -> x.isEnabled()).collect(Collectors.toList()));
  }

  public synchronized List<OSecurityAuthenticator> getEnabledAuthenticators() {
    return enabledAuthenticators;
  }

  public synchronized List<OSecurityAuthenticator> getAuthenticatorsList() {
    return authenticatorsList;
  }

  public OTokenSign getTokenSign() {
    return tokenSign;
  }
}
