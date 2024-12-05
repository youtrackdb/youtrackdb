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

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.OScenarioThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.OSystemDatabase;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.OClassTrigger;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSecurityAccessException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.ONullOutputListener;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunction;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule.ResourceGeneric;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityRole.ALLOW_MODES;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser.STATUSES;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.OAuthenticationInfo;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.YTSequence;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.security.OGlobalUser;
import com.jetbrains.youtrack.db.internal.core.security.OSecuritySystem;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBooleanExpression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Shared security class. It's shared by all the database instances that point to the same storage.
 */
public class OSecurityShared implements OSecurityInternal {

  private static final String DEFAULT_WRITER_ROLE_NAME = "writer";

  private static final String DEFAULT_READER_ROLE_NAME = "reader";

  private final AtomicLong version = new AtomicLong();

  public static final String RESTRICTED_CLASSNAME = "ORestricted";
  public static final String IDENTITY_CLASSNAME = "OIdentity";

  /**
   * role name -> class name -> true: has some rules, ie. it's not all allowed
   */
  protected Map<String, Map<String, Boolean>> roleHasPredicateSecurityForClass;

  // used to avoid updating the above while the security schema is being created
  protected boolean skipRoleHasPredicateSecurityForClassUpdate = false;

  protected Map<String, Map<String, OBooleanExpression>> securityPredicateCache =
      new ConcurrentHashMap<>();

  /**
   * set of all the security resources defined on properties (used for optimizations)
   */
  protected Set<OSecurityResourceProperty> filteredProperties;

  private final OSecuritySystem security;

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_ALL_FIELD = ORestrictedOperation.ALLOW_ALL.getFieldName();

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_READ_FIELD = ORestrictedOperation.ALLOW_READ.getFieldName();

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_UPDATE_FIELD = ORestrictedOperation.ALLOW_UPDATE.getFieldName();

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_DELETE_FIELD = ORestrictedOperation.ALLOW_DELETE.getFieldName();

  public static final String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  public static final String ONCREATE_FIELD = "onCreate.fields";

  public static final Set<String> ALLOW_FIELDS =
      Collections.unmodifiableSet(
          new HashSet<>() {
            {
              add(ORestrictedOperation.ALLOW_ALL.getFieldName());
              add(ORestrictedOperation.ALLOW_READ.getFieldName());
              add(ORestrictedOperation.ALLOW_UPDATE.getFieldName());
              add(ORestrictedOperation.ALLOW_DELETE.getFieldName());
            }
          });

  public OSecurityShared(OSecuritySystem security) {
    this.security = security;
  }

  @Override
  public YTIdentifiable allowRole(
      final YTDatabaseSession session,
      final EntityImpl iDocument,
      final ORestrictedOperation iOperation,
      final String iRoleName) {
    return session.computeInTx(
        () -> {
          final YTRID role = getRoleRID(session, iRoleName);
          if (role == null) {
            throw new IllegalArgumentException("Role '" + iRoleName + "' not found");
          }

          return allowIdentity(session, iDocument, iOperation.getFieldName(), role);
        });
  }

  @Override
  public YTIdentifiable allowUser(
      final YTDatabaseSession session,
      final EntityImpl iDocument,
      final ORestrictedOperation iOperation,
      final String iUserName) {
    return session.computeInTx(
        () -> {
          final YTRID user = getUserRID(session, iUserName);
          if (user == null) {
            throw new IllegalArgumentException("User '" + iUserName + "' not found");
          }

          return allowIdentity(session, iDocument, iOperation.getFieldName(), user);
        });
  }

  public YTIdentifiable allowIdentity(
      final YTDatabaseSession session,
      final EntityImpl iDocument,
      final String iAllowFieldName,
      final YTIdentifiable iId) {
    return session.computeInTx(
        () -> {
          Set<YTIdentifiable> field = iDocument.field(iAllowFieldName);
          if (field == null) {
            field = new TrackedSet<>(iDocument);
            iDocument.field(iAllowFieldName, field);
          }
          field.add(iId);

          return iId;
        });
  }

  @Override
  public YTIdentifiable denyUser(
      final YTDatabaseSessionInternal session,
      final EntityImpl iDocument,
      final ORestrictedOperation iOperation,
      final String iUserName) {
    return session.computeInTx(
        () -> {
          final YTRID user = getUserRID(session, iUserName);
          if (user == null) {
            throw new IllegalArgumentException("User '" + iUserName + "' not found");
          }

          return disallowIdentity(session, iDocument, iOperation.getFieldName(), user);
        });
  }

  @Override
  public YTIdentifiable denyRole(
      final YTDatabaseSessionInternal session,
      final EntityImpl iDocument,
      final ORestrictedOperation iOperation,
      final String iRoleName) {
    return session.computeInTx(
        () -> {
          final YTRID role = getRoleRID(session, iRoleName);
          if (role == null) {
            throw new IllegalArgumentException("Role '" + iRoleName + "' not found");
          }

          return disallowIdentity(session, iDocument, iOperation.getFieldName(), role);
        });
  }

  public YTIdentifiable disallowIdentity(
      final YTDatabaseSessionInternal session,
      final EntityImpl iDocument,
      final String iAllowFieldName,
      final YTIdentifiable iId) {
    Set<YTIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field != null) {
      field.remove(iId);
    }
    return iId;
  }

  @Override
  public boolean isAllowed(
      final YTDatabaseSessionInternal session,
      final Set<YTIdentifiable> iAllowAll,
      final Set<YTIdentifiable> iAllowOperation) {
    if ((iAllowAll == null || iAllowAll.isEmpty())
        && (iAllowOperation == null || iAllowOperation.isEmpty()))
    // NO AUTHORIZATION: CAN'T ACCESS
    {
      return false;
    }

    final YTSecurityUser currentUser = session.getUser();
    if (currentUser != null) {
      // CHECK IF CURRENT USER IS ENLISTED
      if (iAllowAll == null
          || (iAllowAll != null && !iAllowAll.contains(currentUser.getIdentity(session)))) {
        // CHECK AGAINST SPECIFIC _ALLOW OPERATION
        if (iAllowOperation != null && iAllowOperation.contains(currentUser.getIdentity(session))) {
          return true;
        }

        // CHECK IF AT LEAST ONE OF THE USER'S ROLES IS ENLISTED
        for (OSecurityRole r : currentUser.getRoles()) {
          // CHECK AGAINST GENERIC _ALLOW
          if (iAllowAll != null && iAllowAll.contains(r.getIdentity(session))) {
            return true;
          }
          // CHECK AGAINST SPECIFIC _ALLOW OPERATION
          if (iAllowOperation != null && iAllowOperation.contains(r.getIdentity(session))) {
            return true;
          }
          // CHECK inherited permissions from parent roles, fixes #1980: Record Level Security:
          // permissions don't follow role's
          // inheritance
          OSecurityRole parentRole = r.getParentRole();
          while (parentRole != null) {
            if (iAllowAll != null && iAllowAll.contains(parentRole.getIdentity(session))) {
              return true;
            }
            if (iAllowOperation != null && iAllowOperation.contains(
                parentRole.getIdentity(session))) {
              return true;
            }
            parentRole = parentRole.getParentRole();
          }
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public YTSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, OAuthenticationInfo authenticationInfo) {
    YTSecurityUser user = null;
    final String dbName = session.getName();
    assert !session.isRemote();
    user = security.authenticate(session, authenticationInfo);

    if (user != null) {
      if (user.getAccountStatus(session) != YTSecurityUser.STATUSES.ACTIVE) {
        throw new YTSecurityAccessException(dbName,
            "User '" + user.getName(session) + "' is not active");
      }
    } else {
      // WAIT A BIT TO AVOID BRUTE FORCE
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }

      throw new YTSecurityAccessException(
          dbName, "Invalid authentication info for access to the database " + authenticationInfo);
    }
    return user;
  }

  @Override
  public YTSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, String userName, String password) {
    YTSecurityUser user;
    final String dbName = session.getName();
    assert !session.isRemote();
    user = security.authenticate(session, userName, password);

    if (user != null) {
      if (user.getAccountStatus(session) != YTSecurityUser.STATUSES.ACTIVE) {
        throw new YTSecurityAccessException(dbName,
            "User '" + user.getName(session) + "' is not active");
      }
    } else {
      // WAIT A BIT TO AVOID BRUTE FORCE
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }

      throw new YTSecurityAccessException(
          dbName,
          "User or password not valid for username: " + userName + ", database: '" + dbName + "'");
    }
    return user;
  }

  @Override
  public YTUser authenticate(
      YTDatabaseSessionInternal session, final String iUsername, final String iUserPassword) {
    return null;
  }

  // Token MUST be validated before being passed to this method.
  public YTUser authenticate(final YTDatabaseSessionInternal session, final OToken authToken) {
    final String dbName = session.getName();
    if (!authToken.getIsValid()) {
      throw new YTSecurityAccessException(dbName, "Token not valid");
    }

    YTUser user = authToken.getUser(session);
    if (user == null && authToken.getUserName() != null) {
      // Token handler may not support returning an OUser so let's get username (subject) and query:
      user = getUser(session, authToken.getUserName());
    }

    if (user == null) {
      throw new YTSecurityAccessException(
          dbName, "Authentication failed, could not load user from token");
    }
    if (user.getAccountStatus(session) != STATUSES.ACTIVE) {
      throw new YTSecurityAccessException(dbName,
          "User '" + user.getName(session) + "' is not active");
    }

    return user;
  }

  public YTUser getUser(final YTDatabaseSession session, final YTRID iRecordId) {
    if (iRecordId == null) {
      return null;
    }

    EntityImpl result;
    result = session.load(iRecordId);
    if (!result.getClassName().equals(YTUser.CLASS_NAME)) {
      result = null;
    }
    return new YTUser(session, result);
  }

  public YTUser createUser(
      final YTDatabaseSessionInternal session,
      final String iUserName,
      final String iUserPassword,
      final String... iRoles) {
    final YTUser user = new YTUser(session, iUserName, iUserPassword);

    if (iRoles != null) {
      for (String r : iRoles) {
        user.addRole(session, r);
      }
    }

    return user.save(session);
  }

  public YTUser createUser(
      final YTDatabaseSessionInternal session,
      final String userName,
      final String userPassword,
      final ORole... roles) {
    final YTUser user = new YTUser(session, userName, userPassword);

    if (roles != null) {
      for (ORole r : roles) {
        user.addRole(session, r);
      }
    }

    return user.save(session);
  }

  public boolean dropUser(final YTDatabaseSession session, final String iUserName) {
    final Number removed;
    try (YTResultSet res = session.command("delete from OUser where name = ?", iUserName)) {
      removed = res.next().getProperty("count");
    }

    return removed != null && removed.intValue() > 0;
  }

  public ORole getRole(final YTDatabaseSession session, final YTIdentifiable iRole) {
    try {
      final EntityImpl doc = iRole.getRecord();
      YTImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);

      if (clazz != null && clazz.isOrole()) {
        return new ORole(session, doc);
      }
    } catch (YTRecordNotFoundException rnf) {
      return null;
    }

    return null;
  }

  public ORole getRole(final YTDatabaseSession session, final String iRoleName) {
    if (iRoleName == null) {
      return null;
    }

    try (final YTResultSet result =
        session.query("select from ORole where name = ? limit 1", iRoleName)) {
      if (result.hasNext()) {
        return new ORole(session,
            (EntityImpl) result.next().getEntity().get());
      }
    }

    return null;
  }

  public static YTRID getRoleRID(final YTDatabaseSession session, final String iRoleName) {
    if (iRoleName == null) {
      return null;
    }

    try (final YTResultSet result =
        session.query("select @rid as rid from ORole where name = ? limit 1", iRoleName)) {

      if (result.hasNext()) {
        return result.next().getProperty("rid");
      }
    }
    return null;
  }

  public ORole createRole(
      final YTDatabaseSessionInternal session, final String iRoleName,
      final ALLOW_MODES iAllowMode) {
    return createRole(session, iRoleName, null, iAllowMode);
  }

  public ORole createRole(
      final YTDatabaseSessionInternal session,
      final String iRoleName,
      final ORole iParent,
      final ALLOW_MODES iAllowMode) {
    final ORole role = new ORole(session, iRoleName, iParent,
        iAllowMode);
    return role.save(session);
  }

  public boolean dropRole(final YTDatabaseSession session, final String iRoleName) {
    final Number removed;
    try (YTResultSet result =
        session.command("delete from ORole where name = '" + iRoleName + "'")) {
      removed = result.next().getProperty("count");
    }

    return removed != null && removed.intValue() > 0;
  }

  public List<EntityImpl> getAllUsers(final YTDatabaseSession session) {
    try (YTResultSet rs = session.query("select from OUser")) {
      return rs.stream().map((e) -> (EntityImpl) e.getEntity().get())
          .collect(Collectors.toList());
    }
  }

  public List<EntityImpl> getAllRoles(final YTDatabaseSession session) {
    try (YTResultSet rs = session.query("select from ORole")) {
      return rs.stream().map((e) -> (EntityImpl) e.getEntity().get())
          .collect(Collectors.toList());
    }
  }

  @Override
  public Map<String, OSecurityPolicy> getSecurityPolicies(
      YTDatabaseSession session, OSecurityRole role) {
    Map<String, OSecurityPolicy> result = role.getPolicies(session);
    return result != null ? result : Collections.emptyMap();
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(
      YTDatabaseSession session, OSecurityRole role, String resource) {
    resource = normalizeSecurityResource(session, resource);
    return role.getPolicy(session, resource);
  }

  public void setSecurityPolicyWithBitmask(
      YTDatabaseSessionInternal session, OSecurityRole role, String resource, int legacyPolicy) {
    String policyName = "default_" + legacyPolicy;
    OSecurityPolicyImpl policy = getSecurityPolicy(session, policyName);
    if (policy == null) {
      policy = createSecurityPolicy(session, policyName);
      policy.setCreateRule(session,
          (legacyPolicy & ORole.PERMISSION_CREATE) > 0 ? "true" : "false");
      policy.setReadRule(session, (legacyPolicy & ORole.PERMISSION_READ) > 0 ? "true" : "false");
      policy.setBeforeUpdateRule(session,
          (legacyPolicy & ORole.PERMISSION_UPDATE) > 0 ? "true" : "false");
      policy.setAfterUpdateRule(session,
          (legacyPolicy & ORole.PERMISSION_UPDATE) > 0 ? "true" : "false");
      policy.setDeleteRule(session,
          (legacyPolicy & ORole.PERMISSION_DELETE) > 0 ? "true" : "false");
      policy.setExecuteRule(session,
          (legacyPolicy & ORole.PERMISSION_EXECUTE) > 0 ? "true" : "false");
      saveSecurityPolicy(session, policy);
    }
    setSecurityPolicy(session, role, resource, policy);
  }

  @Override
  public void setSecurityPolicy(
      YTDatabaseSessionInternal session, OSecurityRole role, String resource,
      OSecurityPolicyImpl policy) {
    var currentResource = normalizeSecurityResource(session, resource);
    Entity roleDoc = session.load(role.getIdentity(session).getIdentity());
    validatePolicyWithIndexes(session, currentResource);
    Map<String, YTIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      policies = new HashMap<>();
      roleDoc.setProperty("policies", policies);
    }

    policies.put(currentResource, policy.getElement(session));
    session.save(roleDoc);
    if (session.getUser() != null && session.getUser()
        .hasRole(session, role.getName(session), true)) {
      session.reloadUser();
    }
    updateAllFilteredProperties(session);
    initPredicateSecurityOptimizations(session);
  }

  private static void validatePolicyWithIndexes(YTDatabaseSession session, String resource)
      throws IllegalArgumentException {
    OSecurityResource res = OSecurityResource.getInstance(resource);
    if (res instanceof OSecurityResourceProperty) {
      String clazzName = ((OSecurityResourceProperty) res).getClassName();
      YTClass clazz =
          ((YTDatabaseSessionInternal) session)
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClass(clazzName);
      if (clazz == null) {
        return;
      }
      Set<YTClass> allClasses = new HashSet<>();
      allClasses.add(clazz);
      allClasses.addAll(clazz.getAllSubclasses());
      allClasses.addAll(clazz.getAllSuperClasses());
      for (YTClass c : allClasses) {
        for (OIndex index : c.getIndexes(session)) {
          List<String> indexFields = index.getDefinition().getFields();
          if (indexFields.size() > 1
              && indexFields.contains(((OSecurityResourceProperty) res).getPropertyName())) {
            throw new IllegalArgumentException(
                "Cannot bind security policy on "
                    + resource
                    + " because of existing composite indexes: "
                    + index.getName());
          }
        }
      }
    }
  }

  @Override
  public OSecurityPolicyImpl createSecurityPolicy(YTDatabaseSession session, String name) {
    Entity elem = session.newEntity(OSecurityPolicy.class.getSimpleName());
    elem.setProperty("name", name);
    OSecurityPolicyImpl policy = new OSecurityPolicyImpl(elem);
    saveSecurityPolicy(session, policy);
    return policy;
  }

  @Override
  public OSecurityPolicyImpl getSecurityPolicy(YTDatabaseSession session, String name) {
    try (YTResultSet rs =
        session.query(
            "SELECT FROM " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", name)) {
      if (rs.hasNext()) {
        YTResult result = rs.next();
        return new OSecurityPolicyImpl(result.getEntity().get());
      }
    }
    return null;
  }

  @Override
  public void saveSecurityPolicy(YTDatabaseSession session, OSecurityPolicyImpl policy) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal
        .save(
            policy.getElement(sessionInternal),
            OSecurityPolicy.class.getSimpleName().toLowerCase(Locale.ENGLISH));
  }

  @Override
  public void deleteSecurityPolicy(YTDatabaseSession session, String name) {
    session
        .command("DELETE FROM " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", name)
        .close();
  }

  @Override
  public void removeSecurityPolicy(YTDatabaseSession session, ORole role, String resource) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    String calculatedResource = normalizeSecurityResource(session, resource);
    final Entity roleDoc = session.load(role.getIdentity(session).getIdentity());
    Map<String, YTIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      return;
    }
    policies.remove(calculatedResource);

    roleDoc.save();

    updateAllFilteredProperties(sessionInternal);
    initPredicateSecurityOptimizations(sessionInternal);
  }

  private static String normalizeSecurityResource(YTDatabaseSession session, String resource) {
    return resource; // TODO
  }

  public YTUser create(final YTDatabaseSessionInternal session) {
    if (!session.getMetadata().getSchema().getClasses().isEmpty()) {
      return null;
    }

    skipRoleHasPredicateSecurityForClassUpdate = true;
    YTUser adminUser = null;
    try {
      YTClass identityClass =
          session.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME); // SINCE 1.2.0
      if (identityClass == null) {
        identityClass = session.getMetadata().getSchema().createAbstractClass(OIdentity.CLASS_NAME);
      }

      createOrUpdateOSecurityPolicyClass(session);

      YTClass roleClass = createOrUpdateORoleClass(session, identityClass);

      createOrUpdateOUserClass(session, identityClass, roleClass);
      createOrUpdateORestrictedClass(session);

      if (!OSystemDatabase.SYSTEM_DB_NAME.equals(session.getName())) {
        // CREATE ROLES AND USERS
        createDefaultRoles(session);
        adminUser = createDefaultUsers(session);
      }

    } finally {
      skipRoleHasPredicateSecurityForClassUpdate = false;
    }
    initPredicateSecurityOptimizations(session);

    return adminUser;
  }

  private void createDefaultRoles(final YTDatabaseSessionInternal session) {
    session.executeInTx(
        () -> {
          createDefaultAdminRole(session);
          createDefaultReaderRole(session);
          createDefaultWriterRole(session);
        });
  }

  private YTUser createDefaultUsers(final YTDatabaseSessionInternal session) {
    boolean createDefUsers =
        session.getConfiguration().getValueAsBoolean(GlobalConfiguration.CREATE_DEFAULT_USERS);

    YTUser adminUser = null;
    // This will return the global value if a local storage context configuration value does not
    // exist.
    if (createDefUsers) {
      session.computeInTx(
          () -> {
            var admin = createUser(session, YTUser.ADMIN, YTUser.ADMIN, ORole.ADMIN);
            createUser(session, "reader", "reader", DEFAULT_READER_ROLE_NAME);
            createUser(session, "writer", "writer", DEFAULT_WRITER_ROLE_NAME);
            return admin;
          });
    }

    return adminUser;
  }

  private ORole createDefaultWriterRole(final YTDatabaseSessionInternal session) {
    final ORole writerRole =
        createRole(session, DEFAULT_WRITER_ROLE_NAME, ORole.ALLOW_MODES.DENY_ALL_BUT);
    sedDefaultWriterPermissions(session, writerRole);
    return writerRole;
  }

  private void sedDefaultWriterPermissions(final YTDatabaseSessionInternal session,
      final ORole writerRole) {
    setSecurityPolicyWithBitmask(session, writerRole, "database.class.*.*", ORole.PERMISSION_ALL);

    writerRole.addRule(session, ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    writerRole.addRule(session,
        ResourceGeneric.SCHEMA,
        null, ORole.PERMISSION_READ + ORole.PERMISSION_CREATE + ORole.PERMISSION_UPDATE);
    writerRole.addRule(session,
        ResourceGeneric.CLUSTER,
        OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    writerRole.addRule(session, ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL);
    writerRole.addRule(session, ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_READ);
    writerRole.addRule(session, ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL);
    writerRole.addRule(session, ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
    writerRole.addRule(session, ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_ALL);
    writerRole.addRule(session, ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    writerRole.addRule(session, ResourceGeneric.CLASS, YTSequence.CLASS_NAME,
        ORole.PERMISSION_READ);
    writerRole.addRule(session, ResourceGeneric.CLASS, "OTriggered", ORole.PERMISSION_READ);
    writerRole.addRule(session, ResourceGeneric.CLASS, "OSchedule", ORole.PERMISSION_READ);
    writerRole.addRule(session,
        ResourceGeneric.CLASS,
        OSecurityResource.class.getSimpleName(), ORole.PERMISSION_READ);
    writerRole.addRule(session, ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
    writerRole.save(session);

    setSecurityPolicyWithBitmask(
        session, writerRole, ORule.ResourceGeneric.DATABASE.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.SCHEMA.getLegacyName(),
        ORole.PERMISSION_READ + ORole.PERMISSION_CREATE + ORole.PERMISSION_UPDATE);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName()
            + "."
            + OMetadataDefault.CLUSTER_INTERNAL_NAME,
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".orole",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".ouser",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLASS.getLegacyName() + ".*",
        ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLASS.getLegacyName() + ".OUser",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".*",
        ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(
        session, writerRole, ORule.ResourceGeneric.COMMAND.getLegacyName(), ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.RECORD_HOOK.getLegacyName(),
        ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.FUNCTION.getLegacyName() + ".*",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.CLASS.getLegacyName() + "." + YTSequence.CLASS_NAME,
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName() + ".OTriggered",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName() + ".OSchedule",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        writerRole,
        ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName(),
        ORole.PERMISSION_NONE);
  }

  private ORole createDefaultReaderRole(final YTDatabaseSessionInternal session) {
    final ORole readerRole =
        createRole(session, DEFAULT_READER_ROLE_NAME, ORole.ALLOW_MODES.DENY_ALL_BUT);
    setDefaultReaderPermissions(session, readerRole);
    return readerRole;
  }

  private void setDefaultReaderPermissions(final YTDatabaseSessionInternal session,
      final ORole readerRole) {
    setSecurityPolicyWithBitmask(session, readerRole, "database.class.*.*", ORole.PERMISSION_ALL);

    readerRole.addRule(session, ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
    readerRole.addRule(session,
        ResourceGeneric.CLUSTER,
        OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
    readerRole.addRule(session, ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.COMMAND, null, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    readerRole.addRule(session, ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);

    readerRole.save(session);

    setSecurityPolicyWithBitmask(
        session, readerRole, ORule.ResourceGeneric.DATABASE.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session, readerRole, ORule.ResourceGeneric.SCHEMA.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName()
            + "."
            + OMetadataDefault.CLUSTER_INTERNAL_NAME,
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".orole",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".ouser",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.CLASS.getLegacyName() + ".*",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.CLASS.getLegacyName() + ".OUser",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".*",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session, readerRole, ORule.ResourceGeneric.COMMAND.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.RECORD_HOOK.getLegacyName(),
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.FUNCTION.getLegacyName() + ".*",
        ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(
        session,
        readerRole,
        ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName(),
        ORole.PERMISSION_NONE);
  }

  private ORole createDefaultAdminRole(final YTDatabaseSessionInternal session) {
    ORole adminRole;
    adminRole = createRole(session, ORole.ADMIN, ORole.ALLOW_MODES.DENY_ALL_BUT);
    setDefaultAdminPermissions(session, adminRole);
    return adminRole;
  }

  private void setDefaultAdminPermissions(final YTDatabaseSessionInternal session,
      ORole adminRole) {
    setSecurityPolicyWithBitmask(session, adminRole, "*", ORole.PERMISSION_ALL);
    adminRole.addRule(session, ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL)
        .save(session);
    adminRole.addRule(session, ResourceGeneric.ALL, null, ORole.PERMISSION_ALL).save(session);
    adminRole.addRule(session, ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL).save(session);
    adminRole.addRule(session, ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL).save(session);
    adminRole.addRule(session, ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_ALL)
        .save(session);
    adminRole.addRule(session, ResourceGeneric.DATABASE, null, ORole.PERMISSION_ALL).save(session);
    adminRole.addRule(session, ResourceGeneric.SCHEMA, null, ORole.PERMISSION_ALL).save(session);
    adminRole.addRule(session, ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL).save(session);
    adminRole.addRule(session, ResourceGeneric.COMMAND_GREMLIN, null, ORole.PERMISSION_ALL)
        .save(session);
    adminRole.addRule(session, ResourceGeneric.FUNCTION, null, ORole.PERMISSION_ALL).save(session);

    adminRole.save(session);
  }

  private void createOrUpdateORestrictedClass(final YTDatabaseSessionInternal database) {
    YTClass restrictedClass = database.getMetadata().getSchema().getClass(RESTRICTED_CLASSNAME);
    boolean unsafe = false;
    if (restrictedClass == null) {
      restrictedClass =
          database.getMetadata().getSchema().createAbstractClass(RESTRICTED_CLASSNAME);
      unsafe = true;
    }
    if (!restrictedClass.existsProperty(ALLOW_ALL_FIELD)) {
      restrictedClass.createProperty(database,
          ALLOW_ALL_FIELD,
          YTType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), unsafe);
    }
    if (!restrictedClass.existsProperty(ALLOW_READ_FIELD)) {
      restrictedClass.createProperty(database,
          ALLOW_READ_FIELD,
          YTType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), unsafe);
    }
    if (!restrictedClass.existsProperty(ALLOW_UPDATE_FIELD)) {
      restrictedClass.createProperty(database,
          ALLOW_UPDATE_FIELD,
          YTType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), unsafe);
    }
    if (!restrictedClass.existsProperty(ALLOW_DELETE_FIELD)) {
      restrictedClass.createProperty(database,
          ALLOW_DELETE_FIELD,
          YTType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), unsafe);
    }
  }

  private void createOrUpdateOUserClass(
      final YTDatabaseSessionInternal database, YTClass identityClass, YTClass roleClass) {
    boolean unsafe = false;
    YTClass userClass = database.getMetadata().getSchema().getClass("OUser");
    if (userClass == null) {
      userClass = database.getMetadata().getSchema().createClass("OUser", identityClass);
      unsafe = true;
    } else if (!userClass.getSuperClasses().contains(identityClass))
    // MIGRATE AUTOMATICALLY TO 1.2.0
    {
      userClass.setSuperClasses(database, Collections.singletonList(identityClass));
    }

    if (!userClass.existsProperty("name")) {
      userClass
          .createProperty(database, "name", YTType.STRING, (YTType) null, unsafe)
          .setMandatory(database, true)
          .setNotNull(database, true)
          .setCollate(database, "ci")
          .setMin(database, "1")
          .setRegexp(database, "\\S+(.*\\S+)*");
      userClass.createIndex(database, "OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE,
          "name");
    } else {
      final YTProperty name = userClass.getProperty("name");
      if (name.getAllIndexes(database).isEmpty()) {
        userClass.createIndex(database,
            "OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
      }
    }
    if (!userClass.existsProperty(YTUser.PASSWORD_FIELD)) {
      userClass
          .createProperty(database, YTUser.PASSWORD_FIELD, YTType.STRING, (YTType) null, unsafe)
          .setMandatory(database, true)
          .setNotNull(database, true);
    }
    if (!userClass.existsProperty("roles")) {
      userClass.createProperty(database, "roles", YTType.LINKSET, roleClass, unsafe);
    }
    if (!userClass.existsProperty("status")) {
      userClass
          .createProperty(database, "status", YTType.STRING, (YTType) null, unsafe)
          .setMandatory(database, true)
          .setNotNull(database, true);
    }
  }

  private static YTClass createOrUpdateOSecurityPolicyClass(
      final YTDatabaseSessionInternal database) {
    YTClass policyClass = database.getMetadata().getSchema().getClass("OSecurityPolicy");
    boolean unsafe = false;
    if (policyClass == null) {
      policyClass = database.getMetadata().getSchema().createClass("OSecurityPolicy");
      unsafe = true;
    }

    if (!policyClass.existsProperty("name")) {
      policyClass
          .createProperty(database, "name", YTType.STRING, (YTType) null, unsafe)
          .setMandatory(database, true)
          .setNotNull(database, true)
          .setCollate(database, "ci");
      policyClass.createIndex(database,
          "OSecurityPolicy.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final YTProperty name = policyClass.getProperty("name");
      if (name.getAllIndexes(database).isEmpty()) {
        policyClass.createIndex(database,
            "OSecurityPolicy.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
      }
    }

    if (!policyClass.existsProperty("create")) {
      policyClass.createProperty(database, "create", YTType.STRING, (YTType) null, unsafe);
    }
    if (!policyClass.existsProperty("read")) {
      policyClass.createProperty(database, "read", YTType.STRING, (YTType) null, unsafe);
    }
    if (!policyClass.existsProperty("beforeUpdate")) {
      policyClass.createProperty(database, "beforeUpdate", YTType.STRING, (YTType) null, unsafe);
    }
    if (!policyClass.existsProperty("afterUpdate")) {
      policyClass.createProperty(database, "afterUpdate", YTType.STRING, (YTType) null, unsafe);
    }
    if (!policyClass.existsProperty("delete")) {
      policyClass.createProperty(database, "delete", YTType.STRING, (YTType) null, unsafe);
    }
    if (!policyClass.existsProperty("execute")) {
      policyClass.createProperty(database, "execute", YTType.STRING, (YTType) null, unsafe);
    }

    if (!policyClass.existsProperty("active")) {
      policyClass.createProperty(database, "active", YTType.BOOLEAN, (YTType) null, unsafe);
    }

    return policyClass;
  }

  private static YTClass createOrUpdateORoleClass(final YTDatabaseSessionInternal database,
      YTClass identityClass) {
    YTClass roleClass = database.getMetadata().getSchema().getClass("ORole");
    boolean unsafe = false;
    if (roleClass == null) {
      roleClass = database.getMetadata().getSchema().createClass("ORole", identityClass);
      unsafe = true;
    } else if (!roleClass.getSuperClasses().contains(identityClass))
    // MIGRATE AUTOMATICALLY TO 1.2.0
    {
      roleClass.setSuperClasses(database, Collections.singletonList(identityClass));
    }

    if (!roleClass.existsProperty("name")) {
      roleClass
          .createProperty(database, "name", YTType.STRING, (YTType) null, unsafe)
          .setMandatory(database, true)
          .setNotNull(database, true)
          .setCollate(database, "ci");
      roleClass.createIndex(database, "ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE,
          "name");
    } else {
      final YTProperty name = roleClass.getProperty("name");
      if (name.getAllIndexes(database).isEmpty()) {
        roleClass.createIndex(database,
            "ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
      }
    }

    if (!roleClass.existsProperty("mode")) {
      roleClass.createProperty(database, "mode", YTType.BYTE, (YTType) null, unsafe);
    }

    if (!roleClass.existsProperty("rules")) {
      roleClass.createProperty(database, "rules", YTType.EMBEDDEDMAP, YTType.BYTE, unsafe);
    }
    if (!roleClass.existsProperty("inheritedRole")) {
      roleClass.createProperty(database, "inheritedRole", YTType.LINK, roleClass, unsafe);
    }

    if (!roleClass.existsProperty("policies")) {
      roleClass.createProperty(database,
          "policies", YTType.LINKMAP, database.getClass("OSecurityPolicy"), unsafe);
    }

    return roleClass;
  }

  public void load(YTDatabaseSessionInternal session) {
    var sessionInternal = session;
    final YTClass userClass = session.getMetadata().getSchema().getClass("OUser");
    if (userClass != null) {
      // @COMPATIBILITY <1.3.0
      if (!userClass.existsProperty("status")) {
        userClass.createProperty(session, "status", YTType.STRING).setMandatory(session, true)
            .setNotNull(session, true);
      }
      YTProperty p = userClass.getProperty("name");
      if (p == null) {
        p =
            userClass
                .createProperty(session, "name", YTType.STRING)
                .setMandatory(session, true)
                .setNotNull(session, true)
                .setMin(session, "1")
                .setRegexp(session, "\\S+(.*\\S+)*");
      }

      if (userClass.getInvolvedIndexes(session, "name") == null) {
        p.createIndex(session, INDEX_TYPE.UNIQUE);
      }

      // ROLE
      final YTClass roleClass = session.getMetadata().getSchema().getClass("ORole");

      final YTProperty rules = roleClass.getProperty("rules");
      if (rules != null && !YTType.EMBEDDEDMAP.equals(rules.getType())) {
        roleClass.dropProperty(session, "rules");
      }

      if (!roleClass.existsProperty("inheritedRole")) {
        roleClass.createProperty(session, "inheritedRole", YTType.LINK, roleClass);
      }

      p = roleClass.getProperty("name");
      if (p == null) {
        p = roleClass.createProperty(session, "name", YTType.STRING).
            setMandatory(session, true)
            .setNotNull(session, true);
      }

      if (roleClass.getInvolvedIndexes(session, "name") == null) {
        p.createIndex(session, INDEX_TYPE.UNIQUE);
      }

      // TODO migrate ORole to use security policies
    }

    setupPredicateSecurity(sessionInternal);
    initPredicateSecurityOptimizations(sessionInternal);
  }

  private void setupPredicateSecurity(YTDatabaseSessionInternal session) {
    YTClass securityPolicyClass = session.getMetadata().getSchema().getClass(OSecurityPolicy.class);
    if (securityPolicyClass == null) {
      createOrUpdateOSecurityPolicyClass(session);
      ORole adminRole = getRole(session, "admin");
      if (adminRole != null) {
        setDefaultAdminPermissions(session, adminRole);
      }
      ORole readerRole = getRole(session, DEFAULT_READER_ROLE_NAME);
      if (readerRole != null) {
        setDefaultReaderPermissions(session, readerRole);
      }

      ORole writerRole = getRole(session, DEFAULT_WRITER_ROLE_NAME);
      if (writerRole != null) {
        sedDefaultWriterPermissions(session, writerRole);
      }

      incrementVersion(session);
    }
  }

  public void createClassTrigger(YTDatabaseSessionInternal session) {
    YTClass classTrigger = session.getMetadata().getSchema().getClass(OClassTrigger.CLASSNAME);
    if (classTrigger == null) {
      session.getMetadata().getSchema().createAbstractClass(OClassTrigger.CLASSNAME);
    }
  }

  public static YTUser getUserInternal(final YTDatabaseSession session, final String iUserName) {
    return (YTUser)
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              try (YTResultSet result =
                  session.query("select from OUser where name = ? limit 1", iUserName)) {
                if (result.hasNext()) {
                  return new YTUser(session,
                      (EntityImpl) result.next().getEntity().get());
                }
              }
              return null;
            });
  }

  @Override
  public YTUser getUser(YTDatabaseSession session, String username) {
    return getUserInternal(session, username);
  }

  public static OSecurityRole createRole(OGlobalUser serverUser) {

    final OSecurityRole role;
    if (serverUser.getResources().equalsIgnoreCase("*")) {
      role = createRoot(serverUser);
    } else {
      Map<ResourceGeneric, ORule> permissions = mapPermission(serverUser);
      role = new OImmutableRole(null, serverUser.getName(), permissions, null);
    }

    return role;
  }

  private static OSecurityRole createRoot(OGlobalUser serverUser) {
    Map<String, OImmutableSecurityPolicy> policies = createrRootSecurityPolicy("*");
    Map<ORule.ResourceGeneric, ORule> rules = new HashMap<ORule.ResourceGeneric, ORule>();
    for (ORule.ResourceGeneric resource : ORule.ResourceGeneric.values()) {
      ORule rule = new ORule(resource, null, null);
      rule.grantAccess(null, ORole.PERMISSION_ALL);
      rules.put(resource, rule);
    }

    return new OImmutableRole(null, serverUser.getName(), rules, policies);
  }

  private static Map<ResourceGeneric, ORule> mapPermission(OGlobalUser user) {
    Map<ORule.ResourceGeneric, ORule> rules = new HashMap<ORule.ResourceGeneric, ORule>();
    String[] strings = user.getResources().split(",");

    for (String string : strings) {
      ORule.ResourceGeneric generic = ORule.mapLegacyResourceToGenericResource(string);
      if (generic != null) {
        ORule rule = new ORule(generic, null, null);
        rule.grantAccess(null, ORole.PERMISSION_ALL);
        rules.put(generic, rule);
      }
    }
    return rules;
  }

  public static Map<String, OImmutableSecurityPolicy> createrRootSecurityPolicy(String resource) {
    Map<String, OImmutableSecurityPolicy> policies =
        new HashMap<String, OImmutableSecurityPolicy>();
    policies.put(
        resource,
        new OImmutableSecurityPolicy(resource, "true", "true", "true", "true", "true", "true"));
    return policies;
  }

  public YTRID getUserRID(final YTDatabaseSession session, final String userName) {
    return (YTRID)
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              try (YTResultSet result =
                  session.query("select @rid as rid from OUser where name = ? limit 1", userName)) {

                if (result.hasNext()) {
                  return result.next().getProperty("rid");
                }
              }

              return null;
            });
  }

  @Override
  public void close() {
  }

  @Override
  public long getVersion(final YTDatabaseSession session) {
    return version.get();
  }

  @Override
  public void incrementVersion(final YTDatabaseSession session) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    version.incrementAndGet();
    securityPredicateCache.clear();
    updateAllFilteredProperties(sessionInternal);
    initPredicateSecurityOptimizations(sessionInternal);
  }

  protected void initPredicateSecurityOptimizations(YTDatabaseSessionInternal session) {
    if (skipRoleHasPredicateSecurityForClassUpdate) {
      return;
    }
    YTSecurityUser user = session.getUser();
    try {
      if (user != null) {
        session.setUser(null);
      }

      initPredicateSecurityOptimizationsInternal(session);
    } finally {

      if (user != null) {
        session.setUser(user);
      }
    }
  }

  private void initPredicateSecurityOptimizationsInternal(YTDatabaseSessionInternal session) {
    Map<String, Map<String, Boolean>> result = new HashMap<>();
    Collection<YTClass> allClasses = session.getMetadata().getSchema().getClasses();

    if (!session
        .getMetadata()
        .getImmutableSchemaSnapshot()
        .existsClass("ORole")) {
      return;
    }

    session.executeInTx(
        () -> {
          synchronized (this) {
            try (YTResultSet rs = session.query("select name, policies from ORole")) {
              while (rs.hasNext()) {
                YTResult item = rs.next();
                String roleName = item.getProperty("name");

                Map<String, YTIdentifiable> policies = item.getProperty("policies");
                if (policies != null) {
                  for (Map.Entry<String, YTIdentifiable> policyEntry : policies.entrySet()) {
                    OSecurityResource res = OSecurityResource.getInstance(policyEntry.getKey());
                    try {
                      Entity policy = policyEntry.getValue().getRecord();

                      for (YTClass clazz : allClasses) {
                        if (isClassInvolved(clazz, res)
                            && !isAllAllowed(
                            session,
                            new OImmutableSecurityPolicy(session,
                                new OSecurityPolicyImpl(policy)))) {
                          Map<String, Boolean> roleMap =
                              result.computeIfAbsent(roleName, k -> new HashMap<>());
                          roleMap.put(clazz.getName(), true);
                        }
                      }
                    } catch (YTRecordNotFoundException rne) {
                      // ignore
                    }
                  }
                }
              }
            }
            this.roleHasPredicateSecurityForClass = result;
          }
        });
  }

  private boolean isAllAllowed(YTDatabaseSessionInternal db, OSecurityPolicy policy) {
    for (OSecurityPolicy.Scope scope : OSecurityPolicy.Scope.values()) {
      String predicateString = policy.get(scope, db);
      if (predicateString == null) {
        continue;
      }
      OBooleanExpression predicate = OSecurityEngine.parsePredicate(db, predicateString);
      if (!predicate.isAlwaysTrue()) {
        return false;
      }
    }
    return true;
  }

  private boolean isClassInvolved(YTClass clazz, OSecurityResource res) {

    if (res instanceof OSecurityResourceAll
        || res.equals(OSecurityResourceClass.ALL_CLASSES)
        || res.equals(OSecurityResourceProperty.ALL_PROPERTIES)) {
      return true;
    }
    if (res instanceof OSecurityResourceClass) {
      String resourceClass = ((OSecurityResourceClass) res).getClassName();
      return clazz.isSubClassOf(resourceClass);
    } else if (res instanceof OSecurityResourceProperty) {
      String resourceClass = ((OSecurityResourceProperty) res).getClassName();
      return clazz.isSubClassOf(resourceClass);
    }
    return false;
  }

  @Override
  public Set<String> getFilteredProperties(YTDatabaseSessionInternal session,
      EntityImpl document) {
    if (session.getUser() == null) {
      return Collections.emptySet();
    }
    YTImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz == null) {
      return Collections.emptySet();
    }
    if (clazz.isSecurityPolicy()) {
      return Collections.emptySet();
    }

    if (roleHasPredicateSecurityForClass != null) {
      for (OSecurityRole role : session.getUser().getRoles()) {

        Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(role.getName(session));
        if (roleMap == null) {
          return Collections.emptySet(); // TODO hierarchy...?
        }
        Boolean val = roleMap.get(clazz.getName());
        if (!(Boolean.TRUE.equals(val))) {
          return Collections.emptySet(); // TODO hierarchy...?
        }
      }
    }
    Set<String> props = document.getPropertyNamesInternal();
    Set<String> result = new HashSet<>();

    var sessionInternal = session;
    for (String prop : props) {
      OBooleanExpression predicate =
          OSecurityEngine.getPredicateForSecurityResource(
              sessionInternal,
              this,
              "database.class.`" + clazz.getName() + "`.`" + prop + "`",
              OSecurityPolicy.Scope.READ);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, document)) {
        result.add(prop);
      }
    }
    return result;
  }

  @Override
  public boolean isAllowedWrite(YTDatabaseSessionInternal session, EntityImpl document,
      String propertyName) {

    if (session.getUser() == null) {
      // executeNoAuth
      return true;
    }

    String className;
    YTClass clazz = null;
    if (document instanceof EntityImpl) {
      className = document.getClassName();
    } else {
      clazz = document.getSchemaType().orElse(null);
      className = clazz == null ? null : clazz.getName();
    }
    if (className == null) {
      return true;
    }

    if (roleHasPredicateSecurityForClass != null) {
      for (OSecurityRole role : session.getUser().getRoles()) {
        Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(role.getName(session));
        if (roleMap == null) {
          return true; // TODO hierarchy...?
        }
        Boolean val = roleMap.get(className);
        if (!(Boolean.TRUE.equals(val))) {
          return true; // TODO hierarchy...?
        }
      }
    }

    var sessionInternal = session;
    if (document.getIdentity().isNew()) {
      OBooleanExpression predicate =
          OSecurityEngine.getPredicateForSecurityResource(
              sessionInternal,
              this,
              "database.class.`" + className + "`.`" + propertyName + "`",
              OSecurityPolicy.Scope.CREATE);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, document);
    } else {

      OBooleanExpression readPredicate =
          OSecurityEngine.getPredicateForSecurityResource(
              sessionInternal,
              this,
              "database.class.`" + className + "`.`" + propertyName + "`",
              OSecurityPolicy.Scope.READ);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, readPredicate, document)) {
        return false;
      }

      OBooleanExpression beforePredicate =
          OSecurityEngine.getPredicateForSecurityResource(
              sessionInternal,
              this,
              "database.class.`" + className + "`.`" + propertyName + "`",
              OSecurityPolicy.Scope.BEFORE_UPDATE);
      YTResultInternal originalRecord = calculateOriginalValue(document,
          session);

      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(
          session, beforePredicate, originalRecord)) {
        return false;
      }

      OBooleanExpression predicate =
          OSecurityEngine.getPredicateForSecurityResource(
              sessionInternal,
              this,
              "database.class.`" + className + "`.`" + propertyName + "`",
              OSecurityPolicy.Scope.AFTER_UPDATE);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, document);
    }
  }

  @Override
  public boolean canCreate(YTDatabaseSessionInternal session, Record record) {
    if (session.getUser() == null) {
      // executeNoAuth
      return true;
    }

    if (record instanceof Entity) {
      String className;
      if (record instanceof EntityImpl) {
        className = ((EntityImpl) record).getClassName();
      } else {
        className = ((Entity) record).getSchemaType().map(YTClass::getName).orElse(null);
      }

      if (roleHasPredicateSecurityForClass != null) {
        for (OSecurityRole role : session.getUser().getRoles()) {
          Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(
              role.getName(session));
          if (roleMap == null) {
            return true; // TODO hierarchy...?
          }
          Boolean val = roleMap.get(className);
          if (!(Boolean.TRUE.equals(val))) {
            return true; // TODO hierarchy...?
          }
        }
      }

      OBooleanExpression predicate;
      if (className == null) {
        predicate = null;
      } else {
        predicate =
            OSecurityEngine.getPredicateForSecurityResource(
                session, this, "database.class.`" + className + "`",
                OSecurityPolicy.Scope.CREATE);
      }
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  @Override
  public boolean canRead(YTDatabaseSessionInternal session, Record record) {
    // TODO what about server users?
    if (session.getUser() == null) {
      // executeNoAuth
      return true;
    }

    var sessionInternal = session;
    if (record instanceof Entity) {
      YTImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass((EntityImpl) record);
      if (clazz == null) {
        return true;
      }
      if (clazz.isSecurityPolicy()) {
        return true;
      }

      if (roleHasPredicateSecurityForClass != null) {
        for (OSecurityRole role : session.getUser().getRoles()) {
          Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(
              role.getName(session));
          if (roleMap == null) {
            return true; // TODO hierarchy...?
          }
          Boolean val = roleMap.get(((EntityImpl) record).getClassName());
          if (!(Boolean.TRUE.equals(val))) {
            return true; // TODO hierarchy...?
          }
        }
      }

      OBooleanExpression predicate =
          OSecurityEngine.getPredicateForSecurityResource(
              sessionInternal,
              this,
              "database.class.`" + ((EntityImpl) record).getClassName() + "`",
              OSecurityPolicy.Scope.READ);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  @Override
  public boolean canUpdate(YTDatabaseSessionInternal session, Record record) {
    if (session.getUser() == null) {
      // executeNoAuth
      return true;
    }
    if (record instanceof Entity) {

      String className;
      if (record instanceof EntityImpl) {
        className = ((EntityImpl) record).getClassName();
      } else {
        className = ((Entity) record).getSchemaType().map(x -> x.getName()).orElse(null);
      }

      if (className != null && roleHasPredicateSecurityForClass != null) {
        for (OSecurityRole role : session.getUser().getRoles()) {
          Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(
              role.getName(session));
          if (roleMap == null) {
            return true; // TODO hierarchy...?
          }
          Boolean val = roleMap.get(className);
          if (!(Boolean.TRUE.equals(val))) {
            return true; // TODO hierarchy...?
          }
        }
      }

      var sessionInternal = session;
      OBooleanExpression beforePredicate = null;
      if (className != null) {
        beforePredicate =
            OSecurityEngine.getPredicateForSecurityResource(
                sessionInternal,
                this,
                "database.class.`" + className + "`",
                OSecurityPolicy.Scope.BEFORE_UPDATE);
      }

      // TODO avoid calculating original valueif not needed!!!

      YTResultInternal originalRecord = calculateOriginalValue(record, sessionInternal);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(
          session, beforePredicate, originalRecord)) {
        return false;
      }

      OBooleanExpression predicate = null;
      if (className != null) {
        predicate =
            OSecurityEngine.getPredicateForSecurityResource(
                sessionInternal,
                this,
                "database.class.`" + className + "`",
                OSecurityPolicy.Scope.AFTER_UPDATE);
      }
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  private YTResultInternal calculateOriginalValue(Record record, YTDatabaseSessionInternal db) {
    return calculateBefore(record.getRecord(), db);
  }

  public static YTResultInternal calculateBefore(EntityImpl iDocument,
      YTDatabaseSessionInternal db) {
    // iDocument = db.load(iDocument.getIdentity(), null, true);
    YTResultInternal result = new YTResultInternal(db);
    for (String prop : iDocument.getPropertyNamesInternal()) {
      result.setProperty(prop, unboxRidbags(iDocument.getProperty(prop)));
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion());
    for (String prop : iDocument.getDirtyFields()) {
      result.setProperty(prop, convert(iDocument.getOriginalValue(prop)));
    }
    return result;
  }

  private static Object convert(Object originalValue) {
    if (originalValue instanceof RidBag) {
      Set result = new LinkedHashSet<>();
      ((RidBag) originalValue).iterator().forEachRemaining(result::add);
      return result;
    }
    return originalValue;
  }

  public static Object unboxRidbags(Object value) {
    // TODO move it to some helper class
    if (value instanceof RidBag) {
      List<YTIdentifiable> result = new ArrayList<>(((RidBag) value).size());
      for (YTIdentifiable identifiable : (RidBag) value) {
        result.add(identifiable);
      }

      return result;
    }
    return value;
  }

  @Override
  public boolean canDelete(YTDatabaseSessionInternal session, Record record) {
    if (session.getUser() == null) {
      // executeNoAuth
      return true;
    }

    if (record instanceof Entity) {
      String className;
      if (record instanceof EntityImpl) {
        className = ((EntityImpl) record).getClassName();
      } else {
        className = ((Entity) record).getSchemaType().map(YTClass::getName).orElse(null);
      }

      if (roleHasPredicateSecurityForClass != null) {
        for (OSecurityRole role : session.getUser().getRoles()) {
          Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(
              role.getName(session));
          if (roleMap == null) {
            return true; // TODO hierarchy...?
          }
          Boolean val = roleMap.get(className);
          if (!(Boolean.TRUE.equals(val))) {
            return true; // TODO hierarchy...?
          }
        }
      }

      OBooleanExpression predicate = null;
      if (className != null) {
        predicate =
            OSecurityEngine.getPredicateForSecurityResource(
                session, this, "database.class.`" + className + "`",
                OSecurityPolicy.Scope.DELETE);
      }

      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  @Override
  public boolean canExecute(YTDatabaseSessionInternal session, OFunction function) {
    if (session.getUser() == null) {
      // executeNoAuth
      return true;
    }

    OBooleanExpression predicate =
        OSecurityEngine.getPredicateForSecurityResource(
            session,
            this,
            "database.function." + function.getName(session),
            OSecurityPolicy.Scope.EXECUTE);
    return OSecurityEngine.evaluateSecuirtyPolicyPredicate(
        session, predicate, function.getDocument(session));
  }

  protected OBooleanExpression getPredicateFromCache(String roleName, String key) {
    Map<String, OBooleanExpression> roleMap = this.securityPredicateCache.get(roleName);
    if (roleMap == null) {
      return null;
    }
    OBooleanExpression result = roleMap.get(key.toLowerCase(Locale.ENGLISH));
    if (result != null) {
      return result.copy();
    }
    return null;
  }

  protected void putPredicateInCache(YTDatabaseSessionInternal session, String roleName, String key,
      OBooleanExpression predicate) {
    if (predicate.isCacheable(session)) {
      Map<String, OBooleanExpression> roleMap = this.securityPredicateCache.get(roleName);
      if (roleMap == null) {
        roleMap = new ConcurrentHashMap<>();
        this.securityPredicateCache.put(roleName, roleMap);
      }

      roleMap.put(key.toLowerCase(Locale.ENGLISH), predicate);
    }
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(YTDatabaseSession session, String resource) {
    if (session.getUser() == null) {
      // executeNoAuth
      return false;
    }

    var sessionInternal = (YTDatabaseSessionInternal) session;
    OBooleanExpression predicate =
        OSecurityEngine.getPredicateForSecurityResource(
            sessionInternal, this, resource, OSecurityPolicy.Scope.READ);
    return predicate != null && !OBooleanExpression.TRUE.equals(predicate);
  }

  @Override
  public synchronized Set<OSecurityResourceProperty> getAllFilteredProperties(
      YTDatabaseSessionInternal database) {
    if (filteredProperties == null) {
      updateAllFilteredProperties(database);
    }
    if (filteredProperties == null) {
      return Collections.emptySet();
    }
    return new HashSet<>(filteredProperties);
  }

  protected void updateAllFilteredProperties(YTDatabaseSessionInternal session) {
    Set<OSecurityResourceProperty> result;
    if (session.getUser() == null) {
      result = calculateAllFilteredProperties(session);
      synchronized (this) {
        filteredProperties = result;
      }

    } else {
      synchronized (this) {
        if (filteredProperties == null) {
          filteredProperties = new HashSet<>();
        }
        updateAllFilteredPropertiesInternal(session);
      }
    }
  }

  protected void updateAllFilteredPropertiesInternal(YTDatabaseSessionInternal session) {
    YTSecurityUser user = session.getUser();
    try {
      if (user != null) {
        session.setUser(null);
      }

      synchronized (OSecurityShared.this) {
        filteredProperties.clear();
        filteredProperties.addAll(calculateAllFilteredProperties(session));
      }
    } finally {
      if (user != null) {
        session.setUser(user);
      }
    }
  }

  protected Set<OSecurityResourceProperty> calculateAllFilteredProperties(
      YTDatabaseSessionInternal session) {
    Set<OSecurityResourceProperty> result = new HashSet<>();
    if (!session
        .getMetadata()
        .getImmutableSchemaSnapshot()
        .existsClass("ORole")) {
      return Collections.emptySet();
    }
    try (YTResultSet rs = session.query("select policies from ORole")) {
      while (rs.hasNext()) {
        YTResult item = rs.next();
        Map<String, YTIdentifiable> policies = item.getProperty("policies");
        if (policies != null) {
          for (Map.Entry<String, YTIdentifiable> policyEntry : policies.entrySet()) {
            try {
              OSecurityResource res = OSecurityResource.getInstance(policyEntry.getKey());
              if (res instanceof OSecurityResourceProperty) {
                final Entity element = policyEntry.getValue().getRecord();
                final OSecurityPolicy policy =
                    new OImmutableSecurityPolicy(session, new OSecurityPolicyImpl(element));
                final String readRule = policy.getReadRule(session);
                if (readRule != null && !readRule.trim().equalsIgnoreCase("true")) {
                  result.add((OSecurityResourceProperty) res);
                }
              }
            } catch (YTRecordNotFoundException e) {
              // ignore
            } catch (Exception e) {
              OLogManager.instance().error(this, "Error on loading security policy", e);
            }
          }
        }
      }
    }
    return result;
  }

  public boolean couldHaveActivePredicateSecurityRoles(YTDatabaseSession session,
      String className) {
    if (session.getUser() == null) {
      return false;
    }
    if (roleHasPredicateSecurityForClass != null) {
      for (OSecurityRole role : session.getUser().getRoles()) {
        Map<String, Boolean> roleMap = roleHasPredicateSecurityForClass.get(role.getName(session));
        if (roleMap == null) {
          return false; // TODO hierarchy...?
        }
        Boolean val = roleMap.get(className);
        if (Boolean.TRUE.equals(val)) {
          return true; // TODO hierarchy...?
        }
      }

      return false;
    }
    return true;
  }
}
