package com.jetbrains.youtrack.db.internal.client.remote.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.RestrictedOperation;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicy;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityResourceProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityRole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityRole.ALLOW_MODES;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Token;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserIml;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.AuthenticationInfo;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SecurityRemote implements SecurityInternal {

  public SecurityRemote() {
  }

  @Override
  public boolean isAllowed(
      DatabaseSessionInternal session, Set<Identifiable> iAllowAll,
      Set<Identifiable> iAllowOperation) {
    return true;
  }

  @Override
  public Identifiable allowRole(
      final DatabaseSession session,
      final EntityImpl iDocument,
      final RestrictedOperation iOperation,
      final String iRoleName) {
    final RID role = getRoleRID(session, iRoleName);
    if (role == null) {
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");
    }

    return allowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public Identifiable allowUser(
      final DatabaseSession session,
      final EntityImpl iDocument,
      final RestrictedOperation iOperation,
      final String iUserName) {
    final RID user = getUserRID(session, iUserName);
    if (user == null) {
      throw new IllegalArgumentException("User '" + iUserName + "' not found");
    }

    return allowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public Identifiable denyUser(
      final DatabaseSessionInternal session,
      final EntityImpl iDocument,
      final RestrictedOperation iOperation,
      final String iUserName) {
    final RID user = getUserRID(session, iUserName);
    if (user == null) {
      throw new IllegalArgumentException("User '" + iUserName + "' not found");
    }

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public Identifiable denyRole(
      final DatabaseSessionInternal session,
      final EntityImpl iDocument,
      final RestrictedOperation iOperation,
      final String iRoleName) {
    final RID role = getRoleRID(session, iRoleName);
    if (role == null) {
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");
    }

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public Identifiable allowIdentity(
      DatabaseSession session, EntityImpl iDocument, String iAllowFieldName,
      Identifiable iId) {
    Set<Identifiable> field = iDocument.field(iAllowFieldName);
    if (field == null) {
      field = new TrackedSet<>(iDocument);
      iDocument.field(iAllowFieldName, field);
    }
    field.add(iId);

    return iId;
  }

  public RID getRoleRID(final DatabaseSession session, final String iRoleName) {
    if (iRoleName == null) {
      return null;
    }

    try (final ResultSet result =
        session.query("select @rid as rid from ORole where name = ? limit 1", iRoleName)) {

      if (result.hasNext()) {
        return result.next().getProperty("rid");
      }
    }
    return null;
  }

  public RID getUserRID(final DatabaseSession session, final String userName) {
    try (ResultSet result =
        session.query("select @rid as rid from OUser where name = ? limit 1", userName)) {

      if (result.hasNext()) {
        return result.next().getProperty("rid");
      }
    }

    return null;
  }

  @Override
  public Identifiable disallowIdentity(
      DatabaseSessionInternal session, EntityImpl iDocument, String iAllowFieldName,
      Identifiable iId) {
    Set<Identifiable> field = iDocument.field(iAllowFieldName);
    if (field != null) {
      field.remove(iId);
    }
    return iId;
  }

  @Override
  public SecurityUserIml authenticate(DatabaseSessionInternal session, String iUsername,
      String iUserPassword) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityUserIml createUser(
      final DatabaseSessionInternal session,
      final String iUserName,
      final String iUserPassword,
      final String... iRoles) {
    final SecurityUserIml user = new SecurityUserIml(session, iUserName, iUserPassword);
    if (iRoles != null) {
      for (String r : iRoles) {
        user.addRole(session, r);
      }
    }
    return user.save(session);
  }

  @Override
  public SecurityUserIml createUser(
      final DatabaseSessionInternal session,
      final String userName,
      final String userPassword,
      final Role... roles) {
    final SecurityUserIml user = new SecurityUserIml(session, userName, userPassword);

    if (roles != null) {
      for (Role r : roles) {
        user.addRole(session, r);
      }
    }

    return user.save(session);
  }

  @Override
  public SecurityUserIml authenticate(DatabaseSessionInternal session, Token authToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Role createRole(
      final DatabaseSessionInternal session, final String iRoleName,
      final ALLOW_MODES iAllowMode) {
    return createRole(session, iRoleName, null, iAllowMode);
  }

  @Override
  public Role createRole(
      final DatabaseSessionInternal session,
      final String iRoleName,
      final Role iParent,
      final ALLOW_MODES iAllowMode) {
    final Role role = new Role(session, iRoleName, iParent, iAllowMode);
    return role.save(session);
  }

  @Override
  public SecurityUserIml getUser(final DatabaseSession session, final String iUserName) {
    try (ResultSet result = session.query("select from OUser where name = ? limit 1",
        iUserName)) {
      if (result.hasNext()) {
        return new SecurityUserIml(session, (EntityImpl) result.next().getEntity().get());
      }
    }
    return null;
  }

  public SecurityUserIml getUser(final DatabaseSession session, final RID iRecordId) {
    if (iRecordId == null) {
      return null;
    }

    EntityImpl result;
    result = session.load(iRecordId);
    if (!result.getClassName().equals(SecurityUserIml.CLASS_NAME)) {
      result = null;
    }
    return new SecurityUserIml(session, result);
  }

  public Role getRole(final DatabaseSession session, final Identifiable iRole) {
    final EntityImpl doc = session.load(iRole.getIdentity());
    SchemaImmutableClass clazz = DocumentInternal.getImmutableSchemaClass(doc);
    if (clazz != null && clazz.isOrole()) {
      return new Role(session, doc);
    }

    return null;
  }

  public Role getRole(final DatabaseSession session, final String iRoleName) {
    if (iRoleName == null) {
      return null;
    }

    try (final ResultSet result =
        session.query("select from ORole where name = ? limit 1", iRoleName)) {
      if (result.hasNext()) {
        return new Role(session, (EntityImpl) result.next().getEntity().get());
      }
    }

    return null;
  }

  public List<EntityImpl> getAllUsers(final DatabaseSession session) {
    try (ResultSet rs = session.query("select from OUser")) {
      return rs.stream().map((e) -> (EntityImpl) e.getEntity().get())
          .collect(Collectors.toList());
    }
  }

  public List<EntityImpl> getAllRoles(final DatabaseSession session) {
    try (ResultSet rs = session.query("select from ORole")) {
      return rs.stream().map((e) -> (EntityImpl) e.getEntity().get())
          .collect(Collectors.toList());
    }
  }

  @Override
  public Map<String, SecurityPolicy> getSecurityPolicies(
      DatabaseSession session, SecurityRole role) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityPolicy getSecurityPolicy(
      DatabaseSession session, SecurityRole role, String resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSecurityPolicy(
      DatabaseSessionInternal session, SecurityRole role, String resource,
      SecurityPolicyImpl policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityPolicyImpl createSecurityPolicy(DatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityPolicyImpl getSecurityPolicy(DatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveSecurityPolicy(DatabaseSession session, SecurityPolicyImpl policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSecurityPolicy(DatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeSecurityPolicy(DatabaseSession session, Role role, String resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropUser(final DatabaseSession session, final String iUserName) {
    final Number removed =
        session.command("delete from OUser where name = ?", iUserName).next().getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  @Override
  public boolean dropRole(final DatabaseSession session, final String iRoleName) {
    final Number removed =
        session
            .command("delete from ORole where name = '" + iRoleName + "'")
            .next()
            .getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  @Override
  public void createClassTrigger(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getVersion(DatabaseSession session) {
    return 0;
  }

  @Override
  public void incrementVersion(DatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityUserIml create(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void load(DatabaseSessionInternal session) {
  }

  @Override
  public void close() {
  }

  @Override
  public Set<String> getFilteredProperties(DatabaseSessionInternal session,
      EntityImpl document) {
    return Collections.emptySet();
  }

  @Override
  public boolean isAllowedWrite(DatabaseSessionInternal session, EntityImpl document,
      String propertyName) {
    return true;
  }

  @Override
  public boolean canCreate(DatabaseSessionInternal session, Record record) {
    return true;
  }

  @Override
  public boolean canRead(DatabaseSessionInternal session, Record record) {
    return true;
  }

  @Override
  public boolean canUpdate(DatabaseSessionInternal session, Record record) {
    return true;
  }

  @Override
  public boolean canDelete(DatabaseSessionInternal session, Record record) {
    return true;
  }

  @Override
  public boolean canExecute(DatabaseSessionInternal session, Function function) {
    return true;
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(DatabaseSession session, String resource) {
    return false;
  }

  @Override
  public Set<SecurityResourceProperty> getAllFilteredProperties(
      DatabaseSessionInternal database) {
    return Collections.EMPTY_SET;
  }

  @Override
  public SecurityUser securityAuthenticate(
      DatabaseSessionInternal session, String userName, String password) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityUser securityAuthenticate(
      DatabaseSessionInternal session, AuthenticationInfo authenticationInfo) {
    throw new UnsupportedOperationException();
  }
}
