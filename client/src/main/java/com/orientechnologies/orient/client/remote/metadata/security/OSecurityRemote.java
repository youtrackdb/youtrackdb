package com.orientechnologies.orient.client.remote.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.metadata.security.ORestrictedOperation;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicyImpl;
import com.orientechnologies.orient.core.metadata.security.OSecurityResourceProperty;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole.ALLOW_MODES;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OSecurityRemote implements OSecurityInternal {

  public OSecurityRemote() {
  }

  @Override
  public boolean isAllowed(
      YTDatabaseSessionInternal session, Set<YTIdentifiable> iAllowAll,
      Set<YTIdentifiable> iAllowOperation) {
    return true;
  }

  @Override
  public YTIdentifiable allowRole(
      final YTDatabaseSession session,
      final YTDocument iDocument,
      final ORestrictedOperation iOperation,
      final String iRoleName) {
    final YTRID role = getRoleRID(session, iRoleName);
    if (role == null) {
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");
    }

    return allowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public YTIdentifiable allowUser(
      final YTDatabaseSession session,
      final YTDocument iDocument,
      final ORestrictedOperation iOperation,
      final String iUserName) {
    final YTRID user = getUserRID(session, iUserName);
    if (user == null) {
      throw new IllegalArgumentException("User '" + iUserName + "' not found");
    }

    return allowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public YTIdentifiable denyUser(
      final YTDatabaseSessionInternal session,
      final YTDocument iDocument,
      final ORestrictedOperation iOperation,
      final String iUserName) {
    final YTRID user = getUserRID(session, iUserName);
    if (user == null) {
      throw new IllegalArgumentException("User '" + iUserName + "' not found");
    }

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public YTIdentifiable denyRole(
      final YTDatabaseSessionInternal session,
      final YTDocument iDocument,
      final ORestrictedOperation iOperation,
      final String iRoleName) {
    final YTRID role = getRoleRID(session, iRoleName);
    if (role == null) {
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");
    }

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public YTIdentifiable allowIdentity(
      YTDatabaseSession session, YTDocument iDocument, String iAllowFieldName, YTIdentifiable iId) {
    Set<YTIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field == null) {
      field = new OTrackedSet<>(iDocument);
      iDocument.field(iAllowFieldName, field);
    }
    field.add(iId);

    return iId;
  }

  public YTRID getRoleRID(final YTDatabaseSession session, final String iRoleName) {
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

  public YTRID getUserRID(final YTDatabaseSession session, final String userName) {
    try (YTResultSet result =
        session.query("select @rid as rid from OUser where name = ? limit 1", userName)) {

      if (result.hasNext()) {
        return result.next().getProperty("rid");
      }
    }

    return null;
  }

  @Override
  public YTIdentifiable disallowIdentity(
      YTDatabaseSessionInternal session, YTDocument iDocument, String iAllowFieldName,
      YTIdentifiable iId) {
    Set<YTIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field != null) {
      field.remove(iId);
    }
    return iId;
  }

  @Override
  public OUser authenticate(YTDatabaseSessionInternal session, String iUsername,
      String iUserPassword) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OUser createUser(
      final YTDatabaseSessionInternal session,
      final String iUserName,
      final String iUserPassword,
      final String... iRoles) {
    final OUser user = new OUser(session, iUserName, iUserPassword);
    if (iRoles != null) {
      for (String r : iRoles) {
        user.addRole(session, r);
      }
    }
    return user.save(session);
  }

  @Override
  public OUser createUser(
      final YTDatabaseSessionInternal session,
      final String userName,
      final String userPassword,
      final ORole... roles) {
    final OUser user = new OUser(session, userName, userPassword);

    if (roles != null) {
      for (ORole r : roles) {
        user.addRole(session, r);
      }
    }

    return user.save(session);
  }

  @Override
  public OUser authenticate(YTDatabaseSessionInternal session, OToken authToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ORole createRole(
      final YTDatabaseSessionInternal session, final String iRoleName,
      final ALLOW_MODES iAllowMode) {
    return createRole(session, iRoleName, null, iAllowMode);
  }

  @Override
  public ORole createRole(
      final YTDatabaseSessionInternal session,
      final String iRoleName,
      final ORole iParent,
      final ALLOW_MODES iAllowMode) {
    final ORole role = new ORole(session, iRoleName, iParent, iAllowMode);
    return role.save(session);
  }

  @Override
  public OUser getUser(final YTDatabaseSession session, final String iUserName) {
    try (YTResultSet result = session.query("select from OUser where name = ? limit 1",
        iUserName)) {
      if (result.hasNext()) {
        return new OUser(session, (YTDocument) result.next().getElement().get());
      }
    }
    return null;
  }

  public OUser getUser(final YTDatabaseSession session, final YTRID iRecordId) {
    if (iRecordId == null) {
      return null;
    }

    YTDocument result;
    result = session.load(iRecordId);
    if (!result.getClassName().equals(OUser.CLASS_NAME)) {
      result = null;
    }
    return new OUser(session, result);
  }

  public ORole getRole(final YTDatabaseSession session, final YTIdentifiable iRole) {
    final YTDocument doc = session.load(iRole.getIdentity());
    YTImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
    if (clazz != null && clazz.isOrole()) {
      return new ORole(session, doc);
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
        return new ORole(session, (YTDocument) result.next().getElement().get());
      }
    }

    return null;
  }

  public List<YTDocument> getAllUsers(final YTDatabaseSession session) {
    try (YTResultSet rs = session.query("select from OUser")) {
      return rs.stream().map((e) -> (YTDocument) e.getElement().get()).collect(Collectors.toList());
    }
  }

  public List<YTDocument> getAllRoles(final YTDatabaseSession session) {
    try (YTResultSet rs = session.query("select from ORole")) {
      return rs.stream().map((e) -> (YTDocument) e.getElement().get()).collect(Collectors.toList());
    }
  }

  @Override
  public Map<String, OSecurityPolicy> getSecurityPolicies(
      YTDatabaseSession session, OSecurityRole role) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(
      YTDatabaseSession session, OSecurityRole role, String resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSecurityPolicy(
      YTDatabaseSessionInternal session, OSecurityRole role, String resource,
      OSecurityPolicyImpl policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityPolicyImpl createSecurityPolicy(YTDatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityPolicyImpl getSecurityPolicy(YTDatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveSecurityPolicy(YTDatabaseSession session, OSecurityPolicyImpl policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSecurityPolicy(YTDatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeSecurityPolicy(YTDatabaseSession session, ORole role, String resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropUser(final YTDatabaseSession session, final String iUserName) {
    final Number removed =
        session.command("delete from OUser where name = ?", iUserName).next().getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  @Override
  public boolean dropRole(final YTDatabaseSession session, final String iRoleName) {
    final Number removed =
        session
            .command("delete from ORole where name = '" + iRoleName + "'")
            .next()
            .getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  @Override
  public void createClassTrigger(YTDatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getVersion(YTDatabaseSession session) {
    return 0;
  }

  @Override
  public void incrementVersion(YTDatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OUser create(YTDatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void load(YTDatabaseSessionInternal session) {
  }

  @Override
  public void close() {
  }

  @Override
  public Set<String> getFilteredProperties(YTDatabaseSessionInternal session, YTDocument document) {
    return Collections.emptySet();
  }

  @Override
  public boolean isAllowedWrite(YTDatabaseSessionInternal session, YTDocument document,
      String propertyName) {
    return true;
  }

  @Override
  public boolean canCreate(YTDatabaseSessionInternal session, YTRecord record) {
    return true;
  }

  @Override
  public boolean canRead(YTDatabaseSessionInternal session, YTRecord record) {
    return true;
  }

  @Override
  public boolean canUpdate(YTDatabaseSessionInternal session, YTRecord record) {
    return true;
  }

  @Override
  public boolean canDelete(YTDatabaseSessionInternal session, YTRecord record) {
    return true;
  }

  @Override
  public boolean canExecute(YTDatabaseSessionInternal session, OFunction function) {
    return true;
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(YTDatabaseSession session, String resource) {
    return false;
  }

  @Override
  public Set<OSecurityResourceProperty> getAllFilteredProperties(
      YTDatabaseSessionInternal database) {
    return Collections.EMPTY_SET;
  }

  @Override
  public OSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, String userName, String password) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityUser securityAuthenticate(
      YTDatabaseSessionInternal session, OAuthenticationInfo authenticationInfo) {
    throw new UnsupportedOperationException();
  }
}
