package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTSchemaException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTClassImpl;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTPropertyImpl;
import com.orientechnologies.core.metadata.schema.OSchemaShared;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.metadata.schema.YTView;
import com.orientechnologies.core.metadata.schema.YTViewImpl;
import com.orientechnologies.core.metadata.schema.OViewRemovedMetadata;
import com.orientechnologies.core.metadata.security.ORole;
import com.orientechnologies.core.metadata.security.ORule;
import java.util.List;

/**
 *
 */
public class YTViewRemote extends YTViewImpl {

  protected YTViewRemote(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected YTProperty addProperty(
      YTDatabaseSessionInternal session, final String propertyName,
      final YTType type,
      final YTType linkedType,
      final YTClass linkedClass,
      final boolean unsafe) {
    if (type == null) {
      throw new YTSchemaException("Property type not defined.");
    }

    if (propertyName == null || propertyName.length() == 0) {
      throw new YTSchemaException("Property name is null or empty");
    }

    validatePropertyName(propertyName);
    if (session.getTransaction().isActive()) {
      throw new YTSchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null) {
      YTPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      YTPropertyImpl.checkSupportLinkedClass(type);
    }

    acquireSchemaWriteLock(session);
    try {
      final StringBuilder cmd = new StringBuilder("create property ");
      // CLASS.PROPERTY NAME
      cmd.append('`');
      cmd.append(name);
      cmd.append('`');
      cmd.append('.');
      cmd.append('`');
      cmd.append(propertyName);
      cmd.append('`');

      // TYPE
      cmd.append(' ');
      cmd.append(type.getName());

      if (linkedType != null) {
        // TYPE
        cmd.append(' ');
        cmd.append(linkedType.getName());

      } else if (linkedClass != null) {
        // TYPE
        cmd.append(' ');
        cmd.append('`');
        cmd.append(linkedClass.getName());
        cmd.append('`');
      }

      if (unsafe) {
        cmd.append(" unsafe ");
      }

      session.command(cmd.toString()).close();

      return getProperty(propertyName);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTClassImpl setEncryption(YTDatabaseSessionInternal session, final String iValue) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      final String cmd = String.format("alter view `%s` encryption %s", name, iValue);
      session.command(cmd);
    } finally {
      releaseSchemaWriteLock(session);
    }
    return this;
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session, final String value) {
    throw new UnsupportedOperationException();
  }

  public YTClassImpl setCustom(YTDatabaseSession session, final String name, final String value) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` custom %s = ?", getName(), name);
      sessionInternal.command(cmd, value).close();
      return this;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  public void clearCustom(YTDatabaseSession session) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` custom clear", getName());
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  @Override
  public YTClass setSuperClasses(YTDatabaseSession session, final List<? extends YTClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass addSuperClass(YTDatabaseSession session, final YTClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass removeSuperClass(YTDatabaseSession session, YTClass superClass) {
    throw new UnsupportedOperationException();
  }

  public YTView setName(YTDatabaseSession session, final String name) {
    if (getName().equals(name)) {
      return this;
    }
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    YTView oClass = sessionInternal.getMetadata().getSchema().getView(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename view %s to %s. A Class with name %s exists", this.name, name, name);
      throw new YTSchemaException(error);
    }
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in view name '"
              + name
              + "'");
    }
    acquireSchemaWriteLock(sessionInternal);
    try {

      final String cmd = String.format("alter view `%s` name `%s`", this.name, name);
      sessionInternal.command(cmd);

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public YTView setShortName(YTDatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }

    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` shortname `%s`", name, shortName);
      sessionInternal.command(cmd);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected YTPropertyImpl createPropertyInstance() {
    return new YTPropertyRemote(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTView truncateCluster(YTDatabaseSession session, String clusterName) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);
    acquireSchemaReadLock();
    try {

      final String cmd = String.format("truncate cluster %s", clusterName);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public YTView setStrictMode(YTDatabaseSession session, final boolean isStrict) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` strictmode %s", name, isStrict);
      sessionInternal.command(cmd);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public YTView setDescription(YTDatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` description ?", name);
      sessionInternal.command(cmd, iDescription).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public YTView addClusterId(YTDatabaseSession session, final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public YTView removeClusterId(YTDatabaseSession session, final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public void dropProperty(YTDatabaseSession session, final String propertyName) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    if (sessionInternal.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new YTSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      session.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  @Override
  public YTView addCluster(YTDatabaseSession session, final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public YTView setOverSize(YTDatabaseSession session, final float overSize) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(sessionInternal);
    try {
      // FORMAT FLOAT LOCALE AGNOSTIC
      final String cmd = Float.toString(overSize);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public YTView setAbstract(YTDatabaseSession session, boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  public YTView removeBaseClassInternal(YTDatabaseSessionInternal session,
      final YTClass baseClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(YTDatabaseSessionInternal session,
      final List<? extends YTClass> classes) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(YTDatabaseSession session, final int defaultClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OViewRemovedMetadata replaceViewClusterAndIndex(
      YTDatabaseSessionInternal session, int cluster, List<OIndex> indexes, long lastRefreshTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addClusterIdToIndexes(YTDatabaseSessionInternal session, int iId) {
  }
}
