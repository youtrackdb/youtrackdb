package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.schema.OViewImpl;
import com.orientechnologies.orient.core.metadata.schema.OViewRemovedMetadata;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import java.util.List;

/**
 *
 */
public class OViewRemote extends OViewImpl {

  protected OViewRemote(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OProperty addProperty(
      ODatabaseSessionInternal session, final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    if (type == null) {
      throw new OSchemaException("Property type not defined.");
    }

    if (propertyName == null || propertyName.length() == 0) {
      throw new OSchemaException("Property name is null or empty");
    }

    validatePropertyName(propertyName);
    if (session.getTransaction().isActive()) {
      throw new OSchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null) {
      OPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      OPropertyImpl.checkSupportLinkedClass(type);
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

  public OClassImpl setEncryption(ODatabaseSessionInternal session, final String iValue) {
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
  public OClass setClusterSelection(ODatabaseSession session, final String value) {
    throw new UnsupportedOperationException();
  }

  public OClassImpl setCustom(ODatabaseSession session, final String name, final String value) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  public void clearCustom(ODatabaseSession session) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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
  public OClass setSuperClasses(ODatabaseSession session, final List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass addSuperClass(ODatabaseSession session, final OClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass removeSuperClass(ODatabaseSession session, OClass superClass) {
    throw new UnsupportedOperationException();
  }

  public OView setName(ODatabaseSession session, final String name) {
    if (getName().equals(name)) {
      return this;
    }
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OView oClass = sessionInternal.getMetadata().getSchema().getView(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename view %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null) {
      throw new OSchemaException(
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

  public OView setShortName(ODatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }

    var sessionInternal = (ODatabaseSessionInternal) session;
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

  protected OPropertyImpl createPropertyInstance() {
    return new OPropertyRemote(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OView truncateCluster(ODatabaseSession session, String clusterName) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  public OView setStrictMode(ODatabaseSession session, final boolean isStrict) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  public OView setDescription(ODatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  public OView addClusterId(ODatabaseSession session, final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public OView removeClusterId(ODatabaseSession session, final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public void dropProperty(ODatabaseSession session, final String propertyName) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    if (sessionInternal.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new OSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      session.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  @Override
  public OView addCluster(ODatabaseSession session, final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public OView setOverSize(ODatabaseSession session, final float overSize) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  public OView setAbstract(ODatabaseSession session, boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  public OView removeBaseClassInternal(ODatabaseSessionInternal session, final OClass baseClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(ODatabaseSessionInternal session,
      final List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(ODatabaseSession session, final int defaultClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OViewRemovedMetadata replaceViewClusterAndIndex(
      ODatabaseSessionInternal session, int cluster, List<OIndex> indexes, long lastRefreshTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addClusterIdToIndexes(ODatabaseSessionInternal session, int iId) {
  }
}
