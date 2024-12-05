package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSchemaException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTClassAbstractDelegate;
import com.orientechnologies.core.metadata.schema.YTClassImpl;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTPropertyImpl;
import com.orientechnologies.core.metadata.schema.OSchemaShared;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.metadata.security.ORole;
import com.orientechnologies.core.metadata.security.ORule;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class YTClassRemote extends YTClassImpl {

  protected YTClassRemote(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected YTClassRemote(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
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
      getOwner().reload(session);

      return getProperty(propertyName);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTClassImpl setEncryption(YTDatabaseSessionInternal session, final String iValue) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      final String cmd = String.format("alter class `%s` encryption %s", name, iValue);
      session.command(cmd);
    } finally {
      releaseSchemaWriteLock(session);
    }
    return this;
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session, final String value) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` clusterselection '%s'", name, value);
      database.command(cmd).close();
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public YTClassImpl setCustom(YTDatabaseSession session, final String name, final String value) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` custom %s = ?", getName(), name);
      database.command(cmd, value).close();
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void clearCustom(YTDatabaseSession session) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` custom clear", getName());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public YTClass setSuperClasses(YTDatabaseSession session, final List<? extends YTClass> classes) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<YTClass> toCheck = new ArrayList<YTClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock(database);
    try {
      final StringBuilder sb = new StringBuilder();
      if (classes != null && !classes.isEmpty()) {
        for (YTClass superClass : classes) {
          sb.append('`').append(superClass.getName()).append("`,");
        }
        sb.deleteCharAt(sb.length() - 1);
      } else {
        sb.append("null");
      }

      final String cmd = String.format("alter class `%s` superclasses %s", name, sb);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  @Override
  public YTClass addSuperClass(YTDatabaseSession session, final YTClass superClass) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkParametersConflict(database, superClass);
    acquireSchemaWriteLock(database);
    try {

      final String cmd =
          String.format(
              "alter class `%s` superclass +`%s`",
              name, superClass != null ? superClass.getName() : null);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  @Override
  public YTClass removeSuperClass(YTDatabaseSession session, YTClass superClass) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter class `%s` superclass -`%s`",
              name, superClass != null ? superClass.getName() : null);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public YTClass setName(YTDatabaseSession session, final String name) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    if (getName().equals(name)) {
      return this;
    }
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    YTClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new YTSchemaException(error);
    }
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + name
              + "'");
    }
    acquireSchemaWriteLock(database);
    try {

      final String cmd = String.format("alter class `%s` name `%s`", this.name, name);
      database.command(cmd);

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass setShortName(YTDatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` shortname `%s`", name, shortName);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock(database);
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
  public YTClass truncateCluster(YTDatabaseSession session, String clusterName) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);
    acquireSchemaReadLock();
    try {

      final String cmd = String.format("truncate cluster %s", clusterName);
      database.command(cmd).close();
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public YTClass setStrictMode(YTDatabaseSession session, final boolean isStrict) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` strictmode %s", name, isStrict);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass setDescription(YTDatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` description ?", name);
      database.command(cmd, iDescription).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass addClusterId(YTDatabaseSession session, final int clusterId) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new YTSchemaException("Impossible to associate a cluster to an abstract class class");
    }
    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` addcluster %d", name, clusterId);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public YTClass removeClusterId(YTDatabaseSession session, final int clusterId) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new YTDatabaseException(
          " Impossible to remove the last cluster of class '"
              + getName()
              + "' drop the class instead");
    }

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` removecluster %d", name, clusterId);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public void dropProperty(YTDatabaseSession session, final String propertyName) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new YTSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      database.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public YTClass addCluster(YTDatabaseSession session, final String clusterNameOrId) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new YTSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` addcluster `%s`", name, clusterNameOrId);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass setOverSize(YTDatabaseSession session, final float overSize) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      // FORMAT FLOAT LOCALE AGNOSTIC
      final String cmd = Float.toString(overSize);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass setAbstract(YTDatabaseSession session, boolean isAbstract) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` abstract %s", name, isAbstract);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass removeBaseClassInternal(YTDatabaseSessionInternal session,
      final YTClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (YTClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void setSuperClassesInternal(YTDatabaseSessionInternal session,
      final List<? extends YTClass> classes) {
    List<YTClassImpl> newSuperClasses = new ArrayList<YTClassImpl>();
    YTClassImpl cls;
    for (YTClass superClass : classes) {
      if (superClass instanceof YTClassAbstractDelegate) {
        cls = (YTClassImpl) ((YTClassAbstractDelegate) superClass).getDelegate();
      } else {
        cls = (YTClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new YTSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<YTClassImpl> toAddList = new ArrayList<YTClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<YTClassImpl> toRemoveList = new ArrayList<YTClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (YTClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (YTClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public void setDefaultClusterId(YTDatabaseSession session, final int defaultClusterId) {
    YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    String clusterName = database.getClusterNameById(defaultClusterId);
    if (clusterName == null) {
      throw new YTSchemaException("Cluster with id '" + defaultClusterId + "' does not exist");
    }
    final String cmd =
        String.format("alter class `%s` DEFAULTCLUSTER `%s`", this.name, clusterName);
    database.command(cmd).close();
  }

  protected void addClusterIdToIndexes(YTDatabaseSessionInternal session, int iId) {
  }
}
