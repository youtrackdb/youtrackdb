package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassAbstractDelegate;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OClassRemote extends OClassImpl {

  protected OClassRemote(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OClassRemote(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
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
      getOwner().reload(session);

      return getProperty(propertyName);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OClassImpl setEncryption(ODatabaseSessionInternal session, final String iValue) {
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
  public OClass setClusterSelection(ODatabaseSession session, final String value) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClassImpl setCustom(ODatabaseSession session, final String name, final String value) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public void clearCustom(ODatabaseSession session) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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
  public OClass setSuperClasses(ODatabaseSession session, final List<? extends OClass> classes) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<OClass> toCheck = new ArrayList<OClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock(database);
    try {
      final StringBuilder sb = new StringBuilder();
      if (classes != null && !classes.isEmpty()) {
        for (OClass superClass : classes) {
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
  public OClass addSuperClass(ODatabaseSession session, final OClass superClass) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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
  public OClass removeSuperClass(ODatabaseSession session, OClass superClass) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClass setName(ODatabaseSession session, final String name) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    if (getName().equals(name)) {
      return this;
    }
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null) {
      throw new OSchemaException(
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

  public OClass setShortName(ODatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  protected OPropertyImpl createPropertyInstance() {
    return new OPropertyRemote(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OClass truncateCluster(ODatabaseSession session, String clusterName) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClass setStrictMode(ODatabaseSession session, final boolean isStrict) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClass setDescription(ODatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClass addClusterId(ODatabaseSession session, final int clusterId) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
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

  public OClass removeClusterId(ODatabaseSession session, final int clusterId) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new ODatabaseException(
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

  public void dropProperty(ODatabaseSession session, final String propertyName) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new OSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      database.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public OClass addCluster(ODatabaseSession session, final String clusterNameOrId) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
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

  public OClass setOverSize(ODatabaseSession session, final float overSize) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClass setAbstract(ODatabaseSession session, boolean isAbstract) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  public OClass removeBaseClassInternal(ODatabaseSessionInternal session, final OClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (OClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void setSuperClassesInternal(ODatabaseSessionInternal session,
      final List<? extends OClass> classes) {
    List<OClassImpl> newSuperClasses = new ArrayList<OClassImpl>();
    OClassImpl cls;
    for (OClass superClass : classes) {
      if (superClass instanceof OClassAbstractDelegate) {
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).getDelegate();
      } else {
        cls = (OClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new OSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<OClassImpl> toAddList = new ArrayList<OClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<OClassImpl> toRemoveList = new ArrayList<OClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (OClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (OClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public void setDefaultClusterId(ODatabaseSession session, final int defaultClusterId) {
    ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    String clusterName = database.getClusterNameById(defaultClusterId);
    if (clusterName == null) {
      throw new OSchemaException("Cluster with id '" + defaultClusterId + "' does not exist");
    }
    final String cmd =
        String.format("alter class `%s` DEFAULTCLUSTER `%s`", this.name, clusterName);
    database.command(cmd).close();
  }

  protected void addClusterIdToIndexes(ODatabaseSessionInternal session, int iId) {
  }
}
