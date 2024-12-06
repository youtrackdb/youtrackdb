package com.jetbrains.youtrack.db.internal.client.remote.metadata.schema;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Property;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaView;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaViewImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ViewRemovedMetadata;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import java.util.List;

/**
 *
 */
public class SchemaViewRemote extends SchemaViewImpl {

  protected SchemaViewRemote(SchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected Property addProperty(
      DatabaseSessionInternal session, final String propertyName,
      final PropertyType type,
      final PropertyType linkedType,
      final SchemaClass linkedClass,
      final boolean unsafe) {
    if (type == null) {
      throw new SchemaException("Property type not defined.");
    }

    if (propertyName == null || propertyName.length() == 0) {
      throw new SchemaException("Property name is null or empty");
    }

    validatePropertyName(propertyName);
    if (session.getTransaction().isActive()) {
      throw new SchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (linkedType != null) {
      PropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      PropertyImpl.checkSupportLinkedClass(type);
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

  public SchemaClassImpl setEncryption(DatabaseSessionInternal session, final String iValue) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

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
  public SchemaClass setClusterSelection(DatabaseSession session, final String value) {
    throw new UnsupportedOperationException();
  }

  public SchemaClassImpl setCustom(DatabaseSession session, final String name, final String value) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` custom %s = ?", getName(), name);
      sessionInternal.command(cmd, value).close();
      return this;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  public void clearCustom(DatabaseSession session) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` custom clear", getName());
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  @Override
  public SchemaClass setSuperClasses(DatabaseSession session,
      final List<? extends SchemaClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass addSuperClass(DatabaseSession session, final SchemaClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass removeSuperClass(DatabaseSession session, SchemaClass superClass) {
    throw new UnsupportedOperationException();
  }

  public SchemaView setName(DatabaseSession session, final String name) {
    if (getName().equals(name)) {
      return this;
    }
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    final Character wrongCharacter = SchemaShared.checkClassNameIfValid(name);
    SchemaView oClass = sessionInternal.getMetadata().getSchema().getView(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename view %s to %s. A Class with name %s exists", this.name, name, name);
      throw new SchemaException(error);
    }
    if (wrongCharacter != null) {
      throw new SchemaException(
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

  public SchemaView setShortName(DatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }

    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` shortname `%s`", name, shortName);
      sessionInternal.command(cmd);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected PropertyImpl createPropertyInstance() {
    return new PropertyRemote(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchemaView truncateCluster(DatabaseSession session, String clusterName) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_DELETE, name);
    acquireSchemaReadLock();
    try {

      final String cmd = String.format("truncate cluster %s", clusterName);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public SchemaView setStrictMode(DatabaseSession session, final boolean isStrict) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` strictmode %s", name, isStrict);
      sessionInternal.command(cmd);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaView setDescription(DatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final String cmd = String.format("alter view `%s` description ?", name);
      sessionInternal.command(cmd, iDescription).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaView addClusterId(DatabaseSession session, final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public SchemaView removeClusterId(DatabaseSession session, final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public void dropProperty(DatabaseSession session, final String propertyName) {
    var sessionInternal = (DatabaseSessionInternal) session;
    if (sessionInternal.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new SchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      session.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  @Override
  public SchemaView addCluster(DatabaseSession session, final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public SchemaView setOverSize(DatabaseSession session, final float overSize) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
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

  public SchemaView setAbstract(DatabaseSession session, boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  public SchemaView removeBaseClassInternal(DatabaseSessionInternal session,
      final SchemaClass baseClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(DatabaseSessionInternal session,
      final List<? extends SchemaClass> classes) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(DatabaseSession session, final int defaultClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ViewRemovedMetadata replaceViewClusterAndIndex(
      DatabaseSessionInternal session, int cluster, List<Index> indexes, long lastRefreshTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addClusterIdToIndexes(DatabaseSessionInternal session, int iId) {
  }
}
