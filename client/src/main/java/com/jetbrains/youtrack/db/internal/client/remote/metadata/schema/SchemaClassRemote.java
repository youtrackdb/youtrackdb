package com.jetbrains.youtrack.db.internal.client.remote.metadata.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinitionFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SchemaClassRemote extends SchemaClassImpl {

  protected SchemaClassRemote(SchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected SchemaClassRemote(SchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
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
      getOwner().reload(session);

      return getProperty(propertyName);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSession session, final String value) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` clusterselection '%s'", name, value);
      database.command(cmd).close();
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public SchemaClassImpl setCustom(DatabaseSession session, final String name, final String value) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` custom %s = ?", getName(), name);
      database.command(cmd, value).close();
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void clearCustom(DatabaseSession session) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` custom clear", getName());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public SchemaClass setSuperClasses(DatabaseSession session,
      final List<? extends SchemaClass> classes) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    if (classes != null) {
      List<SchemaClass> toCheck = new ArrayList<SchemaClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock(database);
    try {
      final StringBuilder sb = new StringBuilder();
      if (classes != null && !classes.isEmpty()) {
        for (SchemaClass superClass : classes) {
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
  public SchemaClass addSuperClass(DatabaseSession session, final SchemaClass superClass) {
    final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
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
  public void removeSuperClass(DatabaseSession session, SchemaClass superClass) {
    final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
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
  }

  @Override
  public void createIndex(DatabaseSession session, String name, String type,
      ProgressListener progressListener, Map<String, ?> metadata, String algorithm,
      String... fields) {
    if (type == null) {
      throw new IllegalArgumentException("Index type is null");
    }

    type = type.toUpperCase(Locale.ENGLISH);

    if (fields.length == 0) {
      throw new IndexException("List of fields to index cannot be empty.");
    }

    var sessionInternal = (DatabaseSessionInternal) session;
    final String localName = this.name;

    for (final String fieldToIndex : fields) {
      final String fieldName =
          decodeClassName(IndexDefinitionFactory.extractFieldName(fieldToIndex));

      if (!fieldName.equals("@rid") && !existsProperty(fieldName)) {
        throw new IndexException(
            "Index with name '"
                + name
                + "' cannot be created on class '"
                + localName
                + "' because the field '"
                + fieldName
                + "' is absent in class definition");
      }
    }

    var queryBuilder = new StringBuilder();
    queryBuilder.append("create index ").append(name).append(" on ").append(localName).append(" (");
    for (int i = 0; i < fields.length - 1; i++) {
      queryBuilder.append(fields[i]).append(", ");
    }

    queryBuilder.append(fields[fields.length - 1]).append(") ");
    queryBuilder.append(type);

    if (metadata != null) {
      var objectMapper = new ObjectMapper();
      try {
        var json = objectMapper.writeValueAsString(metadata);
        queryBuilder.append(" metadata ").append(json);
      } catch (JsonProcessingException e) {
        throw BaseException.wrapException(
            new DatabaseException("Error during conversion of metadata in JSON format"), e);
      }
    }

    sessionInternal.command(queryBuilder.toString()).close();
  }

  public SchemaClass setName(DatabaseSession session, final String name) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    if (getName().equals(name)) {
      return this;
    }
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    final Character wrongCharacter = SchemaShared.checkClassNameIfValid(name);
    SchemaClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new SchemaException(error);
    }
    if (wrongCharacter != null) {
      throw new SchemaException(
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

  public SchemaClass setShortName(DatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` shortname `%s`", name, shortName);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock(database);
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
  public SchemaClass truncateCluster(DatabaseSession session, String clusterName) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_DELETE, name);
    acquireSchemaReadLock();
    try {

      final String cmd = String.format("truncate cluster %s", clusterName);
      database.command(cmd).close();
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public SchemaClass setStrictMode(DatabaseSession session, final boolean isStrict) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` strict_mode %s", name, isStrict);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public SchemaClass setDescription(DatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` description ?", name);
      database.command(cmd, iDescription).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public SchemaClass addClusterId(DatabaseSession session, final int clusterId) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new SchemaException("Impossible to associate a cluster to an abstract class class");
    }
    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` add_cluster %d", name, clusterId);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public SchemaClass removeClusterId(DatabaseSession session, final int clusterId) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new DatabaseException(
          " Impossible to remove the last cluster of class '"
              + getName()
              + "' drop the class instead");
    }

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` remove_cluster %d", name, clusterId);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public void dropProperty(DatabaseSession session, final String propertyName) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new SchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      database.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public SchemaClass addCluster(DatabaseSession session, final String clusterNameOrId) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new SchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` add_cluster `%s`", name, clusterNameOrId);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public SchemaClass setOverSize(DatabaseSession session, final float overSize) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
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

  public SchemaClass setAbstract(DatabaseSession session, boolean isAbstract) {
    DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter class `%s` abstract %s", name, isAbstract);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public SchemaClass removeBaseClassInternal(DatabaseSessionInternal session,
      final SchemaClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (SchemaClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void setSuperClassesInternal(DatabaseSessionInternal session,
      final List<? extends SchemaClass> classes) {
    List<SchemaClassImpl> newSuperClasses = new ArrayList<SchemaClassImpl>();
    SchemaClassImpl cls;
    for (SchemaClass superClass : classes) {
      cls = (SchemaClassImpl) superClass;

      if (newSuperClasses.contains(cls)) {
        throw new SchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<SchemaClassImpl> toAddList = new ArrayList<SchemaClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<SchemaClassImpl> toRemoveList = new ArrayList<SchemaClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (SchemaClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (SchemaClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }


  @Override
  public void getIndexedProperties(DatabaseSessionInternal session,
      Collection<Property> indexedProperties) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Collection<Property> getIndexedProperties(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public boolean areIndexed(DatabaseSession session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public boolean areIndexed(DatabaseSession session, Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSession session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSession session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSession session, Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSession session, Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSession session,
      Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSession session,
      Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSession session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSession session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Index getClassIndex(DatabaseSession session, String name) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getClassIndexes(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getClassIndexesInternal(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public void getClassIndexes(DatabaseSession session, Collection<Index> indexes) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public void getIndexesInternal(DatabaseSession session, Collection<Index> indexes) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getIndexes(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getIndexesInternal(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  protected void addClusterIdToIndexes(DatabaseSessionInternal session, int iId) {
  }
}
