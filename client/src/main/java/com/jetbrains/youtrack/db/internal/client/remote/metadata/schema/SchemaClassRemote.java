package com.jetbrains.youtrack.db.internal.client.remote.metadata.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinitionFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaPropertyImpl;
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

  protected SchemaProperty addProperty(
      DatabaseSessionInternal session, final String propertyName,
      final PropertyType type,
      final PropertyType linkedType,
      final SchemaClass linkedClass,
      final boolean unsafe) {
    if (type == null) {
      throw new SchemaException(session, "Property type not defined.");
    }

    if (propertyName == null || propertyName.length() == 0) {
      throw new SchemaException(session, "Property name is null or empty");
    }

    validatePropertyName(propertyName);
    if (session.getTransaction().isActive()) {
      throw new SchemaException(session,
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (linkedType != null) {
      SchemaPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      SchemaPropertyImpl.checkSupportLinkedClass(type);
    }

    acquireSchemaWriteLock(session);
    try {
      final var cmd = new StringBuilder("create property ");
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
        cmd.append(linkedClass.getName(session));
        cmd.append('`');
      }

      if (unsafe) {
        cmd.append(" unsafe ");
      }

      session.command(cmd.toString()).close();
      getOwner().reload(session);

      return getProperty(session, propertyName);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSession session, final String value) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` clusterselection '%s'", name, value);
      database.command(cmd).close();
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public SchemaClassImpl setCustom(DatabaseSession session, final String name, final String value) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` custom %s = ?", getName(session), name);
      database.command(cmd, value).close();
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void clearCustom(DatabaseSession session) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` custom clear", getName(session));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public SchemaClass setSuperClasses(DatabaseSession session,
      final List<? extends SchemaClass> classes) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    if (classes != null) {
      List<SchemaClass> toCheck = new ArrayList<SchemaClass>(classes);
      toCheck.add(this);
      checkParametersConflict(sessionInternal, toCheck);
    }
    acquireSchemaWriteLock(sessionInternal);
    try {
      final var sb = new StringBuilder();
      if (classes != null && !classes.isEmpty()) {
        for (var superClass : classes) {
          sb.append('`').append(superClass.getName(sessionInternal)).append("`,");
        }
        sb.deleteCharAt(sb.length() - 1);
      } else {
        sb.append("null");
      }

      final var cmd = String.format("alter class `%s` superclasses %s", name, sb);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  @Override
  public SchemaClass addSuperClass(DatabaseSession session, final SchemaClass superClass) {
    final var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    checkParametersConflict(sessionInternal, superClass);
    acquireSchemaWriteLock(sessionInternal);
    try {

      final var cmd =
          String.format(
              "alter class `%s` superclass +`%s`",
              name, superClass != null ? superClass.getName(sessionInternal) : null);
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  @Override
  public void removeSuperClass(DatabaseSession session, SchemaClass superClass) {
    final var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter class `%s` superclass -`%s`",
              name, superClass != null ? superClass.getName(sessionInternal) : null);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
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
      throw new IndexException(session, "List of fields to index cannot be empty.");
    }

    var sessionInternal = (DatabaseSessionInternal) session;
    final var localName = this.name;

    for (final var fieldToIndex : fields) {
      final var fieldName =
          decodeClassName(IndexDefinitionFactory.extractFieldName(fieldToIndex));

      if (!fieldName.equals("@rid") && !existsProperty(session, fieldName)) {
        throw new IndexException(session,
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
    for (var i = 0; i < fields.length - 1; i++) {
      queryBuilder.append(fields[i]).append(", ");
    }

    queryBuilder.append(fields[fields.length - 1]).append(") ");
    queryBuilder.append(type);

    if (algorithm != null) {
      queryBuilder.append(" engine ").append(algorithm);
    }

    if (metadata != null) {
      var objectMapper = new ObjectMapper();
      try {
        var json = objectMapper.writeValueAsString(metadata);
        queryBuilder.append(" metadata ").append(json);
      } catch (JsonProcessingException e) {
        throw BaseException.wrapException(
            new DatabaseException(session, "Error during conversion of metadata in JSON format"), e,
            session);
      }
    }

    sessionInternal.command(queryBuilder.toString()).close();
  }

  public SchemaClass setName(DatabaseSession session, final String name) {
    var database = (DatabaseSessionInternal) session;
    if (getName(session).equals(name)) {
      return this;
    }
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    final var wrongCharacter = SchemaShared.checkClassNameIfValid(name);
    var oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      var error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new SchemaException(session, error);
    }
    if (wrongCharacter != null) {
      throw new SchemaException(session,
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + name
              + "'");
    }
    acquireSchemaWriteLock(database);
    try {

      final var cmd = String.format("alter class `%s` name `%s`", this.name, name);
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
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` shortname `%s`", name, shortName);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected SchemaPropertyImpl createPropertyInstance() {
    return new SchemaPropertyRemote(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchemaClass truncateCluster(DatabaseSessionInternal session, String clusterName) {
    var sessionInternal = session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_DELETE, name);
    acquireSchemaReadLock(sessionInternal);
    try {

      final var cmd = String.format("truncate cluster %s", clusterName);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaReadLock(session);
    }

    return this;
  }

  public void setStrictMode(DatabaseSession session, final boolean isStrict) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` strict_mode %s", name, isStrict);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock(database);
    }

  }

  public SchemaClass setDescription(DatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` description ?", name);
      database.command(cmd, iDescription).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public SchemaClass addClusterId(DatabaseSession session, final int clusterId) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (isAbstract(sessionInternal)) {
      throw new SchemaException(sessionInternal,
          "Impossible to associate a cluster to an abstract class class");
    }
    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd = String.format("alter class `%s` add_cluster %d", name, clusterId);
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  public SchemaClass removeClusterId(DatabaseSession session, final int clusterId) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new DatabaseException(sessionInternal,
          " Impossible to remove the last cluster of class '"
              + getName(sessionInternal)
              + "' drop the class instead");
    }

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd = String.format("alter class `%s` remove_cluster %d", name, clusterId);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
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
        throw new SchemaException(sessionInternal,
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      sessionInternal.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  @Override
  public SchemaClass addCluster(DatabaseSession session, final String clusterNameOrId) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (isAbstract(sessionInternal)) {
      throw new SchemaException(sessionInternal,
          "Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd = String.format("alter class `%s` add_cluster `%s`", name, clusterNameOrId);
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaClass setOverSize(DatabaseSession session, final float overSize) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      // FORMAT FLOAT LOCALE AGNOSTIC
      final var cmd = Float.toString(overSize);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public SchemaClass setAbstract(DatabaseSession session, boolean isAbstract) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final var cmd = String.format("alter class `%s` abstract %s", name, isAbstract);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public void removeBaseClassInternal(DatabaseSessionInternal session,
      final SchemaClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      if (subclasses == null) {
        return;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (SchemaClassImpl) baseClass);
      }

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void setSuperClassesInternal(DatabaseSessionInternal session,
      final List<? extends SchemaClass> classes) {
    List<SchemaClassImpl> newSuperClasses = new ArrayList<SchemaClassImpl>();
    SchemaClassImpl cls;
    for (var superClass : classes) {
      cls = (SchemaClassImpl) superClass;

      if (newSuperClasses.contains(cls)) {
        throw new SchemaException(session, "Duplicated superclass '" + cls.getName(session) + "'");
      }

      newSuperClasses.add(cls);
    }

    List<SchemaClassImpl> toAddList = new ArrayList<SchemaClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<SchemaClassImpl> toRemoveList = new ArrayList<SchemaClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (var toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (var addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }


  @Override
  public void getIndexedProperties(DatabaseSessionInternal session,
      Collection<SchemaProperty> indexedProperties) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Collection<SchemaProperty> getIndexedProperties(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public boolean areIndexed(DatabaseSessionInternal session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public boolean areIndexed(DatabaseSessionInternal session, Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSessionInternal session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSessionInternal session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSessionInternal session,
      Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSessionInternal session,
      Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSessionInternal session,
      Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSessionInternal session,
      Collection<String> fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSessionInternal session, String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSessionInternal session,
      String... fields) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Index getClassIndex(DatabaseSessionInternal session, String name) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getClassIndexes(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getClassIndexesInternal(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public void getClassIndexes(DatabaseSession session, Collection<Index> indexes) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public void getIndexesInternal(DatabaseSessionInternal session, Collection<Index> indexes) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<String> getIndexes(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Set<Index> getIndexesInternal(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  protected void addClusterIdToIndexes(DatabaseSessionInternal session, int iId) {
  }
}
