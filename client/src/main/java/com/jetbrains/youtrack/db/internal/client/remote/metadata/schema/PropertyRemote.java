package com.jetbrains.youtrack.db.internal.client.remote.metadata.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.common.comparator.CaseInsentiveComparator;
import com.jetbrains.youtrack.db.internal.common.util.Collections;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.PropertyIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class PropertyRemote extends PropertyImpl {

  PropertyRemote(SchemaClassImpl owner) {
    super(owner);
  }

  public PropertyRemote(SchemaClassImpl oClassImpl, GlobalProperty global) {
    super(oClassImpl, global);
  }

  public PropertyImpl setType(DatabaseSession session, final PropertyType type) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter property %s type %s", getFullNameQuoted(), quoteString(type.toString()));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public Property setName(DatabaseSession session, final String name) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s name %s", getFullNameQuoted(), quoteString(name));
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  @Override
  public PropertyImpl setDescription(DatabaseSession session, final String iDescription) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter property %s description %s", getFullNameQuoted(), quoteString(iDescription));
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public Property setCollate(DatabaseSession session, String collate) {
    if (collate == null) {
      collate = DefaultCollate.NAME;
    }

    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s collate %s", getFullNameQuoted(), quoteString(collate));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public void clearCustom(DatabaseSession session) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter property %s custom clear", getFullNameQuoted());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public PropertyImpl setCustom(DatabaseSession session, final String name,
      final String value) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter property %s custom %s=%s", getFullNameQuoted(), name, quoteString(value));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public PropertyImpl setRegexp(DatabaseSession session, final String regexp) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s regexp %s", getFullNameQuoted(), quoteString(regexp));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public PropertyImpl setLinkedClass(DatabaseSession session, final SchemaClass linkedClass) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType());

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter property %s linkedclass %s",
              getFullNameQuoted(), quoteString(linkedClass.getName()));
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public Property setLinkedType(DatabaseSession session, final PropertyType linkedType) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType());

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter property %s linkedtype %s",
              getFullNameQuoted(), quoteString(linkedType.toString()));
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public PropertyImpl setNotNull(DatabaseSession session, final boolean isNotNull) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s notnull %s", getFullNameQuoted(), isNotNull);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public PropertyImpl setDefaultValue(DatabaseSession session, final String defaultValue) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format(
              "alter property %s default %s", getFullNameQuoted(), quoteString(defaultValue));
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public PropertyImpl setMax(DatabaseSession session, final String max) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s max %s", getFullNameQuoted(), quoteString(max));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public PropertyImpl setMin(DatabaseSession session, final String min) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s min %s", getFullNameQuoted(), quoteString(min));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public PropertyImpl setReadonly(DatabaseSession session, final boolean isReadonly) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s readonly %s", getFullNameQuoted(), isReadonly);
      database.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public PropertyImpl setMandatory(DatabaseSession session, final boolean isMandatory) {
    var database = (DatabaseSessionInternal) session;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd =
          String.format("alter property %s mandatory %s", getFullNameQuoted(), isMandatory);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  @Override
  public String createIndex(DatabaseSession session, String iType) {
    var indexName = getFullName();
    owner.createIndex(session, indexName, iType, globalRef.getName());
    return indexName;
  }

  @Override
  public String createIndex(DatabaseSession session, INDEX_TYPE iType) {
    return createIndex(session, iType.toString());
  }

  @Override
  public String createIndex(DatabaseSession session, String iType, Map<String, ?> metadata) {
    var indexName = getFullName();
    owner.createIndex(session,
        indexName, iType, null, metadata, new String[]{globalRef.getName()});
    return indexName;
  }

  @Override
  public String createIndex(DatabaseSession session, INDEX_TYPE iType, Map<String, ?> metadata) {
    return createIndex(session, iType.name(), metadata);
  }

  @Override
  public PropertyImpl dropIndexes(DatabaseSessionInternal session) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

    final IndexManagerAbstract indexManager = session.getMetadata().getIndexManagerInternal();

    final ArrayList<Index> relatedIndexes = new ArrayList<Index>();
    for (final Index index : indexManager.getClassIndexes(session, owner.getName())) {
      final IndexDefinition definition = index.getDefinition();

      if (Collections.indexOf(
          definition.getFields(), globalRef.getName(), new CaseInsentiveComparator())
          > -1) {
        if (definition instanceof PropertyIndexDefinition) {
          relatedIndexes.add(index);
        } else {
          throw new IllegalArgumentException(
              "This operation applicable only for property indexes. "
                  + index.getName()
                  + " is "
                  + index.getDefinition());
        }
      }
    }

    for (final Index index : relatedIndexes) {
      session.getMetadata().getIndexManagerInternal().dropIndex(session, index.getName());
    }

    return this;
  }

  @Override
  public Collection<String> getAllIndexes(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public Collection<Index> getAllIndexesInternal(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }

  @Override
  public boolean isIndexed(DatabaseSession session) {
    throw new UnsupportedOperationException("Not supported in remote environment");
  }
}
