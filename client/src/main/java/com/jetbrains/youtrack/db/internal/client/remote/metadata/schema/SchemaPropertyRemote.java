package com.jetbrains.youtrack.db.internal.client.remote.metadata.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.comparator.CaseInsentiveComparator;
import com.jetbrains.youtrack.db.internal.common.util.Collections;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.PropertyIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaPropertyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class SchemaPropertyRemote extends SchemaPropertyImpl {

  SchemaPropertyRemote(SchemaClassImpl owner) {
    super(owner);
  }

  public SchemaPropertyRemote(SchemaClassImpl oClassImpl, GlobalProperty global) {
    super(oClassImpl, global);
  }

  public SchemaPropertyImpl setType(DatabaseSession session, final PropertyType type) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter property %s type %s", getFullNameQuoted(sessionInternal),
              quoteString(type.toString()));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  public SchemaProperty setName(DatabaseSession session, final String name) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s name %s", getFullNameQuoted(sessionInternal),
              quoteString(name));
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  @Override
  public SchemaPropertyImpl setDescription(DatabaseSession session, final String iDescription) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter property %s description %s", getFullNameQuoted(sessionInternal),
              quoteString(iDescription));
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  public SchemaProperty setCollate(DatabaseSession session, String collate) {
    if (collate == null) {
      collate = DefaultCollate.NAME;
    }

    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s collate %s", getFullNameQuoted(sessionInternal),
              quoteString(collate));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public void clearCustom(DatabaseSession session) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd = String.format("alter property %s custom clear",
          getFullNameQuoted(sessionInternal));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  public SchemaPropertyImpl setCustom(DatabaseSession session, final String name,
      final String value) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter property %s custom %s=%s", getFullNameQuoted(sessionInternal), name,
              quoteString(value));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaPropertyImpl setRegexp(DatabaseSession session, final String regexp) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s regexp %s", getFullNameQuoted(sessionInternal),
              quoteString(regexp));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  public SchemaPropertyImpl setLinkedClass(DatabaseSession session, final SchemaClass linkedClass) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType(sessionInternal));

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter property %s linkedclass %s",
              getFullNameQuoted(sessionInternal),
              quoteString(linkedClass.getName(sessionInternal)));
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaProperty setLinkedType(DatabaseSession session, final PropertyType linkedType) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType(sessionInternal));

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter property %s linkedtype %s",
              getFullNameQuoted(sessionInternal), quoteString(linkedType.toString()));
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaPropertyImpl setNotNull(DatabaseSession session, final boolean isNotNull) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s notnull %s", getFullNameQuoted(sessionInternal),
              isNotNull);
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  public SchemaPropertyImpl setDefaultValue(DatabaseSession session, final String defaultValue) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format(
              "alter property %s default %s", getFullNameQuoted(sessionInternal),
              quoteString(defaultValue));
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaPropertyImpl setMax(DatabaseSession session, final String max) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s max %s", getFullNameQuoted(sessionInternal),
              quoteString(max));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaPropertyImpl setMin(DatabaseSession session, final String min) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s min %s", getFullNameQuoted(sessionInternal),
              quoteString(min));
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaPropertyImpl setReadonly(DatabaseSession session, final boolean isReadonly) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s readonly %s", getFullNameQuoted(sessionInternal),
              isReadonly);
      sessionInternal.command(cmd).close();

    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  public SchemaPropertyImpl setMandatory(DatabaseSession session, final boolean isMandatory) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      final var cmd =
          String.format("alter property %s mandatory %s", getFullNameQuoted(sessionInternal),
              isMandatory);
      sessionInternal.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  @Override
  public String createIndex(DatabaseSession session, String iType) {
    var indexName = getFullName(session);
    owner.createIndex(session, indexName, iType, globalRef.getName());
    return indexName;
  }

  @Override
  public String createIndex(DatabaseSession session, INDEX_TYPE iType) {
    return createIndex(session, iType.toString());
  }

  @Override
  public String createIndex(DatabaseSession session, String iType, Map<String, ?> metadata) {
    var indexName = getFullName(session);
    owner.createIndex(session,
        indexName, iType, null, metadata, new String[]{globalRef.getName()});
    return indexName;
  }

  @Override
  public String createIndex(DatabaseSession session, INDEX_TYPE iType, Map<String, ?> metadata) {
    return createIndex(session, iType.name(), metadata);
  }

  @Override
  public SchemaPropertyImpl dropIndexes(DatabaseSessionInternal session) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var relatedIndexes = new ArrayList<Index>();
    for (final var index : indexManager.getClassIndexes(session, owner.getName(session))) {
      final var definition = index.getDefinition();

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

    for (final var index : relatedIndexes) {
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
