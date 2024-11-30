package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;

/**
 *
 */
public class OPropertyRemote extends OPropertyImpl {

  OPropertyRemote(OClassImpl owner) {
    super(owner);
  }

  public OPropertyRemote(OClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public OPropertyImpl setType(ODatabaseSession session, final OType type) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
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

  public OProperty setName(ODatabaseSession session, final String name) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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
  public OPropertyImpl setDescription(ODatabaseSession session, final String iDescription) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OProperty setCollate(ODatabaseSession session, String collate) {
    if (collate == null) {
      collate = ODefaultCollate.NAME;
    }

    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public void clearCustom(ODatabaseSession session) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter property %s custom clear", getFullNameQuoted());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public OPropertyImpl setCustom(ODatabaseSession session, final String name, final String value) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setRegexp(ODatabaseSession session, final String regexp) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setLinkedClass(ODatabaseSession session, final OClass linkedClass) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OProperty setLinkedType(ODatabaseSession session, final OType linkedType) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setNotNull(ODatabaseSession session, final boolean isNotNull) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setDefaultValue(ODatabaseSession session, final String defaultValue) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setMax(ODatabaseSession session, final String max) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setMin(ODatabaseSession session, final String min) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setReadonly(ODatabaseSession session, final boolean isReadonly) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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

  public OPropertyImpl setMandatory(ODatabaseSession session, final boolean isMandatory) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

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
  public OIndex createIndex(ODatabaseSession session, String iType) {
    return owner.createIndex(session, getFullName(), iType, globalRef.getName());
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, INDEX_TYPE iType) {
    return createIndex(session, iType.toString());
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, String iType, ODocument metadata) {
    return owner.createIndex(session,
        getFullName(), iType, null, metadata, new String[]{globalRef.getName()});
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, INDEX_TYPE iType, ODocument metadata) {
    return createIndex(session, iType.name(), metadata);
  }

  @Override
  public OPropertyImpl dropIndexes(ODatabaseSessionInternal session) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    final OIndexManagerAbstract indexManager = session.getMetadata().getIndexManagerInternal();

    final ArrayList<OIndex> relatedIndexes = new ArrayList<OIndex>();
    for (final OIndex index : indexManager.getClassIndexes(session, owner.getName())) {
      final OIndexDefinition definition = index.getDefinition();

      if (OCollections.indexOf(
          definition.getFields(), globalRef.getName(), new OCaseInsentiveComparator())
          > -1) {
        if (definition instanceof OPropertyIndexDefinition) {
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

    for (final OIndex index : relatedIndexes) {
      session.getMetadata().getIndexManagerInternal().dropIndex(session, index.getName());
    }

    return this;
  }
}
