package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.YTClassImpl;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.ArrayList;

/**
 *
 */
public class YTPropertyRemote extends YTPropertyImpl {

  YTPropertyRemote(YTClassImpl owner) {
    super(owner);
  }

  public YTPropertyRemote(YTClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public YTPropertyImpl setType(YTDatabaseSession session, final YTType type) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTProperty setName(YTDatabaseSession session, final String name) {
    var database = (YTDatabaseSessionInternal) session;
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
  public YTPropertyImpl setDescription(YTDatabaseSession session, final String iDescription) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTProperty setCollate(YTDatabaseSession session, String collate) {
    if (collate == null) {
      collate = ODefaultCollate.NAME;
    }

    var database = (YTDatabaseSessionInternal) session;
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

  public void clearCustom(YTDatabaseSession session) {
    var database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      final String cmd = String.format("alter property %s custom clear", getFullNameQuoted());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public YTPropertyImpl setCustom(YTDatabaseSession session, final String name,
      final String value) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setRegexp(YTDatabaseSession session, final String regexp) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setLinkedClass(YTDatabaseSession session, final YTClass linkedClass) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTProperty setLinkedType(YTDatabaseSession session, final YTType linkedType) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setNotNull(YTDatabaseSession session, final boolean isNotNull) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setDefaultValue(YTDatabaseSession session, final String defaultValue) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setMax(YTDatabaseSession session, final String max) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setMin(YTDatabaseSession session, final String min) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setReadonly(YTDatabaseSession session, final boolean isReadonly) {
    var database = (YTDatabaseSessionInternal) session;
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

  public YTPropertyImpl setMandatory(YTDatabaseSession session, final boolean isMandatory) {
    var database = (YTDatabaseSessionInternal) session;
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
  public OIndex createIndex(YTDatabaseSession session, String iType) {
    return owner.createIndex(session, getFullName(), iType, globalRef.getName());
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType) {
    return createIndex(session, iType.toString());
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iType, YTEntityImpl metadata) {
    return owner.createIndex(session,
        getFullName(), iType, null, metadata, new String[]{globalRef.getName()});
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType, YTEntityImpl metadata) {
    return createIndex(session, iType.name(), metadata);
  }

  @Override
  public YTPropertyImpl dropIndexes(YTDatabaseSessionInternal session) {
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
