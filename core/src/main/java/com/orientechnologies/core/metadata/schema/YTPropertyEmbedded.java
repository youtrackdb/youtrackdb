package com.orientechnologies.core.metadata.schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.collate.OCollate;
import com.orientechnologies.core.collate.ODefaultCollate;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexDefinition;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.index.OIndexMetadata;
import com.orientechnologies.core.metadata.security.ORole;
import com.orientechnologies.core.metadata.security.ORule;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.OSQLEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class YTPropertyEmbedded extends YTPropertyImpl {

  protected YTPropertyEmbedded(YTClassImpl owner) {
    super(owner);
  }

  protected YTPropertyEmbedded(YTClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public YTPropertyImpl setType(YTDatabaseSession session, final YTType type) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setTypeInternal(sessionInternal, type);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    owner.fireDatabaseMigration(sessionInternal, globalRef.getName(), globalRef.getType());

    return this;
  }

  /**
   * Change the type. It checks for compatibility between the change of type.
   *
   * @param session
   * @param iType
   */
  protected void setTypeInternal(YTDatabaseSessionInternal session, final YTType iType) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      if (iType == globalRef.getType())
      // NO CHANGES
      {
        return;
      }

      if (!iType.getCastable().contains(globalRef.getType())) {
        throw new IllegalArgumentException(
            "Cannot change property type from " + globalRef.getType() + " to " + iType);
      }

      this.globalRef = owner.owner.findOrCreateGlobalProperty(this.globalRef.getName(), iType);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTProperty setName(YTDatabaseSession session, final String name) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setNameInternal(sessionInternal, name);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setNameInternal(YTDatabaseSessionInternal session, final String name) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    String oldName = this.globalRef.getName();
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      owner.renameProperty(oldName, name);
      this.globalRef = owner.owner.findOrCreateGlobalProperty(name, this.globalRef.getType());
    } finally {
      releaseSchemaWriteLock(session);
    }
    owner.firePropertyNameMigration(session, oldName, name, this.globalRef.getType());
  }

  @Override
  public YTPropertyImpl setDescription(YTDatabaseSession session, final String iDescription) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setDescriptionInternal(sessionInternal, iDescription);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setDescriptionInternal(YTDatabaseSessionInternal session,
      final String iDescription) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.description = iDescription;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTProperty setCollate(YTDatabaseSession session, String collate) {
    if (collate == null) {
      collate = ODefaultCollate.NAME;
    }

    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setCollateInternal(sessionInternal, collate);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setCollateInternal(YTDatabaseSessionInternal session, String iCollate) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      final OCollate oldCollate = this.collate;

      if (iCollate == null) {
        iCollate = ODefaultCollate.NAME;
      }

      collate = OSQLEngine.getCollate(iCollate);

      if ((this.collate != null && !this.collate.equals(oldCollate))
          || (this.collate == null && oldCollate != null)) {
        final Set<OIndex> indexes = owner.getClassIndexes(session);
        final List<OIndex> indexesToRecreate = new ArrayList<OIndex>();

        for (OIndex index : indexes) {
          OIndexDefinition definition = index.getDefinition();

          final List<String> fields = definition.getFields();
          if (fields.contains(getName())) {
            indexesToRecreate.add(index);
          }
        }

        if (!indexesToRecreate.isEmpty()) {
          OLogManager.instance()
              .info(
                  this,
                  "Collate value was changed, following indexes will be rebuilt %s",
                  indexesToRecreate);

          final OIndexManagerAbstract indexManager =
              session.getMetadata().getIndexManagerInternal();

          for (OIndex indexToRecreate : indexesToRecreate) {
            final OIndexMetadata indexMetadata =
                indexToRecreate.getInternal()
                    .loadMetadata(indexToRecreate.getConfiguration(session));

            final YTEntityImpl metadata = new YTEntityImpl();
            metadata.fromMap(indexToRecreate.getMetadata());

            final List<String> fields = indexMetadata.getIndexDefinition().getFields();
            final String[] fieldsToIndex = fields.toArray(new String[0]);

            indexManager.dropIndex(session, indexMetadata.getName());
            owner.createIndex(session,
                indexMetadata.getName(),
                indexMetadata.getType(),
                null,
                metadata,
                indexMetadata.getAlgorithm(), fieldsToIndex);
          }
        }
      }
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public void clearCustom(YTDatabaseSession session) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      clearCustomInternal(sessionInternal);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  protected void clearCustomInternal(YTDatabaseSessionInternal session) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      customFields = null;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setCustom(YTDatabaseSession session, final String name,
      final String value) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setCustomInternal(sessionInternal, name, value);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setCustomInternal(YTDatabaseSessionInternal session, final String iName,
      final String iValue) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      if (customFields == null) {
        customFields = new HashMap<>();
      }
      if (iValue == null || "null".equalsIgnoreCase(iValue)) {
        customFields.remove(iName);
      } else {
        customFields.put(iName, iValue);
      }
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setRegexp(YTDatabaseSession session, final String regexp) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setRegexpInternal(sessionInternal, regexp);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setRegexpInternal(YTDatabaseSessionInternal session, final String regexp) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      this.regexp = regexp;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setLinkedClass(YTDatabaseSession session, final YTClass linkedClass) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType());

    acquireSchemaWriteLock(sessionInternal);
    try {
      setLinkedClassInternal(sessionInternal, linkedClass);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setLinkedClassInternal(YTDatabaseSessionInternal session,
      final YTClass iLinkedClass) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.linkedClass = iLinkedClass;

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTProperty setLinkedType(YTDatabaseSession session, final YTType linkedType) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType());

    acquireSchemaWriteLock(sessionInternal);
    try {
      setLinkedTypeInternal(sessionInternal, linkedType);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setLinkedTypeInternal(YTDatabaseSessionInternal session,
      final YTType iLinkedType) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);
      this.linkedType = iLinkedType;

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setNotNull(YTDatabaseSession session, final boolean isNotNull) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setNotNullInternal(sessionInternal, isNotNull);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setNotNullInternal(YTDatabaseSessionInternal session, final boolean isNotNull) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      notNull = isNotNull;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setDefaultValue(YTDatabaseSession session, final String defaultValue) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setDefaultValueInternal(sessionInternal, defaultValue);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on setting default value", e);
      throw e;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setDefaultValueInternal(YTDatabaseSessionInternal session,
      final String defaultValue) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.defaultValue = defaultValue;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setMax(YTDatabaseSession session, final String max) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkCorrectLimitValue(sessionInternal, max);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMaxInternal(sessionInternal, max);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  private void checkCorrectLimitValue(YTDatabaseSessionInternal session, final String value) {
    if (value != null) {
      if (this.getType().equals(YTType.STRING)
          || this.getType().equals(YTType.LINKBAG)
          || this.getType().equals(YTType.BINARY)
          || this.getType().equals(YTType.EMBEDDEDLIST)
          || this.getType().equals(YTType.EMBEDDEDSET)
          || this.getType().equals(YTType.LINKLIST)
          || this.getType().equals(YTType.LINKSET)
          || this.getType().equals(YTType.LINKBAG)
          || this.getType().equals(YTType.EMBEDDEDMAP)
          || this.getType().equals(YTType.LINKMAP)) {
        YTType.convert(session, value, Integer.class);
      } else if (this.getType().equals(YTType.DATE)
          || this.getType().equals(YTType.BYTE)
          || this.getType().equals(YTType.SHORT)
          || this.getType().equals(YTType.INTEGER)
          || this.getType().equals(YTType.LONG)
          || this.getType().equals(YTType.FLOAT)
          || this.getType().equals(YTType.DOUBLE)
          || this.getType().equals(YTType.DECIMAL)
          || this.getType().equals(YTType.DATETIME)) {
        YTType.convert(session, value, this.getType().getDefaultJavaType());
      }
    }
  }

  protected void setMaxInternal(YTDatabaseSessionInternal sesisson, final String max) {
    sesisson.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sesisson);
    try {
      checkEmbedded(sesisson);

      checkForDateFormat(sesisson, max);
      this.max = max;
    } finally {
      releaseSchemaWriteLock(sesisson);
    }
  }

  public YTPropertyImpl setMin(YTDatabaseSession session, final String min) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkCorrectLimitValue(sessionInternal, min);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMinInternal(sessionInternal, min);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setMinInternal(YTDatabaseSessionInternal session, final String min) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      checkForDateFormat(session, min);
      this.min = min;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setReadonly(YTDatabaseSession session, final boolean isReadonly) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setReadonlyInternal(sessionInternal, isReadonly);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setReadonlyInternal(YTDatabaseSessionInternal session, final boolean isReadonly) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.readonly = isReadonly;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTPropertyImpl setMandatory(YTDatabaseSession session, final boolean isMandatory) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMandatoryInternal(sessionInternal, isMandatory);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setMandatoryInternal(YTDatabaseSessionInternal session,
      final boolean isMandatory) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);
      this.mandatory = isMandatory;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }
}
