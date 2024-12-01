package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class OPropertyEmbedded extends OPropertyImpl {

  protected OPropertyEmbedded(OClassImpl owner) {
    super(owner);
  }

  protected OPropertyEmbedded(OClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public OPropertyImpl setType(ODatabaseSession session, final OType type) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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
  protected void setTypeInternal(ODatabaseSessionInternal session, final OType iType) {
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

  public OProperty setName(ODatabaseSession session, final String name) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setNameInternal(sessionInternal, name);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setNameInternal(ODatabaseSessionInternal session, final String name) {
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
  public OPropertyImpl setDescription(ODatabaseSession session, final String iDescription) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setDescriptionInternal(sessionInternal, iDescription);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setDescriptionInternal(ODatabaseSessionInternal session,
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

  public OProperty setCollate(ODatabaseSession session, String collate) {
    if (collate == null) {
      collate = ODefaultCollate.NAME;
    }

    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setCollateInternal(sessionInternal, collate);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setCollateInternal(ODatabaseSessionInternal session, String iCollate) {
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

            final ODocument metadata = new ODocument();
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

  public void clearCustom(ODatabaseSession session) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      clearCustomInternal(sessionInternal);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  protected void clearCustomInternal(ODatabaseSessionInternal session) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      customFields = null;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OPropertyImpl setCustom(ODatabaseSession session, final String name, final String value) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setCustomInternal(sessionInternal, name, value);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setCustomInternal(ODatabaseSessionInternal session, final String iName,
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

  public OPropertyImpl setRegexp(ODatabaseSession session, final String regexp) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setRegexpInternal(sessionInternal, regexp);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setRegexpInternal(ODatabaseSessionInternal session, final String regexp) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      this.regexp = regexp;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OPropertyImpl setLinkedClass(ODatabaseSession session, final OClass linkedClass) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  protected void setLinkedClassInternal(ODatabaseSessionInternal session,
      final OClass iLinkedClass) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.linkedClass = iLinkedClass;

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OProperty setLinkedType(ODatabaseSession session, final OType linkedType) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  protected void setLinkedTypeInternal(ODatabaseSessionInternal session, final OType iLinkedType) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);
      this.linkedType = iLinkedType;

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OPropertyImpl setNotNull(ODatabaseSession session, final boolean isNotNull) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setNotNullInternal(sessionInternal, isNotNull);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setNotNullInternal(ODatabaseSessionInternal session, final boolean isNotNull) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      notNull = isNotNull;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OPropertyImpl setDefaultValue(ODatabaseSession session, final String defaultValue) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  protected void setDefaultValueInternal(ODatabaseSessionInternal session,
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

  public OPropertyImpl setMax(ODatabaseSession session, final String max) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  private void checkCorrectLimitValue(ODatabaseSessionInternal session, final String value) {
    if (value != null) {
      if (this.getType().equals(OType.STRING)
          || this.getType().equals(OType.LINKBAG)
          || this.getType().equals(OType.BINARY)
          || this.getType().equals(OType.EMBEDDEDLIST)
          || this.getType().equals(OType.EMBEDDEDSET)
          || this.getType().equals(OType.LINKLIST)
          || this.getType().equals(OType.LINKSET)
          || this.getType().equals(OType.LINKBAG)
          || this.getType().equals(OType.EMBEDDEDMAP)
          || this.getType().equals(OType.LINKMAP)) {
        OType.convert(session, value, Integer.class);
      } else if (this.getType().equals(OType.DATE)
          || this.getType().equals(OType.BYTE)
          || this.getType().equals(OType.SHORT)
          || this.getType().equals(OType.INTEGER)
          || this.getType().equals(OType.LONG)
          || this.getType().equals(OType.FLOAT)
          || this.getType().equals(OType.DOUBLE)
          || this.getType().equals(OType.DECIMAL)
          || this.getType().equals(OType.DATETIME)) {
        OType.convert(session, value, this.getType().getDefaultJavaType());
      }
    }
  }

  protected void setMaxInternal(ODatabaseSessionInternal sesisson, final String max) {
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

  public OPropertyImpl setMin(ODatabaseSession session, final String min) {
    var sessionInternal = (ODatabaseSessionInternal) session;
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

  protected void setMinInternal(ODatabaseSessionInternal session, final String min) {
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

  public OPropertyImpl setReadonly(ODatabaseSession session, final boolean isReadonly) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setReadonlyInternal(sessionInternal, isReadonly);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setReadonlyInternal(ODatabaseSessionInternal session, final boolean isReadonly) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.readonly = isReadonly;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OPropertyImpl setMandatory(ODatabaseSession session, final boolean isMandatory) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMandatoryInternal(sessionInternal, isMandatory);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setMandatoryInternal(ODatabaseSessionInternal session, final boolean isMandatory) {
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
