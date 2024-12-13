package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class PropertyEmbedded extends PropertyImpl {

  protected PropertyEmbedded(SchemaClassImpl owner) {
    super(owner);
  }

  protected PropertyEmbedded(SchemaClassImpl oClassImpl, GlobalProperty global) {
    super(oClassImpl, global);
  }

  public PropertyImpl setType(DatabaseSession session, final PropertyType type) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

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
  protected void setTypeInternal(DatabaseSessionInternal session, final PropertyType iType) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

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

  public Property setName(DatabaseSession session, final String name) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setNameInternal(sessionInternal, name);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setNameInternal(DatabaseSessionInternal session, final String name) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

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
  public PropertyImpl setDescription(DatabaseSession session, final String iDescription) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setDescriptionInternal(sessionInternal, iDescription);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setDescriptionInternal(DatabaseSessionInternal session,
      final String iDescription) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.description = iDescription;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public Property setCollate(DatabaseSession session, String collate) {
    if (collate == null) {
      collate = DefaultCollate.NAME;
    }

    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setCollateInternal(sessionInternal, collate);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setCollateInternal(DatabaseSessionInternal session, String iCollate) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      final Collate oldCollate = this.collate;

      if (iCollate == null) {
        iCollate = DefaultCollate.NAME;
      }

      collate = SQLEngine.getCollate(iCollate);

      if ((this.collate != null && !this.collate.equals(oldCollate))
          || (this.collate == null && oldCollate != null)) {
        final Set<Index> indexes = owner.getClassIndexesInternal(session);
        final List<Index> indexesToRecreate = new ArrayList<Index>();

        for (Index index : indexes) {
          IndexDefinition definition = index.getDefinition();

          final List<String> fields = definition.getFields();
          if (fields.contains(getName())) {
            indexesToRecreate.add(index);
          }
        }

        if (!indexesToRecreate.isEmpty()) {
          LogManager.instance()
              .info(
                  this,
                  "Collate value was changed, following indexes will be rebuilt %s",
                  indexesToRecreate);

          final IndexManagerAbstract indexManager =
              session.getMetadata().getIndexManagerInternal();

          for (Index indexToRecreate : indexesToRecreate) {
            final IndexMetadata indexMetadata =
                indexToRecreate.getInternal()
                    .loadMetadata(indexToRecreate.getConfiguration(session));


            final List<String> fields = indexMetadata.getIndexDefinition().getFields();
            final String[] fieldsToIndex = fields.toArray(new String[0]);

            indexManager.dropIndex(session, indexMetadata.getName());
            owner.createIndex(session,
                indexMetadata.getName(),
                indexMetadata.getType(),
                null,
                indexToRecreate.getMetadata(),
                indexMetadata.getAlgorithm(), fieldsToIndex);
          }
        }
      }
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public void clearCustom(DatabaseSession session) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      clearCustomInternal(sessionInternal);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  protected void clearCustomInternal(DatabaseSessionInternal session) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      customFields = null;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setCustom(DatabaseSession session, final String name,
      final String value) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setCustomInternal(sessionInternal, name, value);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setCustomInternal(DatabaseSessionInternal session, final String iName,
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

  public PropertyImpl setRegexp(DatabaseSession session, final String regexp) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setRegexpInternal(sessionInternal, regexp);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setRegexpInternal(DatabaseSessionInternal session, final String regexp) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      this.regexp = regexp;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setLinkedClass(DatabaseSession session, final SchemaClass linkedClass) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType());

    acquireSchemaWriteLock(sessionInternal);
    try {
      setLinkedClassInternal(sessionInternal, linkedClass);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setLinkedClassInternal(DatabaseSessionInternal session,
      final SchemaClass iLinkedClass) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.linkedClass = iLinkedClass;

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public Property setLinkedType(DatabaseSession session, final PropertyType linkedType) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType());

    acquireSchemaWriteLock(sessionInternal);
    try {
      setLinkedTypeInternal(sessionInternal, linkedType);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setLinkedTypeInternal(DatabaseSessionInternal session,
      final PropertyType iLinkedType) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);
      this.linkedType = iLinkedType;

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setNotNull(DatabaseSession session, final boolean isNotNull) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setNotNullInternal(sessionInternal, isNotNull);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
    return this;
  }

  protected void setNotNullInternal(DatabaseSessionInternal session, final boolean isNotNull) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      notNull = isNotNull;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setDefaultValue(DatabaseSession session, final String defaultValue) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setDefaultValueInternal(sessionInternal, defaultValue);
    } catch (Exception e) {
      LogManager.instance().error(this, "Error on setting default value", e);
      throw e;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setDefaultValueInternal(DatabaseSessionInternal session,
      final String defaultValue) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.defaultValue = defaultValue;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setMax(DatabaseSession session, final String max) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    checkCorrectLimitValue(sessionInternal, max);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMaxInternal(sessionInternal, max);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  private void checkCorrectLimitValue(DatabaseSessionInternal session, final String value) {
    if (value != null) {
      if (this.getType().equals(PropertyType.STRING)
          || this.getType().equals(PropertyType.LINKBAG)
          || this.getType().equals(PropertyType.BINARY)
          || this.getType().equals(PropertyType.EMBEDDEDLIST)
          || this.getType().equals(PropertyType.EMBEDDEDSET)
          || this.getType().equals(PropertyType.LINKLIST)
          || this.getType().equals(PropertyType.LINKSET)
          || this.getType().equals(PropertyType.LINKBAG)
          || this.getType().equals(PropertyType.EMBEDDEDMAP)
          || this.getType().equals(PropertyType.LINKMAP)) {
        PropertyType.convert(session, value, Integer.class);
      } else if (this.getType().equals(PropertyType.DATE)
          || this.getType().equals(PropertyType.BYTE)
          || this.getType().equals(PropertyType.SHORT)
          || this.getType().equals(PropertyType.INTEGER)
          || this.getType().equals(PropertyType.LONG)
          || this.getType().equals(PropertyType.FLOAT)
          || this.getType().equals(PropertyType.DOUBLE)
          || this.getType().equals(PropertyType.DECIMAL)
          || this.getType().equals(PropertyType.DATETIME)) {
        PropertyType.convert(session, value, this.getType().getDefaultJavaType());
      }
    }
  }

  protected void setMaxInternal(DatabaseSessionInternal sesisson, final String max) {
    sesisson.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sesisson);
    try {
      checkEmbedded(sesisson);

      checkForDateFormat(sesisson, max);
      this.max = max;
    } finally {
      releaseSchemaWriteLock(sesisson);
    }
  }

  public PropertyImpl setMin(DatabaseSession session, final String min) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    checkCorrectLimitValue(sessionInternal, min);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMinInternal(sessionInternal, min);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setMinInternal(DatabaseSessionInternal session, final String min) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      checkForDateFormat(session, min);
      this.min = min;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setReadonly(DatabaseSession session, final boolean isReadonly) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setReadonlyInternal(sessionInternal, isReadonly);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setReadonlyInternal(DatabaseSessionInternal session, final boolean isReadonly) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);

      this.readonly = isReadonly;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public PropertyImpl setMandatory(DatabaseSession session, final boolean isMandatory) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    acquireSchemaWriteLock(sessionInternal);
    try {
      setMandatoryInternal(sessionInternal, isMandatory);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  protected void setMandatoryInternal(DatabaseSessionInternal session,
      final boolean isMandatory) {
    session.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded(session);
      this.mandatory = isMandatory;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }
}
