package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 */
public class SchemaPropertyEmbedded extends SchemaPropertyImpl {

  protected SchemaPropertyEmbedded(SchemaClassImpl owner) {
    super(owner);
  }

  protected SchemaPropertyEmbedded(SchemaClassImpl oClassImpl, GlobalProperty global) {
    super(oClassImpl, global);
  }

  public SchemaPropertyImpl setType(DatabaseSession session, final PropertyType type) {
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

  public SchemaProperty setName(DatabaseSession session, final String name) {
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

    var oldName = this.globalRef.getName();
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
  public SchemaPropertyImpl setDescription(DatabaseSession session, final String iDescription) {
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

  public SchemaProperty setCollate(DatabaseSession session, String collate) {
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

      final var oldCollate = this.collate;

      if (iCollate == null) {
        iCollate = DefaultCollate.NAME;
      }

      collate = SQLEngine.getCollate(iCollate);

      if ((this.collate != null && !this.collate.equals(oldCollate))
          || (this.collate == null && oldCollate != null)) {
        final var indexes = owner.getClassIndexesInternal(session);
        final List<Index> indexesToRecreate = new ArrayList<Index>();

        for (var index : indexes) {
          var definition = index.getDefinition();

          final var fields = definition.getFields();
          if (fields.contains(getName(session))) {
            indexesToRecreate.add(index);
          }
        }

        if (!indexesToRecreate.isEmpty()) {
          LogManager.instance()
              .info(
                  this,
                  "Collate value was changed, following indexes will be rebuilt %s",
                  indexesToRecreate);

          final var indexManager =
              session.getMetadata().getIndexManagerInternal();

          for (var indexToRecreate : indexesToRecreate) {
            final var indexMetadata =
                indexToRecreate.getInternal()
                    .loadMetadata(session, indexToRecreate.getConfiguration(session));

            final var fields = indexMetadata.getIndexDefinition().getFields();
            final var fieldsToIndex = fields.toArray(new String[0]);

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

  public SchemaPropertyImpl setCustom(DatabaseSession session, final String name,
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

  public SchemaPropertyImpl setRegexp(DatabaseSession session, final String regexp) {
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

  public SchemaPropertyImpl setLinkedClass(DatabaseSession session, final SchemaClass linkedClass) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType(session));

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

  public SchemaProperty setLinkedType(DatabaseSession session, final PropertyType linkedType) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType(sessionInternal));

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

  public SchemaPropertyImpl setNotNull(DatabaseSession session, final boolean isNotNull) {
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

  public SchemaPropertyImpl setDefaultValue(DatabaseSession session, final String defaultValue) {
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

  public SchemaPropertyImpl setMax(DatabaseSession session, final String max) {
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
      if (this.getType(session).equals(PropertyType.STRING)
          || this.getType(session).equals(PropertyType.LINKBAG)
          || this.getType(session).equals(PropertyType.BINARY)
          || this.getType(session).equals(PropertyType.EMBEDDEDLIST)
          || this.getType(session).equals(PropertyType.EMBEDDEDSET)
          || this.getType(session).equals(PropertyType.LINKLIST)
          || this.getType(session).equals(PropertyType.LINKSET)
          || this.getType(session).equals(PropertyType.LINKBAG)
          || this.getType(session).equals(PropertyType.EMBEDDEDMAP)
          || this.getType(session).equals(PropertyType.LINKMAP)) {
        PropertyType.convert(session, value, Integer.class);
      } else if (this.getType(session).equals(PropertyType.DATE)
          || this.getType(session).equals(PropertyType.BYTE)
          || this.getType(session).equals(PropertyType.SHORT)
          || this.getType(session).equals(PropertyType.INTEGER)
          || this.getType(session).equals(PropertyType.LONG)
          || this.getType(session).equals(PropertyType.FLOAT)
          || this.getType(session).equals(PropertyType.DOUBLE)
          || this.getType(session).equals(PropertyType.DECIMAL)
          || this.getType(session).equals(PropertyType.DATETIME)) {
        PropertyType.convert(session, value, this.getType(session).getDefaultJavaType());
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

  public SchemaPropertyImpl setMin(DatabaseSession session, final String min) {
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

  public SchemaPropertyImpl setReadonly(DatabaseSession session, final boolean isReadonly) {
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

  public SchemaPropertyImpl setMandatory(DatabaseSession session, final boolean isMandatory) {
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
