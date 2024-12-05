package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.common.util.OArrays;
import com.jetbrains.youtrack.db.internal.core.db.OScenarioThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSchemaException;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser;
import com.jetbrains.youtrack.db.internal.core.storage.OCluster;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 *
 */
public class YTClassEmbedded extends YTClassImpl {

  protected YTClassEmbedded(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected YTClassEmbedded(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  public YTProperty addProperty(
      YTDatabaseSessionInternal session, final String propertyName,
      final YTType type,
      final YTType linkedType,
      final YTClass linkedClass,
      final boolean unsafe) {
    if (type == null) {
      throw new YTSchemaException("Property type not defined.");
    }

    if (propertyName == null || propertyName.isEmpty()) {
      throw new YTSchemaException("Property name is null or empty");
    }

    validatePropertyName(propertyName);
    if (session.getTransaction().isActive()) {
      throw new YTSchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null) {
      YTPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      YTPropertyImpl.checkSupportLinkedClass(type);
    }

    acquireSchemaWriteLock(session);
    try {
      return addPropertyInternal(session, propertyName, type,
          linkedType, linkedClass, unsafe);

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTClassImpl setEncryption(YTDatabaseSessionInternal session, final String iValue) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      setEncryptionInternal(session, iValue);
    } finally {
      releaseSchemaWriteLock(session);
    }
    return this;
  }

  protected void setEncryptionInternal(YTDatabaseSessionInternal database, final String value) {
    for (int cl : getClusterIds()) {
      final OStorage storage = database.getStorage();
      storage.setClusterAttribute(cl, OCluster.ATTRIBUTES.ENCRYPTION, value);
    }
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session, final String value) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setClusterSelectionInternal(database, value);
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void setClusterSelectionInternal(YTDatabaseSessionInternal session,
      final String clusterSelection) {
    // AVOID TO CHECK THIS IN LOCK TO AVOID RE-GENERATION OF IMMUTABLE SCHEMAS
    if (this.clusterSelection.getName().equals(clusterSelection))
    // NO CHANGES
    {
      return;
    }

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      this.clusterSelection = owner.getClusterSelectionFactory().newInstance(clusterSelection);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTClassImpl setCustom(YTDatabaseSession session, final String name, final String value) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setCustomInternal(database, name, value);
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void clearCustom(YTDatabaseSession session) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      clearCustomInternal(database);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void clearCustomInternal(YTDatabaseSessionInternal session) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      customFields = null;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public YTClass setSuperClasses(YTDatabaseSession session, final List<? extends YTClass> classes) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<YTClass> toCheck = new ArrayList<YTClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock(database);
    try {
      setSuperClassesInternal(database, classes);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public YTClass removeBaseClassInternal(YTDatabaseSessionInternal session,
      final YTClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (YTClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public YTClass addSuperClass(YTDatabaseSession session, final YTClass superClass) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkParametersConflict(database, superClass);
    acquireSchemaWriteLock(database);
    try {
      addSuperClassInternal(database, superClass);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  protected void addSuperClassInternal(YTDatabaseSessionInternal database,
      final YTClass superClass) {
    acquireSchemaWriteLock(database);
    try {
      final YTClassImpl cls;

      if (superClass instanceof YTClassAbstractDelegate) {
        cls = (YTClassImpl) ((YTClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (YTClassImpl) superClass;
      }

      if (cls != null) {

        // CHECK THE USER HAS UPDATE PRIVILEGE AGAINST EXTENDING CLASS
        final YTSecurityUser user = database.getUser();
        if (user != null) {
          user.allow(database, ORule.ResourceGeneric.CLASS, cls.getName(), ORole.PERMISSION_UPDATE);
        }

        if (superClasses.contains(superClass)) {
          throw new YTSchemaException(
              "Class: '"
                  + this.getName()
                  + "' already has the class '"
                  + superClass.getName()
                  + "' as superclass");
        }

        cls.addBaseClass(database, this);
        superClasses.add(cls);
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public YTClass removeSuperClass(YTDatabaseSession session, YTClass superClass) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      removeSuperClassInternal(database, superClass);

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  protected void removeSuperClassInternal(YTDatabaseSessionInternal session,
      final YTClass superClass) {
    acquireSchemaWriteLock(session);
    try {
      final YTClassImpl cls;

      if (superClass instanceof YTClassAbstractDelegate) {
        cls = (YTClassImpl) ((YTClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (YTClassImpl) superClass;
      }

      if (superClasses.contains(cls)) {
        if (cls != null) {
          cls.removeBaseClassInternal(session, this);
        }

        superClasses.remove(superClass);
      }
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void setSuperClassesInternal(YTDatabaseSessionInternal session,
      final List<? extends YTClass> classes) {
    List<YTClassImpl> newSuperClasses = new ArrayList<YTClassImpl>();
    YTClassImpl cls;
    for (YTClass superClass : classes) {
      if (superClass instanceof YTClassAbstractDelegate) {
        cls = (YTClassImpl) ((YTClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (YTClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new YTSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<YTClassImpl> toAddList = new ArrayList<YTClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<YTClassImpl> toRemoveList = new ArrayList<YTClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (YTClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (YTClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public YTClass setName(YTDatabaseSession session, final String name) {
    if (getName().equals(name)) {
      return this;
    }
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    YTClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new YTSchemaException(error);
    }
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + name
              + "'");
    }
    acquireSchemaWriteLock(database);
    try {
      setNameInternal(database, name);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setNameInternal(YTDatabaseSessionInternal database, final String name) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();
      final String oldName = this.name;
      owner.changeClassName(database, this.name, name, this);
      this.name = name;
      renameCluster(database, oldName, this.name);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void setDefaultClusterId(YTDatabaseSession session, final int defaultClusterId) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    acquireSchemaWriteLock(sessionInternal);
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  public YTClass setShortName(YTDatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setShortNameInternal(database, shortName);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setShortNameInternal(YTDatabaseSessionInternal database, final String iShortName) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      String oldName = null;

      if (this.shortName != null) {
        oldName = this.shortName;
      }

      owner.changeClassName(database, oldName, iShortName, this);

      this.shortName = iShortName;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected YTPropertyImpl createPropertyInstance() {
    return new YTPropertyEmbedded(this);
  }

  public YTPropertyImpl addPropertyInternal(
      YTDatabaseSessionInternal session, final String name,
      final YTType type,
      final YTType linkedType,
      final YTClass linkedClass,
      final boolean unsafe) {
    if (name == null || name.isEmpty()) {
      throw new YTSchemaException("Found property name null");
    }

    if (!unsafe) {
      checkPersistentPropertyType(session, name, type, linkedClass);
    }

    final YTPropertyEmbedded prop;

    // This check are doubled because used by sql commands
    if (linkedType != null) {
      YTPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      YTPropertyImpl.checkSupportLinkedClass(type);
    }

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (properties.containsKey(name)) {
        throw new YTSchemaException(
            "Class '" + this.name + "' already has property '" + name + "'");
      }

      OGlobalProperty global = owner.findOrCreateGlobalProperty(name, type);

      prop = createPropertyInstance(global);

      properties.put(name, prop);

      if (linkedType != null) {
        prop.setLinkedTypeInternal(session, linkedType);
      } else if (linkedClass != null) {
        prop.setLinkedClassInternal(session, linkedClass);
      }
    } finally {
      releaseSchemaWriteLock(session);
    }

    if (prop != null && !unsafe) {
      fireDatabaseMigration(session, name, type);
    }

    return prop;
  }

  protected YTPropertyEmbedded createPropertyInstance(OGlobalProperty global) {
    return new YTPropertyEmbedded(this, global);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTClass truncateCluster(YTDatabaseSession session, String clusterName) {
    var database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);

    truncateClusterInternal(clusterName, database);

    return this;
  }

  public YTClass setStrictMode(YTDatabaseSession session, final boolean isStrict) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setStrictModeInternal(database, isStrict);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setStrictModeInternal(YTDatabaseSessionInternal session, final boolean iStrict) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      this.strictMode = iStrict;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTClass setDescription(YTDatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setDescriptionInternal(database, iDescription);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setDescriptionInternal(YTDatabaseSessionInternal session,
      final String iDescription) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();
      this.description = iDescription;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public YTClass addClusterId(YTDatabaseSession session, final int clusterId) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new YTSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(database);
    try {
      addClusterIdInternal(database, clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public YTClass removeClusterId(YTDatabaseSession session, final int clusterId) {
    return removeClusterId((YTDatabaseSessionInternal) session, clusterId, false);
  }

  public YTClass removeClusterId(YTDatabaseSessionInternal session, final int clusterId,
      boolean force) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (!force && clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new YTDatabaseException(
          " Impossible to remove the last cluster of class '"
              + getName()
              + "' drop the class instead");
    }

    acquireSchemaWriteLock(session);
    try {
      removeClusterIdInternal(session, clusterId);
    } finally {
      releaseSchemaWriteLock(session);
    }

    return this;
  }

  protected void removeClusterIdInternal(
      YTDatabaseSessionInternal database, final int clusterToRemove) {
    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      boolean found = false;
      for (int clusterId : clusterIds) {
        if (clusterId == clusterToRemove) {
          found = true;
          break;
        }
      }

      if (found) {
        final int[] newClusterIds = new int[clusterIds.length - 1];
        for (int i = 0, k = 0; i < clusterIds.length; ++i) {
          if (clusterIds[i] == clusterToRemove)
          // JUMP IT
          {
            continue;
          }

          newClusterIds[k] = clusterIds[i];
          k++;
        }
        clusterIds = newClusterIds;

        removePolymorphicClusterId(database, clusterToRemove);
      }

      if (defaultClusterId == clusterToRemove) {
        if (clusterIds.length >= 1) {
          defaultClusterId = clusterIds[0];
        } else {
          defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
        }
      }

      ((OSchemaEmbedded) owner).removeClusterForClass(database, clusterToRemove);
    } finally {
      releaseSchemaWriteLock(database);
    }

  }

  public void dropProperty(YTDatabaseSession session, final String propertyName) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new YTSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      OScenarioThreadLocal.executeAsDistributed(
          (Callable<YTProperty>)
              () -> {
                dropPropertyInternal(database, propertyName);
                return null;
              });

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void dropPropertyInternal(
      YTDatabaseSessionInternal database, final String iPropertyName) {
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      final YTProperty prop = properties.remove(iPropertyName);

      if (prop == null) {
        throw new YTSchemaException(
            "Property '" + iPropertyName + "' not found in class " + name + "'");
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public YTClass addCluster(YTDatabaseSession session, final String clusterNameOrId) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new YTSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(database);
    try {
      final int clusterId = owner.createClusterIfNeeded(database, clusterNameOrId);
      addClusterIdInternal(database, clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  public YTClass setOverSize(YTDatabaseSession session, final float overSize) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      setOverSizeInternal(database, overSize);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setOverSizeInternal(YTDatabaseSessionInternal database, final float overSize) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      this.overSize = overSize;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public YTClass setAbstract(YTDatabaseSession session, boolean isAbstract) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setAbstractInternal(database, isAbstract);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setCustomInternal(YTDatabaseSessionInternal session, final String name,
      final String value) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (customFields == null) {
        customFields = new HashMap<String, String>();
      }
      if (value == null || "null".equalsIgnoreCase(value)) {
        customFields.remove(name);
      } else {
        customFields.put(name, value);
      }
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void setAbstractInternal(YTDatabaseSessionInternal database, final boolean isAbstract) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      if (isAbstract) {
        // SWITCH TO ABSTRACT
        if (defaultClusterId != NOT_EXISTENT_CLUSTER_ID) {
          // CHECK
          if (count(database) > 0) {
            throw new IllegalStateException(
                "Cannot set the class as abstract because contains records.");
          }

          tryDropCluster(database, defaultClusterId);
          for (int clusterId : getClusterIds()) {
            tryDropCluster(database, clusterId);
            removePolymorphicClusterId(database, clusterId);
            ((OSchemaEmbedded) owner).removeClusterForClass(database, clusterId);
          }

          setClusterIds(new int[]{NOT_EXISTENT_CLUSTER_ID});

          defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
        }
      } else {
        if (!abstractClass) {
          return;
        }

        int clusterId = database.getClusterIdByName(name);
        if (clusterId == -1) {
          clusterId = database.addCluster(name);
        }

        this.defaultClusterId = clusterId;
        this.clusterIds[0] = this.defaultClusterId;
        this.polymorphicClusterIds = Arrays.copyOf(clusterIds, clusterIds.length);
        for (YTClass clazz : getAllSubclasses()) {
          if (clazz instanceof YTClassImpl) {
            addPolymorphicClusterIds(database, (YTClassImpl) clazz);
          } else {
            OLogManager.instance()
                .warn(this, "Warning: cannot set polymorphic cluster IDs for class " + name);
          }
        }
      }

      this.abstractClass = isAbstract;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  private void tryDropCluster(YTDatabaseSessionInternal session, final int clusterId) {
    if (name.toLowerCase(Locale.ENGLISH).equals(session.getClusterNameById(clusterId))) {
      // DROP THE DEFAULT CLUSTER CALLED WITH THE SAME NAME ONLY IF EMPTY
      if (session.countClusterElements(clusterId) == 0) {
        session.dropClusterInternal(clusterId);
      }
    }
  }

  protected void addClusterIdInternal(YTDatabaseSessionInternal database, final int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      owner.checkClusterCanBeAdded(clusterId, this);

      for (int currId : clusterIds) {
        if (currId == clusterId)
        // ALREADY ADDED
        {
          return;
        }
      }

      clusterIds = OArrays.copyOf(clusterIds, clusterIds.length + 1);
      clusterIds[clusterIds.length - 1] = clusterId;
      Arrays.sort(clusterIds);

      addPolymorphicClusterId(database, clusterId);

      if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) {
        defaultClusterId = clusterId;
      }

      ((OSchemaEmbedded) owner).addClusterForClass(database, clusterId, this);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void addPolymorphicClusterId(YTDatabaseSessionInternal session, int clusterId) {
    if (Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0) {
      return;
    }

    polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = clusterId;
    Arrays.sort(polymorphicClusterIds);

    addClusterIdToIndexes(session, clusterId);

    for (YTClassImpl superClass : superClasses) {
      ((YTClassEmbedded) superClass).addPolymorphicClusterId(session, clusterId);
    }
  }

  protected void addClusterIdToIndexes(YTDatabaseSessionInternal session, int iId) {
    var clusterName = session.getClusterNameById(iId);
    final List<String> indexesToAdd = new ArrayList<String>();

    for (OIndex index : getIndexes(session)) {
      indexesToAdd.add(index.getName());
    }

    final OIndexManagerAbstract indexManager =
        session.getMetadata().getIndexManagerInternal();
    for (String indexName : indexesToAdd) {
      indexManager.addClusterToIndex(session, clusterName, indexName);
    }
  }
}
