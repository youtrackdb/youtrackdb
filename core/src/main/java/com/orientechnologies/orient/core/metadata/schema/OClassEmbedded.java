package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 *
 */
public class OClassEmbedded extends OClassImpl {

  protected OClassEmbedded(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OClassEmbedded(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  public OProperty addProperty(
      ODatabaseSessionInternal session, final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    if (type == null) {
      throw new OSchemaException("Property type not defined.");
    }

    if (propertyName == null || propertyName.isEmpty()) {
      throw new OSchemaException("Property name is null or empty");
    }

    validatePropertyName(propertyName);
    if (session.getTransaction().isActive()) {
      throw new OSchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null) {
      OPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      OPropertyImpl.checkSupportLinkedClass(type);
    }

    acquireSchemaWriteLock(session);
    try {
      return (OProperty)
          (Callable<OProperty>) () -> addPropertyInternal(session, propertyName, type,
              linkedType, linkedClass, unsafe);

    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OClassImpl setEncryption(ODatabaseSessionInternal session, final String iValue) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      setEncryptionInternal(session, iValue);
    } finally {
      releaseSchemaWriteLock(session);
    }
    return this;
  }

  protected void setEncryptionInternal(ODatabaseSessionInternal database, final String value) {
    for (int cl : getClusterIds()) {
      final OStorage storage = database.getStorage();
      storage.setClusterAttribute(cl, OCluster.ATTRIBUTES.ENCRYPTION, value);
    }
  }

  @Override
  public OClass setClusterSelection(ODatabaseSession session, final String value) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setClusterSelectionInternal(database, value);
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void setClusterSelectionInternal(ODatabaseSessionInternal session,
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

  public OClassImpl setCustom(ODatabaseSession session, final String name, final String value) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setCustomInternal(database, name, value);
      return this;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void clearCustom(ODatabaseSession session) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      clearCustomInternal(database);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void clearCustomInternal(ODatabaseSessionInternal session) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      customFields = null;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public OClass setSuperClasses(ODatabaseSession session, final List<? extends OClass> classes) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<OClass> toCheck = new ArrayList<OClass>(classes);
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

  public OClass removeBaseClassInternal(ODatabaseSessionInternal session, final OClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (OClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public OClass addSuperClass(ODatabaseSession session, final OClass superClass) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
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

  protected void addSuperClassInternal(ODatabaseSessionInternal database, final OClass superClass) {
    acquireSchemaWriteLock(database);
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate) {
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (OClassImpl) superClass;
      }

      if (cls != null) {

        // CHECK THE USER HAS UPDATE PRIVILEGE AGAINST EXTENDING CLASS
        final OSecurityUser user = database.getUser();
        if (user != null) {
          user.allow(database, ORule.ResourceGeneric.CLASS, cls.getName(), ORole.PERMISSION_UPDATE);
        }

        if (superClasses.contains(superClass)) {
          throw new OSchemaException(
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
  public OClass removeSuperClass(ODatabaseSession session, OClass superClass) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      removeSuperClassInternal(database, superClass);

    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  protected void removeSuperClassInternal(ODatabaseSessionInternal session,
      final OClass superClass) {
    acquireSchemaWriteLock(session);
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate) {
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (OClassImpl) superClass;
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

  protected void setSuperClassesInternal(ODatabaseSessionInternal session,
      final List<? extends OClass> classes) {
    List<OClassImpl> newSuperClasses = new ArrayList<OClassImpl>();
    OClassImpl cls;
    for (OClass superClass : classes) {
      if (superClass instanceof OClassAbstractDelegate) {
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (OClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new OSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<OClassImpl> toAddList = new ArrayList<OClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<OClassImpl> toRemoveList = new ArrayList<OClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (OClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (OClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public OClass setName(ODatabaseSession session, final String name) {
    if (getName().equals(name)) {
      return this;
    }
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new OSchemaException(
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

  protected void setNameInternal(ODatabaseSessionInternal database, final String name) {
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

  public void setDefaultClusterId(ODatabaseSession session, final int defaultClusterId) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    acquireSchemaWriteLock(sessionInternal);
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  public OClass setShortName(ODatabaseSession session, String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) {
        shortName = null;
      }
    }
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setShortNameInternal(database, shortName);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setShortNameInternal(ODatabaseSessionInternal database, final String iShortName) {
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

  protected OPropertyImpl createPropertyInstance() {
    return new OPropertyEmbedded(this);
  }

  public OPropertyImpl addPropertyInternal(
      ODatabaseSessionInternal session, final String name,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    if (name == null || name.isEmpty()) {
      throw new OSchemaException("Found property name null");
    }

    if (!unsafe) {
      checkPersistentPropertyType(session, name, type, linkedClass);
    }

    final OPropertyEmbedded prop;

    // This check are doubled because used by sql commands
    if (linkedType != null) {
      OPropertyImpl.checkLinkTypeSupport(type);
    }

    if (linkedClass != null) {
      OPropertyImpl.checkSupportLinkedClass(type);
    }

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (properties.containsKey(name)) {
        throw new OSchemaException("Class '" + this.name + "' already has property '" + name + "'");
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

  protected OPropertyEmbedded createPropertyInstance(OGlobalProperty global) {
    return new OPropertyEmbedded(this, global);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OClass truncateCluster(ODatabaseSession session, String clusterName) {
    var database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);

    truncateClusterInternal(clusterName, database);

    return this;
  }

  public OClass setStrictMode(ODatabaseSession session, final boolean isStrict) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setStrictModeInternal(database, isStrict);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setStrictModeInternal(ODatabaseSessionInternal session, final boolean iStrict) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      this.strictMode = iStrict;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OClass setDescription(ODatabaseSession session, String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) {
        iDescription = null;
      }
    }
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setDescriptionInternal(database, iDescription);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setDescriptionInternal(ODatabaseSessionInternal session,
      final String iDescription) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();
      this.description = iDescription;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public OClass addClusterId(ODatabaseSession session, final int clusterId) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(database);
    try {
      addClusterIdInternal(database, clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return this;
  }

  public OClass removeClusterId(ODatabaseSession session, final int clusterId) {
    return removeClusterId((ODatabaseSessionInternal) session, clusterId, false);
  }

  public OClass removeClusterId(ODatabaseSessionInternal session, final int clusterId,
      boolean force) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (!force && clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new ODatabaseException(
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
      ODatabaseSessionInternal database, final int clusterToRemove) {
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

  public void dropProperty(ODatabaseSession session, final String propertyName) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      if (!properties.containsKey(propertyName)) {
        throw new OSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");
      }

      OScenarioThreadLocal.executeAsDistributed(
          (Callable<OProperty>)
              () -> {
                dropPropertyInternal(database, propertyName);
                return null;
              });

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void dropPropertyInternal(
      ODatabaseSessionInternal database, final String iPropertyName) {
    if (database.getTransaction().isActive()) {
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    }
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      final OProperty prop = properties.remove(iPropertyName);

      if (prop == null) {
        throw new OSchemaException(
            "Property '" + iPropertyName + "' not found in class " + name + "'");
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public OClass addCluster(ODatabaseSession session, final String clusterNameOrId) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
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

  public OClass setOverSize(ODatabaseSession session, final float overSize) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      setOverSizeInternal(database, overSize);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setOverSizeInternal(ODatabaseSessionInternal database, final float overSize) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      this.overSize = overSize;
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public OClass setAbstract(ODatabaseSession session, boolean isAbstract) {
    final ODatabaseSessionInternal database = (ODatabaseSessionInternal) session;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock(database);
    try {
      setAbstractInternal(database, isAbstract);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return this;
  }

  protected void setCustomInternal(ODatabaseSessionInternal session, final String name,
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

  protected void setAbstractInternal(ODatabaseSessionInternal database, final boolean isAbstract) {
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
        for (OClass clazz : getAllSubclasses()) {
          if (clazz instanceof OClassImpl) {
            addPolymorphicClusterIds(database, (OClassImpl) clazz);
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

  private void tryDropCluster(ODatabaseSessionInternal session, final int clusterId) {
    if (name.toLowerCase(Locale.ENGLISH).equals(session.getClusterNameById(clusterId))) {
      // DROP THE DEFAULT CLUSTER CALLED WITH THE SAME NAME ONLY IF EMPTY
      if (session.countClusterElements(clusterId) == 0) {
        session.dropClusterInternal(clusterId);
      }
    }
  }

  protected void addClusterIdInternal(ODatabaseSessionInternal database, final int clusterId) {
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

  protected void addPolymorphicClusterId(ODatabaseSessionInternal session, int clusterId) {
    if (Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0) {
      return;
    }

    polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = clusterId;
    Arrays.sort(polymorphicClusterIds);

    addClusterIdToIndexes(session, clusterId);

    for (OClassImpl superClass : superClasses) {
      ((OClassEmbedded) superClass).addPolymorphicClusterId(session, clusterId);
    }
  }

  protected void addClusterIdToIndexes(ODatabaseSessionInternal session, int iId) {
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
