/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.concur.resource.CloseableInStorage;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.types.ModifiableInteger;
import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.MetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.db.ScenarioThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewCreationListener;
import com.jetbrains.youtrack.db.internal.core.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaNotCreatedException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.ClusterSelectionFactory;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;

/**
 * Shared schema class. It's shared by all the database instances that point to the same storage.
 */
public abstract class SchemaShared implements CloseableInStorage {

  private static final int NOT_EXISTENT_CLUSTER_ID = -1;
  public static final int CURRENT_VERSION_NUMBER = 4;
  public static final int VERSION_NUMBER_V4 = 4;
  // this is needed for guarantee the compatibility to 2.0-M1 and 2.0-M2 no changed associated with
  // it
  public static final int VERSION_NUMBER_V5 = 5;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected final Map<String, SchemaClass> classes = new HashMap<String, SchemaClass>();
  protected final Int2ObjectOpenHashMap<SchemaClass> clustersToClasses = new Int2ObjectOpenHashMap<>();

  protected final Map<String, SchemaView> views = new HashMap<String, SchemaView>();
  protected final Int2ObjectOpenHashMap<SchemaView> clustersToViews = new Int2ObjectOpenHashMap<>();

  private final ClusterSelectionFactory clusterSelectionFactory = new ClusterSelectionFactory();

  private final ModifiableInteger modificationCounter = new ModifiableInteger();
  private final List<GlobalProperty> properties = new ArrayList<>();
  private final Map<String, GlobalProperty> propertiesByNameType = new HashMap<>();
  private IntOpenHashSet blobClusters = new IntOpenHashSet();
  private volatile int version = 0;
  private volatile RID identity;
  protected volatile ImmutableSchema snapshot;

  protected static Set<String> internalClasses = new HashSet<String>();

  static {
    internalClasses.add("ouser");
    internalClasses.add("orole");
    internalClasses.add("osecuritypolicy");
    internalClasses.add("oidentity");
    internalClasses.add("ofunction");
    internalClasses.add("osequence");
    internalClasses.add("otrigger");
    internalClasses.add("oschedule");
    internalClasses.add("orids");
  }

  protected static final class ClusterIdsAreEmptyException extends Exception {

  }

  public SchemaShared() {
  }

  public static Character checkClassNameIfValid(String iName) throws SchemaException {
    if (iName == null) {
      throw new IllegalArgumentException("Name is null");
    }

    //    iName = iName.trim();
    //
    //    final int nameSize = iName.length();
    //
    //    if (nameSize == 0)
    //      throw new IllegalArgumentException("Name is empty");
    //
    //    for (int i = 0; i < nameSize; ++i) {
    //      final char c = iName.charAt(i);
    //      if (c == ':' || c == ',' || c == ';' || c == ' ' || c == '@' || c == '=' || c == '.' ||
    // c == '#')
    //        // INVALID CHARACTER
    //        return c;
    //    }

    return null;
  }

  public static Character checkFieldNameIfValid(String iName) {
    if (iName == null) {
      throw new IllegalArgumentException("Name is null");
    }

    iName = iName.trim();

    final int nameSize = iName.length();

    if (nameSize == 0) {
      throw new IllegalArgumentException("Name is empty");
    }

    for (int i = 0; i < nameSize; ++i) {
      final char c = iName.charAt(i);
      if (c == ':' || c == ',' || c == ';' || c == ' ' || c == '=')
      // INVALID CHARACTER
      {
        return c;
      }
    }

    return null;
  }

  public static Character checkIndexNameIfValid(String iName) {
    if (iName == null) {
      throw new IllegalArgumentException("Name is null");
    }

    iName = iName.trim();

    final int nameSize = iName.length();

    if (nameSize == 0) {
      throw new IllegalArgumentException("Name is empty");
    }

    for (int i = 0; i < nameSize; ++i) {
      final char c = iName.charAt(i);
      if (c == ':' || c == ',' || c == ';' || c == ' ' || c == '=')
      // INVALID CHARACTER
      {
        return c;
      }
    }

    return null;
  }

  public ImmutableSchema makeSnapshot(DatabaseSessionInternal database) {
    if (snapshot == null) {
      // Is null only in the case that is asked while the schema is created
      // all the other cases are already protected by a write lock
      acquireSchemaReadLock();
      try {
        if (snapshot == null) {
          snapshot = new ImmutableSchema(this, database);
        }
      } finally {
        releaseSchemaReadLock();
      }
    }
    return snapshot;
  }

  public void forceSnapshot(DatabaseSessionInternal database) {
    acquireSchemaReadLock();
    try {
      snapshot = new ImmutableSchema(this, database);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public ClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  public int countClasses(DatabaseSessionInternal database) {
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return classes.size();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int countViews(DatabaseSessionInternal database) {
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return views.size();
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Callback invoked when the schema is loaded, after all the initializations.
   */
  public void onPostIndexManagement(DatabaseSessionInternal session) {
    for (SchemaClass c : classes.values()) {
      if (c instanceof SchemaClassImpl) {
        ((SchemaClassImpl) c).onPostIndexManagement(session);
      }
    }
    for (SchemaClass c : views.values()) {
      if (c instanceof SchemaClassImpl) {
        ((SchemaClassImpl) c).onPostIndexManagement(session);
      }
    }
  }

  public SchemaClass createClass(DatabaseSessionInternal database, final String className) {
    return createClass(database, className, null, (int[]) null);
  }

  public SchemaClass createClass(
      DatabaseSessionInternal database, final String iClassName, final SchemaClass iSuperClass) {
    return createClass(database, iClassName, iSuperClass, null);
  }

  public SchemaClass createClass(
      DatabaseSessionInternal database, String iClassName, SchemaClass... superClasses) {
    return createClass(database, iClassName, null, superClasses);
  }

  public SchemaClass getOrCreateClass(DatabaseSessionInternal database, final String iClassName) {
    return getOrCreateClass(database, iClassName, (SchemaClass) null);
  }

  public SchemaClass getOrCreateClass(
      DatabaseSessionInternal database, final String iClassName, final SchemaClass superClass) {
    return getOrCreateClass(
        database, iClassName,
        superClass == null ? new SchemaClass[0] : new SchemaClass[]{superClass});
  }

  public abstract SchemaClass getOrCreateClass(
      DatabaseSessionInternal database, final String iClassName, final SchemaClass... superClasses);

  public SchemaClass createAbstractClass(DatabaseSessionInternal database, final String className) {
    return createClass(database, className, null, new int[]{-1});
  }

  public SchemaClass createAbstractClass(
      DatabaseSessionInternal database, final String className, final SchemaClass superClass) {
    return createClass(database, className, superClass, new int[]{-1});
  }

  public SchemaClass createAbstractClass(
      DatabaseSessionInternal database, String iClassName, SchemaClass... superClasses) {
    return createClass(database, iClassName, new int[]{-1}, superClasses);
  }

  public SchemaClass createClass(
      DatabaseSessionInternal database,
      final String className,
      final SchemaClass superClass,
      int[] clusterIds) {
    return createClass(database, className, clusterIds, superClass);
  }

  public abstract SchemaClass createClass(
      DatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      SchemaClass... superClasses);

  public abstract SchemaClass createClass(
      DatabaseSessionInternal database,
      final String className,
      int clusters,
      SchemaClass... superClasses);

  public abstract SchemaView createView(
      DatabaseSessionInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata);

  public abstract SchemaView createView(DatabaseSessionInternal database, ViewConfig cfg);

  public abstract SchemaView createView(
      DatabaseSessionInternal database, ViewConfig cfg, ViewCreationListener listener)
      throws UnsupportedOperationException;

  public abstract void checkEmbedded();

  void checkClusterCanBeAdded(int clusterId, SchemaClass cls) {
    acquireSchemaReadLock();
    try {
      if (clusterId < 0) {
        return;
      }

      if (blobClusters.contains(clusterId)) {
        throw new SchemaException("Cluster with id " + clusterId + " already belongs to Blob");
      }

      final SchemaClass existingCls = clustersToClasses.get(clusterId);

      if (existingCls != null && (cls == null || !cls.equals(existingCls))) {
        throw new SchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to the class '"
                + clustersToClasses.get(clusterId)
                + "'");
      }

      final SchemaView existingView = clustersToViews.get(clusterId);

      if (existingView != null && (cls == null || !cls.equals(existingView))) {
        throw new SchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to the view '"
                + clustersToViews.get(clusterId)
                + "'");
      }

    } finally {
      releaseSchemaReadLock();
    }
  }

  public SchemaClass getClassByClusterId(int clusterId) {
    acquireSchemaReadLock();
    try {
      return clustersToClasses.get(clusterId);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public SchemaView getViewByClusterId(int clusterId) {
    acquireSchemaReadLock();
    try {
      return clustersToViews.get(clusterId);
    } finally {
      releaseSchemaReadLock();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.schema.metadata.Schema#dropClass(java.lang.String)
   */
  public abstract void dropClass(DatabaseSessionInternal database, final String className);

  public abstract void dropView(DatabaseSessionInternal database, final String viewName);

  /**
   * Reloads the schema inside a storage's shared lock.
   */
  public void reload(DatabaseSessionInternal database) {
    lock.writeLock().lock();
    try {
      database.executeInTx(
          () -> {
            identity = new RecordId(
                database.getStorageInfo().getConfiguration().getSchemaRecordId());

            EntityImpl document = database.load(identity);
            fromStream(database, document);
            forceSnapshot(database);
          });
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean existsClass(final String iClassName) {
    if (iClassName == null) {
      return false;
    }

    acquireSchemaReadLock();
    try {
      return classes.containsKey(iClassName.toLowerCase(Locale.ENGLISH));
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean existsView(final String viewName) {
    if (viewName == null) {
      return false;
    }

    acquireSchemaReadLock();
    try {
      return views.containsKey(viewName.toLowerCase(Locale.ENGLISH));
    } finally {
      releaseSchemaReadLock();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.schema.metadata.Schema#getClass(java.lang.Class)
   */
  public SchemaClass getClass(final Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    return getClass(iClass.getSimpleName());
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.schema.metadata.Schema#getClass(java.lang.String)
   */
  public SchemaClass getClass(final String iClassName) {
    if (iClassName == null) {
      return null;
    }

    acquireSchemaReadLock();
    try {
      return classes.get(iClassName.toLowerCase(Locale.ENGLISH));
    } finally {
      releaseSchemaReadLock();
    }
  }

  public SchemaView getView(final String viewName) {
    if (viewName == null) {
      return null;
    }

    acquireSchemaReadLock();
    try {
      return views.get(viewName.toLowerCase(Locale.ENGLISH));
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void acquireSchemaReadLock() {
    lock.readLock().lock();
  }

  public void releaseSchemaReadLock() {
    lock.readLock().unlock();
  }

  public void acquireSchemaWriteLock(DatabaseSessionInternal database) {
    database.startExclusiveMetadataChange();
    lock.writeLock().lock();
    modificationCounter.increment();
  }

  public void releaseSchemaWriteLock(DatabaseSessionInternal database) {
    releaseSchemaWriteLock(database, true);
  }

  public void releaseSchemaWriteLock(DatabaseSessionInternal database, final boolean iSave) {
    int count;
    try {
      if (modificationCounter.intValue() == 1) {
        // if it is embedded storage modification of schema is done by internal methods otherwise it
        // is done by
        // by sql commands and we need to reload local replica

        if (iSave) {
          if (database.getStorage() instanceof AbstractPaginatedStorage) {
            saveInternal(database);
          } else {
            reload(database);
          }
        } else {
          snapshot = new ImmutableSchema(this, database);
        }
        version++;
      }
    } finally {
      modificationCounter.decrement();
      count = modificationCounter.intValue();
      lock.writeLock().unlock();
      database.endExclusiveMetadataChange();
    }
    assert count >= 0;

    if (count == 0 && database.isRemote()) {
      database.getStorage().reload(database);
    }
  }

  void changeClassName(
      DatabaseSessionInternal database,
      final String oldName,
      final String newName,
      final SchemaClass cls) {

    if (oldName != null && oldName.equalsIgnoreCase(newName)) {
      throw new IllegalArgumentException(
          "Class '" + oldName + "' cannot be renamed with the same name");
    }

    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      if (newName != null
          && (classes.containsKey(newName.toLowerCase(Locale.ENGLISH))
          || views.containsKey(newName.toLowerCase(Locale.ENGLISH)))) {
        throw new IllegalArgumentException("Class '" + newName + "' is already present in schema");
      }

      if (oldName != null) {
        classes.remove(oldName.toLowerCase(Locale.ENGLISH));
      }
      if (newName != null) {
        classes.put(newName.toLowerCase(Locale.ENGLISH), cls);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  void changeViewName(
      DatabaseSessionInternal database,
      final String oldName,
      final String newName,
      final SchemaView view) {

    if (oldName != null && oldName.equalsIgnoreCase(newName)) {
      throw new IllegalArgumentException(
          "View '" + oldName + "' cannot be renamed with the same name");
    }

    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      if (newName != null
          && (classes.containsKey(newName.toLowerCase(Locale.ENGLISH))
          || views.containsKey(newName.toLowerCase(Locale.ENGLISH)))) {
        throw new IllegalArgumentException("View '" + newName + "' is already present in schema");
      }

      if (oldName != null) {
        views.remove(oldName.toLowerCase(Locale.ENGLISH));
      }
      if (newName != null) {
        views.put(newName.toLowerCase(Locale.ENGLISH), view);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  /**
   * Binds EntityImpl to POJO.
   */
  public void fromStream(DatabaseSessionInternal session, EntityImpl document) {
    lock.writeLock().lock();
    modificationCounter.increment();
    try {
      // READ CURRENT SCHEMA VERSION
      final Integer schemaVersion = document.field("schemaVersion");
      if (schemaVersion == null) {
        LogManager.instance()
            .error(
                this,
                "Database's schema is empty! Recreating the system classes and allow the opening of"
                    + " the database but double check the integrity of the database",
                null);
        return;
      } else if (schemaVersion != CURRENT_VERSION_NUMBER && VERSION_NUMBER_V5 != schemaVersion) {
        // VERSION_NUMBER_V5 is needed for guarantee the compatibility to 2.0-M1 and 2.0-M2 no
        // changed associated with it
        // HANDLE SCHEMA UPGRADE
        throw new ConfigurationException(
            "Database schema is different. Please export your old database with the previous"
                + " version of YouTrackDB and reimport it using the current one.");
      }

      properties.clear();
      propertiesByNameType.clear();
      List<EntityImpl> globalProperties = document.field("globalProperties");
      boolean hasGlobalProperties = false;
      if (globalProperties != null) {
        hasGlobalProperties = true;
        for (EntityImpl oDocument : globalProperties) {
          GlobalPropertyImpl prop = new GlobalPropertyImpl();
          prop.fromDocument(oDocument);
          ensurePropertiesSize(prop.getId());
          properties.set(prop.getId(), prop);
          propertiesByNameType.put(prop.getName() + "|" + prop.getType().name(), prop);
        }
      }
      // REGISTER ALL THE CLASSES
      clustersToClasses.clear();

      final Map<String, SchemaClass> newClasses = new HashMap<String, SchemaClass>();
      final Map<String, SchemaView> newViews = new HashMap<String, SchemaView>();

      Collection<EntityImpl> storedClasses = document.field("classes");
      for (EntityImpl c : storedClasses) {
        String name = c.field("name");

        SchemaClassImpl cls;
        if (classes.containsKey(name.toLowerCase(Locale.ENGLISH))) {
          cls = (SchemaClassImpl) classes.get(name.toLowerCase(Locale.ENGLISH));
          cls.fromStream(c);
        } else {
          cls = createClassInstance(name);
          cls.fromStream(c);
        }

        newClasses.put(cls.getName().toLowerCase(Locale.ENGLISH), cls);

        if (cls.getShortName() != null) {
          newClasses.put(cls.getShortName().toLowerCase(Locale.ENGLISH), cls);
        }

        addClusterClassMap(cls);
      }

      classes.clear();
      classes.putAll(newClasses);

      // REBUILD THE INHERITANCE TREE
      Collection<String> superClassNames;
      String legacySuperClassName;
      List<SchemaClass> superClasses;
      SchemaClass superClass;

      for (EntityImpl c : storedClasses) {
        superClassNames = c.field("superClasses");
        legacySuperClassName = c.field("superClass");
        if (superClassNames == null) {
          superClassNames = new ArrayList<String>();
        }
        //        else
        //          superClassNames = new HashSet<String>(superClassNames);

        if (legacySuperClassName != null && !superClassNames.contains(legacySuperClassName)) {
          superClassNames.add(legacySuperClassName);
        }

        if (!superClassNames.isEmpty()) {
          // HAS A SUPER CLASS or CLASSES
          SchemaClassImpl cls =
              (SchemaClassImpl) classes.get(((String) c.field("name")).toLowerCase(Locale.ENGLISH));
          superClasses = new ArrayList<SchemaClass>(superClassNames.size());
          for (String superClassName : superClassNames) {

            superClass = classes.get(superClassName.toLowerCase(Locale.ENGLISH));

            if (superClass == null) {
              throw new ConfigurationException(
                  "Super class '"
                      + superClassName
                      + "' was declared in class '"
                      + cls.getName()
                      + "' but was not found in schema. Remove the dependency or create the class"
                      + " to continue.");
            }
            superClasses.add(superClass);
          }
          cls.setSuperClassesInternal(session, superClasses);
        }
      }

      // VIEWS

      clustersToViews.clear();
      Collection<EntityImpl> storedViews = document.field("views");
      if (storedViews != null) {
        for (EntityImpl v : storedViews) {

          String name = v.field("name");

          SchemaViewImpl view;
          if (views.containsKey(name.toLowerCase(Locale.ENGLISH))) {
            view = (SchemaViewImpl) views.get(name.toLowerCase(Locale.ENGLISH));
            view.fromStream(v);
          } else {
            view = createViewInstance(name);
            view.fromStream(v);
          }

          newViews.put(view.getName().toLowerCase(Locale.ENGLISH), view);

          if (view.getShortName() != null) {
            newViews.put(view.getShortName().toLowerCase(Locale.ENGLISH), view);
          }

          addClusterViewMap(view);
        }
      }

      views.clear();
      views.putAll(newViews);

      if (document.containsField("blobClusters")) {
        blobClusters = new IntOpenHashSet((Set<Integer>) document.field("blobClusters"));
      }

      if (!hasGlobalProperties) {
        DatabaseSessionInternal database = DatabaseRecordThreadLocal.instance().get();
        if (database.getStorage() instanceof AbstractPaginatedStorage) {
          saveInternal(database);
        }
      }

    } finally {
      version++;
      modificationCounter.decrement();
      lock.writeLock().unlock();
    }
  }

  protected abstract SchemaClassImpl createClassInstance(String name);

  protected abstract SchemaViewImpl createViewInstance(String name);

  public EntityImpl toNetworkStream() {
    lock.readLock().lock();
    try {
      EntityImpl document = new EntityImpl();
      document.setTrackingChanges(false);
      document.field("schemaVersion", CURRENT_VERSION_NUMBER);

      Set<EntityImpl> cc = new HashSet<EntityImpl>();
      for (SchemaClass c : classes.values()) {
        cc.add(((SchemaClassImpl) c).toNetworkStream());
      }

      document.field("classes", cc, PropertyType.EMBEDDEDSET);

      // TODO: this should trigger a netowork protocol version change
      Set<EntityImpl> vv = new HashSet<EntityImpl>();
      for (SchemaView v : views.values()) {
        vv.add(((SchemaViewImpl) v).toNetworkStream());
      }

      document.field("views", vv, PropertyType.EMBEDDEDSET);

      List<EntityImpl> globalProperties = new ArrayList<EntityImpl>();
      for (GlobalProperty globalProperty : properties) {
        if (globalProperty != null) {
          globalProperties.add(((GlobalPropertyImpl) globalProperty).toDocument());
        }
      }
      document.field("globalProperties", globalProperties, PropertyType.EMBEDDEDLIST);
      document.field("blobClusters", blobClusters, PropertyType.EMBEDDEDSET);
      return document;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Binds POJO to EntityImpl.
   */
  public EntityImpl toStream(@Nonnull DatabaseSessionInternal db) {
    lock.readLock().lock();
    try {
      EntityImpl document = db.load(identity);
      document.field("schemaVersion", CURRENT_VERSION_NUMBER);

      // This steps is needed because in classes there are duplicate due to aliases
      Set<SchemaClassImpl> realClases = new HashSet<SchemaClassImpl>();
      for (SchemaClass c : classes.values()) {
        realClases.add(((SchemaClassImpl) c));
      }

      Set<EntityImpl> classesDocuments = new HashSet<EntityImpl>();
      for (SchemaClassImpl c : realClases) {
        classesDocuments.add(c.toStream());
      }
      document.field("classes", classesDocuments, PropertyType.EMBEDDEDSET);

      // This steps is needed because in views there are duplicate due to aliases
      Set<SchemaViewImpl> realViews = new HashSet<SchemaViewImpl>();
      for (SchemaView v : views.values()) {
        realViews.add(((SchemaViewImpl) v));
      }

      Set<EntityImpl> viewsDocuments = new HashSet<EntityImpl>();
      for (SchemaClassImpl c : realViews) {
        viewsDocuments.add(c.toStream());
      }
      document.field("views", viewsDocuments, PropertyType.EMBEDDEDSET);

      List<EntityImpl> globalProperties = new ArrayList<EntityImpl>();
      for (GlobalProperty globalProperty : properties) {
        if (globalProperty != null) {
          globalProperties.add(((GlobalPropertyImpl) globalProperty).toDocument());
        }
      }
      document.field("globalProperties", globalProperties, PropertyType.EMBEDDEDLIST);
      document.field("blobClusters", blobClusters, PropertyType.EMBEDDEDSET);
      return document;
    } finally {
      lock.readLock().unlock();
    }
  }

  public Collection<SchemaClass> getClasses(DatabaseSessionInternal database) {
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<SchemaClass>(classes.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<SchemaView> getViews(DatabaseSessionInternal database) {
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<SchemaView>(views.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<SchemaClass> getClassesRelyOnCluster(
      DatabaseSessionInternal database, final String clusterName) {
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final int clusterId = database.getClusterIdByName(clusterName);
      final Set<SchemaClass> result = new HashSet<SchemaClass>();
      for (SchemaClass c : classes.values()) {
        if (ArrayUtils.contains(c.getPolymorphicClusterIds(), clusterId)) {
          result.add(c);
        }
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public SchemaShared load(DatabaseSessionInternal database) {

    lock.writeLock().lock();
    try {
      identity = new RecordId(database.getStorageInfo().getConfiguration().getSchemaRecordId());
      if (!identity.isValid()) {
        throw new SchemaNotCreatedException("Schema is not created and cannot be loaded");
      }
      database.executeInTx(
          () -> {
            EntityImpl document = database.load(identity);
            fromStream(database, document);
          });
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void create(final DatabaseSessionInternal database) {
    lock.writeLock().lock();
    try {
      EntityImpl document =
          database.computeInTx(
              () -> database.save(new EntityImpl(), MetadataDefault.CLUSTER_INTERNAL_NAME));
      this.identity = document.getIdentity();
      database.getStorage().setSchemaRecordId(document.getIdentity().toString());
      snapshot = new ImmutableSchema(this, database);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
  }

  @Deprecated
  public int getVersion() {
    return version;
  }

  public RID getIdentity() {
    acquireSchemaReadLock();
    try {
      return identity;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public GlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size()) {
      return null;
    }
    return properties.get(id);
  }

  public GlobalProperty createGlobalProperty(
      final String name, final PropertyType type, final Integer id) {
    GlobalProperty global;
    if (id < properties.size() && (global = properties.get(id)) != null) {
      if (!global.getName().equals(name) || !global.getType().equals(type)) {
        throw new SchemaException("A property with id " + id + " already exist ");
      }
      return global;
    }

    global = new GlobalPropertyImpl(name, type, id);
    ensurePropertiesSize(id);
    properties.set(id, global);
    propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    return global;
  }

  public List<GlobalProperty> getGlobalProperties() {
    return Collections.unmodifiableList(properties);
  }

  protected GlobalProperty findOrCreateGlobalProperty(final String name, final PropertyType type) {
    GlobalProperty global = propertiesByNameType.get(name + "|" + type.name());
    if (global == null) {
      int id = properties.size();
      global = new GlobalPropertyImpl(name, type, id);
      properties.add(id, global);
      propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    }
    return global;
  }

  protected boolean executeThroughDistributedStorage(DatabaseSessionInternal database) {
    return !database.isLocalEnv();
  }

  private void saveInternal(DatabaseSessionInternal database) {

    var tx = database.getTransaction();
    if (tx.isActive()) {
      throw new SchemaException(
          "Cannot change the schema while a transaction is active. Schema changes are not"
              + " transactional");
    }

    ScenarioThreadLocal.executeAsDistributed(
        () -> {
          database.executeInTx(() -> {
            EntityImpl document = toStream(database);
            database.save(document, MetadataDefault.CLUSTER_INTERNAL_NAME);
          });
          return null;
        });

    forceSnapshot(database);

    for (MetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
      listener.onSchemaUpdate(database, database.getName(), this);
    }
  }

  protected void addClusterClassMap(final SchemaClass cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToClasses.put(clusterId, cls);
    }
  }

  protected void addClusterViewMap(final SchemaView cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToViews.put(clusterId, cls);
    }
  }

  private void ensurePropertiesSize(int size) {
    while (properties.size() <= size) {
      properties.add(null);
    }
  }

  public int addBlobCluster(DatabaseSessionInternal database, int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      checkClusterCanBeAdded(clusterId, null);
      blobClusters.add(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return clusterId;
  }

  public void removeBlobCluster(DatabaseSessionInternal database, String clusterName) {
    acquireSchemaWriteLock(database);
    try {
      int clusterId = getClusterId(database, clusterName);
      blobClusters.remove(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected int getClusterId(DatabaseSessionInternal database, final String stringValue) {
    int clId;
    try {
      clId = Integer.parseInt(stringValue);
    } catch (NumberFormatException ignore) {
      clId = database.getClusterIdByName(stringValue);
    }
    return clId;
  }

  public int createClusterIfNeeded(DatabaseSessionInternal database, String nameOrId) {
    final String[] parts = nameOrId.split(" ");
    int clId = getClusterId(database, parts[0]);

    if (clId == NOT_EXISTENT_CLUSTER_ID) {
      try {
        clId = Integer.parseInt(parts[0]);
        throw new IllegalArgumentException("Cluster id '" + clId + "' cannot be added");
      } catch (NumberFormatException ignore) {
        clId = database.addCluster(parts[0]);
      }
    }

    return clId;
  }

  public IntSet getBlobClusters() {
    return IntSets.unmodifiable(blobClusters);
  }

  public void sendCommand(DatabaseSessionInternal database, String command) {
    throw new UnsupportedOperationException();
  }
}
