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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSchemaNotCreatedException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
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
public abstract class OSchemaShared implements OCloseable {

  private static final int NOT_EXISTENT_CLUSTER_ID = -1;
  public static final int CURRENT_VERSION_NUMBER = 4;
  public static final int VERSION_NUMBER_V4 = 4;
  // this is needed for guarantee the compatibility to 2.0-M1 and 2.0-M2 no changed associated with
  // it
  public static final int VERSION_NUMBER_V5 = 5;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected final Map<String, YTClass> classes = new HashMap<String, YTClass>();
  protected final Int2ObjectOpenHashMap<YTClass> clustersToClasses = new Int2ObjectOpenHashMap<>();

  protected final Map<String, YTView> views = new HashMap<String, YTView>();
  protected final Int2ObjectOpenHashMap<YTView> clustersToViews = new Int2ObjectOpenHashMap<>();

  private final OClusterSelectionFactory clusterSelectionFactory = new OClusterSelectionFactory();

  private final OModifiableInteger modificationCounter = new OModifiableInteger();
  private final List<OGlobalProperty> properties = new ArrayList<>();
  private final Map<String, OGlobalProperty> propertiesByNameType = new HashMap<>();
  private IntOpenHashSet blobClusters = new IntOpenHashSet();
  private volatile int version = 0;
  private volatile YTRID identity;
  protected volatile YTImmutableSchema snapshot;

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

  public OSchemaShared() {
  }

  public static Character checkClassNameIfValid(String iName) throws OSchemaException {
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

  public YTImmutableSchema makeSnapshot(YTDatabaseSessionInternal database) {
    if (snapshot == null) {
      // Is null only in the case that is asked while the schema is created
      // all the other cases are already protected by a write lock
      acquireSchemaReadLock();
      try {
        if (snapshot == null) {
          snapshot = new YTImmutableSchema(this, database);
        }
      } finally {
        releaseSchemaReadLock();
      }
    }
    return snapshot;
  }

  public void forceSnapshot(YTDatabaseSessionInternal database) {
    acquireSchemaReadLock();
    try {
      snapshot = new YTImmutableSchema(this, database);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  public int countClasses(YTDatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return classes.size();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int countViews(YTDatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

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
  public void onPostIndexManagement(YTDatabaseSessionInternal session) {
    for (YTClass c : classes.values()) {
      if (c instanceof YTClassImpl) {
        ((YTClassImpl) c).onPostIndexManagement(session);
      }
    }
    for (YTClass c : views.values()) {
      if (c instanceof YTClassImpl) {
        ((YTClassImpl) c).onPostIndexManagement(session);
      }
    }
  }

  public YTClass createClass(YTDatabaseSessionInternal database, final String className) {
    return createClass(database, className, null, (int[]) null);
  }

  public YTClass createClass(
      YTDatabaseSessionInternal database, final String iClassName, final YTClass iSuperClass) {
    return createClass(database, iClassName, iSuperClass, null);
  }

  public YTClass createClass(
      YTDatabaseSessionInternal database, String iClassName, YTClass... superClasses) {
    return createClass(database, iClassName, null, superClasses);
  }

  public YTClass getOrCreateClass(YTDatabaseSessionInternal database, final String iClassName) {
    return getOrCreateClass(database, iClassName, (YTClass) null);
  }

  public YTClass getOrCreateClass(
      YTDatabaseSessionInternal database, final String iClassName, final YTClass superClass) {
    return getOrCreateClass(
        database, iClassName, superClass == null ? new YTClass[0] : new YTClass[]{superClass});
  }

  public abstract YTClass getOrCreateClass(
      YTDatabaseSessionInternal database, final String iClassName, final YTClass... superClasses);

  public YTClass createAbstractClass(YTDatabaseSessionInternal database, final String className) {
    return createClass(database, className, null, new int[]{-1});
  }

  public YTClass createAbstractClass(
      YTDatabaseSessionInternal database, final String className, final YTClass superClass) {
    return createClass(database, className, superClass, new int[]{-1});
  }

  public YTClass createAbstractClass(
      YTDatabaseSessionInternal database, String iClassName, YTClass... superClasses) {
    return createClass(database, iClassName, new int[]{-1}, superClasses);
  }

  public YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      final YTClass superClass,
      int[] clusterIds) {
    return createClass(database, className, clusterIds, superClass);
  }

  public abstract YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      YTClass... superClasses);

  public abstract YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      int clusters,
      YTClass... superClasses);

  public abstract YTView createView(
      YTDatabaseSessionInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata);

  public abstract YTView createView(YTDatabaseSessionInternal database, OViewConfig cfg);

  public abstract YTView createView(
      YTDatabaseSessionInternal database, OViewConfig cfg, ViewCreationListener listener)
      throws UnsupportedOperationException;

  public abstract void checkEmbedded();

  void checkClusterCanBeAdded(int clusterId, YTClass cls) {
    acquireSchemaReadLock();
    try {
      if (clusterId < 0) {
        return;
      }

      if (blobClusters.contains(clusterId)) {
        throw new OSchemaException("Cluster with id " + clusterId + " already belongs to Blob");
      }

      final YTClass existingCls = clustersToClasses.get(clusterId);

      if (existingCls != null && (cls == null || !cls.equals(existingCls))) {
        throw new OSchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to the class '"
                + clustersToClasses.get(clusterId)
                + "'");
      }

      final YTView existingView = clustersToViews.get(clusterId);

      if (existingView != null && (cls == null || !cls.equals(existingView))) {
        throw new OSchemaException(
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

  public YTClass getClassByClusterId(int clusterId) {
    acquireSchemaReadLock();
    try {
      return clustersToClasses.get(clusterId);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTView getViewByClusterId(int clusterId) {
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
   * @see com.orientechnologies.orient.core.metadata.schema.YTSchema#dropClass(java.lang.String)
   */
  public abstract void dropClass(YTDatabaseSessionInternal database, final String className);

  public abstract void dropView(YTDatabaseSessionInternal database, final String viewName);

  /**
   * Reloads the schema inside a storage's shared lock.
   */
  public void reload(YTDatabaseSessionInternal database) {
    lock.writeLock().lock();
    try {
      database.executeInTx(
          () -> {
            identity = new YTRecordId(
                database.getStorageInfo().getConfiguration().getSchemaRecordId());

            YTDocument document = database.load(identity);
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
   * @see com.orientechnologies.orient.core.metadata.schema.YTSchema#getClass(java.lang.Class)
   */
  public YTClass getClass(final Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    return getClass(iClass.getSimpleName());
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.metadata.schema.YTSchema#getClass(java.lang.String)
   */
  public YTClass getClass(final String iClassName) {
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

  public YTView getView(final String viewName) {
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

  public void acquireSchemaWriteLock(YTDatabaseSessionInternal database) {
    database.startExclusiveMetadataChange();
    lock.writeLock().lock();
    modificationCounter.increment();
  }

  public void releaseSchemaWriteLock(YTDatabaseSessionInternal database) {
    releaseSchemaWriteLock(database, true);
  }

  public void releaseSchemaWriteLock(YTDatabaseSessionInternal database, final boolean iSave) {
    int count;
    try {
      if (modificationCounter.intValue() == 1) {
        // if it is embedded storage modification of schema is done by internal methods otherwise it
        // is done by
        // by sql commands and we need to reload local replica

        if (iSave) {
          if (database.getStorage() instanceof OAbstractPaginatedStorage) {
            saveInternal(database);
          } else {
            reload(database);
          }
        } else {
          snapshot = new YTImmutableSchema(this, database);
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
      YTDatabaseSessionInternal database,
      final String oldName,
      final String newName,
      final YTClass cls) {

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
      YTDatabaseSessionInternal database,
      final String oldName,
      final String newName,
      final YTView view) {

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
   * Binds YTDocument to POJO.
   */
  public void fromStream(YTDatabaseSessionInternal session, YTDocument document) {
    lock.writeLock().lock();
    modificationCounter.increment();
    try {
      // READ CURRENT SCHEMA VERSION
      final Integer schemaVersion = document.field("schemaVersion");
      if (schemaVersion == null) {
        OLogManager.instance()
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
        throw new OConfigurationException(
            "Database schema is different. Please export your old database with the previous"
                + " version of YouTrackDB and reimport it using the current one.");
      }

      properties.clear();
      propertiesByNameType.clear();
      List<YTDocument> globalProperties = document.field("globalProperties");
      boolean hasGlobalProperties = false;
      if (globalProperties != null) {
        hasGlobalProperties = true;
        for (YTDocument oDocument : globalProperties) {
          OGlobalPropertyImpl prop = new OGlobalPropertyImpl();
          prop.fromDocument(oDocument);
          ensurePropertiesSize(prop.getId());
          properties.set(prop.getId(), prop);
          propertiesByNameType.put(prop.getName() + "|" + prop.getType().name(), prop);
        }
      }
      // REGISTER ALL THE CLASSES
      clustersToClasses.clear();

      final Map<String, YTClass> newClasses = new HashMap<String, YTClass>();
      final Map<String, YTView> newViews = new HashMap<String, YTView>();

      Collection<YTDocument> storedClasses = document.field("classes");
      for (YTDocument c : storedClasses) {
        String name = c.field("name");

        YTClassImpl cls;
        if (classes.containsKey(name.toLowerCase(Locale.ENGLISH))) {
          cls = (YTClassImpl) classes.get(name.toLowerCase(Locale.ENGLISH));
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
      List<YTClass> superClasses;
      YTClass superClass;

      for (YTDocument c : storedClasses) {
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
          YTClassImpl cls =
              (YTClassImpl) classes.get(((String) c.field("name")).toLowerCase(Locale.ENGLISH));
          superClasses = new ArrayList<YTClass>(superClassNames.size());
          for (String superClassName : superClassNames) {

            superClass = classes.get(superClassName.toLowerCase(Locale.ENGLISH));

            if (superClass == null) {
              throw new OConfigurationException(
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
      Collection<YTDocument> storedViews = document.field("views");
      if (storedViews != null) {
        for (YTDocument v : storedViews) {

          String name = v.field("name");

          YTViewImpl view;
          if (views.containsKey(name.toLowerCase(Locale.ENGLISH))) {
            view = (YTViewImpl) views.get(name.toLowerCase(Locale.ENGLISH));
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
        YTDatabaseSessionInternal database = ODatabaseRecordThreadLocal.instance().get();
        if (database.getStorage() instanceof OAbstractPaginatedStorage) {
          saveInternal(database);
        }
      }

    } finally {
      version++;
      modificationCounter.decrement();
      lock.writeLock().unlock();
    }
  }

  protected abstract YTClassImpl createClassInstance(String name);

  protected abstract YTViewImpl createViewInstance(String name);

  public YTDocument toNetworkStream() {
    lock.readLock().lock();
    try {
      YTDocument document = new YTDocument();
      document.setTrackingChanges(false);
      document.field("schemaVersion", CURRENT_VERSION_NUMBER);

      Set<YTDocument> cc = new HashSet<YTDocument>();
      for (YTClass c : classes.values()) {
        cc.add(((YTClassImpl) c).toNetworkStream());
      }

      document.field("classes", cc, YTType.EMBEDDEDSET);

      // TODO: this should trigger a netowork protocol version change
      Set<YTDocument> vv = new HashSet<YTDocument>();
      for (YTView v : views.values()) {
        vv.add(((YTViewImpl) v).toNetworkStream());
      }

      document.field("views", vv, YTType.EMBEDDEDSET);

      List<YTDocument> globalProperties = new ArrayList<YTDocument>();
      for (OGlobalProperty globalProperty : properties) {
        if (globalProperty != null) {
          globalProperties.add(((OGlobalPropertyImpl) globalProperty).toDocument());
        }
      }
      document.field("globalProperties", globalProperties, YTType.EMBEDDEDLIST);
      document.field("blobClusters", blobClusters, YTType.EMBEDDEDSET);
      return document;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Binds POJO to YTDocument.
   */
  public YTDocument toStream(@Nonnull YTDatabaseSessionInternal db) {
    lock.readLock().lock();
    try {
      YTDocument document = db.load(identity);
      document.field("schemaVersion", CURRENT_VERSION_NUMBER);

      // This steps is needed because in classes there are duplicate due to aliases
      Set<YTClassImpl> realClases = new HashSet<YTClassImpl>();
      for (YTClass c : classes.values()) {
        realClases.add(((YTClassImpl) c));
      }

      Set<YTDocument> classesDocuments = new HashSet<YTDocument>();
      for (YTClassImpl c : realClases) {
        classesDocuments.add(c.toStream());
      }
      document.field("classes", classesDocuments, YTType.EMBEDDEDSET);

      // This steps is needed because in views there are duplicate due to aliases
      Set<YTViewImpl> realViews = new HashSet<YTViewImpl>();
      for (YTView v : views.values()) {
        realViews.add(((YTViewImpl) v));
      }

      Set<YTDocument> viewsDocuments = new HashSet<YTDocument>();
      for (YTClassImpl c : realViews) {
        viewsDocuments.add(c.toStream());
      }
      document.field("views", viewsDocuments, YTType.EMBEDDEDSET);

      List<YTDocument> globalProperties = new ArrayList<YTDocument>();
      for (OGlobalProperty globalProperty : properties) {
        if (globalProperty != null) {
          globalProperties.add(((OGlobalPropertyImpl) globalProperty).toDocument());
        }
      }
      document.field("globalProperties", globalProperties, YTType.EMBEDDEDLIST);
      document.field("blobClusters", blobClusters, YTType.EMBEDDEDSET);
      return document;
    } finally {
      lock.readLock().unlock();
    }
  }

  public Collection<YTClass> getClasses(YTDatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<YTClass>(classes.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<YTView> getViews(YTDatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<YTView>(views.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<YTClass> getClassesRelyOnCluster(
      YTDatabaseSessionInternal database, final String clusterName) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final int clusterId = database.getClusterIdByName(clusterName);
      final Set<YTClass> result = new HashSet<YTClass>();
      for (YTClass c : classes.values()) {
        if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId)) {
          result.add(c);
        }
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OSchemaShared load(YTDatabaseSessionInternal database) {

    lock.writeLock().lock();
    try {
      identity = new YTRecordId(database.getStorageInfo().getConfiguration().getSchemaRecordId());
      if (!identity.isValid()) {
        throw new OSchemaNotCreatedException("Schema is not created and cannot be loaded");
      }
      database.executeInTx(
          () -> {
            YTDocument document = database.load(identity);
            fromStream(database, document);
          });
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void create(final YTDatabaseSessionInternal database) {
    lock.writeLock().lock();
    try {
      YTDocument document =
          database.computeInTx(
              () -> database.save(new YTDocument(), OMetadataDefault.CLUSTER_INTERNAL_NAME));
      this.identity = document.getIdentity();
      database.getStorage().setSchemaRecordId(document.getIdentity().toString());
      snapshot = new YTImmutableSchema(this, database);
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

  public YTRID getIdentity() {
    acquireSchemaReadLock();
    try {
      return identity;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OGlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size()) {
      return null;
    }
    return properties.get(id);
  }

  public OGlobalProperty createGlobalProperty(
      final String name, final YTType type, final Integer id) {
    OGlobalProperty global;
    if (id < properties.size() && (global = properties.get(id)) != null) {
      if (!global.getName().equals(name) || !global.getType().equals(type)) {
        throw new OSchemaException("A property with id " + id + " already exist ");
      }
      return global;
    }

    global = new OGlobalPropertyImpl(name, type, id);
    ensurePropertiesSize(id);
    properties.set(id, global);
    propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    return global;
  }

  public List<OGlobalProperty> getGlobalProperties() {
    return Collections.unmodifiableList(properties);
  }

  protected OGlobalProperty findOrCreateGlobalProperty(final String name, final YTType type) {
    OGlobalProperty global = propertiesByNameType.get(name + "|" + type.name());
    if (global == null) {
      int id = properties.size();
      global = new OGlobalPropertyImpl(name, type, id);
      properties.add(id, global);
      propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    }
    return global;
  }

  protected boolean executeThroughDistributedStorage(YTDatabaseSessionInternal database) {
    return !database.isLocalEnv();
  }

  private void saveInternal(YTDatabaseSessionInternal database) {

    var tx = database.getTransaction();
    if (tx.isActive()) {
      throw new OSchemaException(
          "Cannot change the schema while a transaction is active. Schema changes are not"
              + " transactional");
    }

    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          database.executeInTx(() -> {
            YTDocument document = toStream(database);
            database.save(document, OMetadataDefault.CLUSTER_INTERNAL_NAME);
          });
          return null;
        });

    forceSnapshot(database);

    for (OMetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
      listener.onSchemaUpdate(database, database.getName(), this);
    }
  }

  protected void addClusterClassMap(final YTClass cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToClasses.put(clusterId, cls);
    }
  }

  protected void addClusterViewMap(final YTView cls) {
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

  public int addBlobCluster(YTDatabaseSessionInternal database, int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      checkClusterCanBeAdded(clusterId, null);
      blobClusters.add(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return clusterId;
  }

  public void removeBlobCluster(YTDatabaseSessionInternal database, String clusterName) {
    acquireSchemaWriteLock(database);
    try {
      int clusterId = getClusterId(database, clusterName);
      blobClusters.remove(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected int getClusterId(YTDatabaseSessionInternal database, final String stringValue) {
    int clId;
    try {
      clId = Integer.parseInt(stringValue);
    } catch (NumberFormatException ignore) {
      clId = database.getClusterIdByName(stringValue);
    }
    return clId;
  }

  public int createClusterIfNeeded(YTDatabaseSessionInternal database, String nameOrId) {
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

  public void sendCommand(YTDatabaseSessionInternal database, String command) {
    throw new UnsupportedOperationException();
  }
}
