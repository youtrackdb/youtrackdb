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
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSchemaNotCreatedException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

  protected final Map<String, OClass> classes = new HashMap<String, OClass>();
  protected final Int2ObjectOpenHashMap<OClass> clustersToClasses = new Int2ObjectOpenHashMap<>();

  protected final Map<String, OView> views = new HashMap<String, OView>();
  protected final Int2ObjectOpenHashMap<OView> clustersToViews = new Int2ObjectOpenHashMap<>();

  private final OClusterSelectionFactory clusterSelectionFactory = new OClusterSelectionFactory();

  private final OModifiableInteger modificationCounter = new OModifiableInteger();
  private final List<OGlobalProperty> properties = new ArrayList<>();
  private final Map<String, OGlobalProperty> propertiesByNameType = new HashMap<>();
  private IntOpenHashSet blobClusters = new IntOpenHashSet();
  private volatile int version = 0;
  private volatile ORID identity;
  protected volatile OImmutableSchema snapshot;

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

  public OImmutableSchema makeSnapshot(ODatabaseSessionInternal database) {
    if (snapshot == null) {
      // Is null only in the case that is asked while the schema is created
      // all the other cases are already protected by a write lock
      acquireSchemaReadLock();
      try {
        if (snapshot == null) {
          snapshot = new OImmutableSchema(this, database);
        }
      } finally {
        releaseSchemaReadLock();
      }
    }
    return snapshot;
  }

  public void forceSnapshot(ODatabaseSessionInternal database) {
    acquireSchemaReadLock();
    try {
      snapshot = new OImmutableSchema(this, database);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  public int countClasses(ODatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return classes.size();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int countViews(ODatabaseSessionInternal database) {
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
  public void onPostIndexManagement(ODatabaseSessionInternal session) {
    for (OClass c : classes.values()) {
      if (c instanceof OClassImpl) {
        ((OClassImpl) c).onPostIndexManagement(session);
      }
    }
    for (OClass c : views.values()) {
      if (c instanceof OClassImpl) {
        ((OClassImpl) c).onPostIndexManagement(session);
      }
    }
  }

  public OClass createClass(ODatabaseSessionInternal database, final String className) {
    return createClass(database, className, null, (int[]) null);
  }

  public OClass createClass(
      ODatabaseSessionInternal database, final String iClassName, final OClass iSuperClass) {
    return createClass(database, iClassName, iSuperClass, null);
  }

  public OClass createClass(
      ODatabaseSessionInternal database, String iClassName, OClass... superClasses) {
    return createClass(database, iClassName, null, superClasses);
  }

  public OClass getOrCreateClass(ODatabaseSessionInternal database, final String iClassName) {
    return getOrCreateClass(database, iClassName, (OClass) null);
  }

  public OClass getOrCreateClass(
      ODatabaseSessionInternal database, final String iClassName, final OClass superClass) {
    return getOrCreateClass(
        database, iClassName, superClass == null ? new OClass[0] : new OClass[]{superClass});
  }

  public abstract OClass getOrCreateClass(
      ODatabaseSessionInternal database, final String iClassName, final OClass... superClasses);

  public OClass createAbstractClass(ODatabaseSessionInternal database, final String className) {
    return createClass(database, className, null, new int[]{-1});
  }

  public OClass createAbstractClass(
      ODatabaseSessionInternal database, final String className, final OClass superClass) {
    return createClass(database, className, superClass, new int[]{-1});
  }

  public OClass createAbstractClass(
      ODatabaseSessionInternal database, String iClassName, OClass... superClasses) {
    return createClass(database, iClassName, new int[]{-1}, superClasses);
  }

  public OClass createClass(
      ODatabaseSessionInternal database,
      final String className,
      final OClass superClass,
      int[] clusterIds) {
    return createClass(database, className, clusterIds, superClass);
  }

  public abstract OClass createClass(
      ODatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      OClass... superClasses);

  public abstract OClass createClass(
      ODatabaseSessionInternal database,
      final String className,
      int clusters,
      OClass... superClasses);

  public abstract OView createView(
      ODatabaseSessionInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata);

  public abstract OView createView(ODatabaseSessionInternal database, OViewConfig cfg);

  public abstract OView createView(
      ODatabaseSessionInternal database, OViewConfig cfg, ViewCreationListener listener)
      throws UnsupportedOperationException;

  public abstract void checkEmbedded();

  void checkClusterCanBeAdded(int clusterId, OClass cls) {
    acquireSchemaReadLock();
    try {
      if (clusterId < 0) {
        return;
      }

      if (blobClusters.contains(clusterId)) {
        throw new OSchemaException("Cluster with id " + clusterId + " already belongs to Blob");
      }

      final OClass existingCls = clustersToClasses.get(clusterId);

      if (existingCls != null && (cls == null || !cls.equals(existingCls))) {
        throw new OSchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to the class '"
                + clustersToClasses.get(clusterId)
                + "'");
      }

      final OView existingView = clustersToViews.get(clusterId);

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

  public OClass getClassByClusterId(int clusterId) {
    acquireSchemaReadLock();
    try {
      return clustersToClasses.get(clusterId);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OView getViewByClusterId(int clusterId) {
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
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#dropClass(java.lang.String)
   */
  public abstract void dropClass(ODatabaseSessionInternal database, final String className);

  public abstract void dropView(ODatabaseSessionInternal database, final String viewName);

  /**
   * Reloads the schema inside a storage's shared lock.
   */
  public void reload(ODatabaseSessionInternal database) {
    lock.writeLock().lock();
    try {
      database.executeInTx(
          () -> {
            identity = new ORecordId(
                database.getStorageInfo().getConfiguration().getSchemaRecordId());

            ODocument document = database.load(identity);
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
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.Class)
   */
  public OClass getClass(final Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    return getClass(iClass.getSimpleName());
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.String)
   */
  public OClass getClass(final String iClassName) {
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

  public OView getView(final String viewName) {
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

  public void acquireSchemaWriteLock(ODatabaseSessionInternal database) {
    database.startExclusiveMetadataChange();
    lock.writeLock().lock();
    modificationCounter.increment();
  }

  public void releaseSchemaWriteLock(ODatabaseSessionInternal database) {
    releaseSchemaWriteLock(database, true);
  }

  public void releaseSchemaWriteLock(ODatabaseSessionInternal database, final boolean iSave) {
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
          snapshot = new OImmutableSchema(this, database);
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
      ODatabaseSessionInternal database,
      final String oldName,
      final String newName,
      final OClass cls) {

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
      ODatabaseSessionInternal database,
      final String oldName,
      final String newName,
      final OView view) {

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
   * Binds ODocument to POJO.
   */
  public void fromStream(ODatabaseSessionInternal session, ODocument document) {
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
                + " version of OxygenDB and reimport it using the current one.");
      }

      properties.clear();
      propertiesByNameType.clear();
      List<ODocument> globalProperties = document.field("globalProperties");
      boolean hasGlobalProperties = false;
      if (globalProperties != null) {
        hasGlobalProperties = true;
        for (ODocument oDocument : globalProperties) {
          OGlobalPropertyImpl prop = new OGlobalPropertyImpl();
          prop.fromDocument(oDocument);
          ensurePropertiesSize(prop.getId());
          properties.set(prop.getId(), prop);
          propertiesByNameType.put(prop.getName() + "|" + prop.getType().name(), prop);
        }
      }
      // REGISTER ALL THE CLASSES
      clustersToClasses.clear();

      final Map<String, OClass> newClasses = new HashMap<String, OClass>();
      final Map<String, OView> newViews = new HashMap<String, OView>();

      Collection<ODocument> storedClasses = document.field("classes");
      for (ODocument c : storedClasses) {
        String name = c.field("name");

        OClassImpl cls;
        if (classes.containsKey(name.toLowerCase(Locale.ENGLISH))) {
          cls = (OClassImpl) classes.get(name.toLowerCase(Locale.ENGLISH));
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
      List<OClass> superClasses;
      OClass superClass;

      for (ODocument c : storedClasses) {
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
          OClassImpl cls =
              (OClassImpl) classes.get(((String) c.field("name")).toLowerCase(Locale.ENGLISH));
          superClasses = new ArrayList<OClass>(superClassNames.size());
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
      Collection<ODocument> storedViews = document.field("views");
      if (storedViews != null) {
        for (ODocument v : storedViews) {

          String name = v.field("name");

          OViewImpl view;
          if (views.containsKey(name.toLowerCase(Locale.ENGLISH))) {
            view = (OViewImpl) views.get(name.toLowerCase(Locale.ENGLISH));
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
        ODatabaseSessionInternal database = ODatabaseRecordThreadLocal.instance().get();
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

  protected abstract OClassImpl createClassInstance(String name);

  protected abstract OViewImpl createViewInstance(String name);

  public ODocument toNetworkStream() {
    lock.readLock().lock();
    try {
      ODocument document = new ODocument();
      document.setTrackingChanges(false);
      document.field("schemaVersion", CURRENT_VERSION_NUMBER);

      Set<ODocument> cc = new HashSet<ODocument>();
      for (OClass c : classes.values()) {
        cc.add(((OClassImpl) c).toNetworkStream());
      }

      document.field("classes", cc, OType.EMBEDDEDSET);

      // TODO: this should trigger a netowork protocol version change
      Set<ODocument> vv = new HashSet<ODocument>();
      for (OView v : views.values()) {
        vv.add(((OViewImpl) v).toNetworkStream());
      }

      document.field("views", vv, OType.EMBEDDEDSET);

      List<ODocument> globalProperties = new ArrayList<ODocument>();
      for (OGlobalProperty globalProperty : properties) {
        if (globalProperty != null) {
          globalProperties.add(((OGlobalPropertyImpl) globalProperty).toDocument());
        }
      }
      document.field("globalProperties", globalProperties, OType.EMBEDDEDLIST);
      document.field("blobClusters", blobClusters, OType.EMBEDDEDSET);
      return document;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Binds POJO to ODocument.
   */
  public ODocument toStream() {
    lock.readLock().lock();
    try {
      ODocument document = ODatabaseSessionInternal.getActiveSession().load(identity);
      document.field("schemaVersion", CURRENT_VERSION_NUMBER);

      // This steps is needed because in classes there are duplicate due to aliases
      Set<OClassImpl> realClases = new HashSet<OClassImpl>();
      for (OClass c : classes.values()) {
        realClases.add(((OClassImpl) c));
      }

      Set<ODocument> classesDocuments = new HashSet<ODocument>();
      for (OClassImpl c : realClases) {
        classesDocuments.add(c.toStream());
      }
      document.field("classes", classesDocuments, OType.EMBEDDEDSET);

      // This steps is needed because in views there are duplicate due to aliases
      Set<OViewImpl> realViews = new HashSet<OViewImpl>();
      for (OView v : views.values()) {
        realViews.add(((OViewImpl) v));
      }

      Set<ODocument> viewsDocuments = new HashSet<ODocument>();
      for (OClassImpl c : realViews) {
        viewsDocuments.add(c.toStream());
      }
      document.field("views", viewsDocuments, OType.EMBEDDEDSET);

      List<ODocument> globalProperties = new ArrayList<ODocument>();
      for (OGlobalProperty globalProperty : properties) {
        if (globalProperty != null) {
          globalProperties.add(((OGlobalPropertyImpl) globalProperty).toDocument());
        }
      }
      document.field("globalProperties", globalProperties, OType.EMBEDDEDLIST);
      document.field("blobClusters", blobClusters, OType.EMBEDDEDSET);
      return document;
    } finally {
      lock.readLock().unlock();
    }
  }

  public Collection<OClass> getClasses(ODatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<OClass>(classes.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<OView> getViews(ODatabaseSessionInternal database) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<OView>(views.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OClass> getClassesRelyOnCluster(
      ODatabaseSessionInternal database, final String clusterName) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final int clusterId = database.getClusterIdByName(clusterName);
      final Set<OClass> result = new HashSet<OClass>();
      for (OClass c : classes.values()) {
        if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId)) {
          result.add(c);
        }
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OSchemaShared load(ODatabaseSessionInternal database) {

    lock.writeLock().lock();
    try {
      identity = new ORecordId(database.getStorageInfo().getConfiguration().getSchemaRecordId());
      if (!identity.isValid()) {
        throw new OSchemaNotCreatedException("Schema is not created and cannot be loaded");
      }
      database.executeInTx(
          () -> {
            ODocument document = database.load(identity);
            fromStream(database, document);
          });
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void create(final ODatabaseSessionInternal database) {
    lock.writeLock().lock();
    try {
      ODocument document =
          database.computeInTx(
              () -> database.save(new ODocument(), OMetadataDefault.CLUSTER_INTERNAL_NAME));
      this.identity = document.getIdentity();
      database.getStorage().setSchemaRecordId(document.getIdentity().toString());
      snapshot = new OImmutableSchema(this, database);
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

  public ORID getIdentity() {
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
      final String name, final OType type, final Integer id) {
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

  protected OGlobalProperty findOrCreateGlobalProperty(final String name, final OType type) {
    OGlobalProperty global = propertiesByNameType.get(name + "|" + type.name());
    if (global == null) {
      int id = properties.size();
      global = new OGlobalPropertyImpl(name, type, id);
      properties.add(id, global);
      propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    }
    return global;
  }

  protected boolean executeThroughDistributedStorage(ODatabaseSessionInternal database) {
    return !database.isLocalEnv();
  }

  private void saveInternal(ODatabaseSessionInternal database) {

    if (database.getTransaction().isActive()) {
      throw new OSchemaException(
          "Cannot change the schema while a transaction is active. Schema changes are not"
              + " transactional");
    }

    OScenarioThreadLocal.executeAsDistributed(
        new Callable<Object>() {
          @Override
          public Object call() {
            database.begin();
            ODocument document = toStream();
            database.save(document, OMetadataDefault.CLUSTER_INTERNAL_NAME);
            database.commit();
            return null;
          }
        });

    forceSnapshot(database);
    database.executeInTx(() -> {
      for (OMetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
        listener.onSchemaUpdate(database, database.getName(), this);
      }
    });

  }

  protected void addClusterClassMap(final OClass cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToClasses.put(clusterId, cls);
    }
  }

  protected void addClusterViewMap(final OView cls) {
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

  public int addBlobCluster(ODatabaseSessionInternal database, int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      checkClusterCanBeAdded(clusterId, null);
      blobClusters.add(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
    return clusterId;
  }

  public void removeBlobCluster(ODatabaseSessionInternal database, String clusterName) {
    acquireSchemaWriteLock(database);
    try {
      int clusterId = getClusterId(database, clusterName);
      blobClusters.remove(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected int getClusterId(ODatabaseSessionInternal database, final String stringValue) {
    int clId;
    try {
      clId = Integer.parseInt(stringValue);
    } catch (NumberFormatException ignore) {
      clId = database.getClusterIdByName(stringValue);
    }
    return clId;
  }

  public int createClusterIfNeeded(ODatabaseSessionInternal database, String nameOrId) {
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

  public void sendCommand(ODatabaseSessionInternal database, String command) {
    throw new UnsupportedOperationException();
  }
}
