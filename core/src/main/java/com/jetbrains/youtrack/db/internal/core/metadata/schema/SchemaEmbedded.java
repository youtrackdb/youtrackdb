package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseLifecycleListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewCreationListener;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewManager;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition.INDEX_BY;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SchemaEmbedded extends SchemaShared {

  public SchemaEmbedded() {
    super();
  }

  public SchemaClass createClass(
      DatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      SchemaClass... superClasses) {
    final Character wrongCharacter = SchemaShared.checkClassNameIfValid(className);
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new SchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + className
              + "'");
    }

    SchemaClass result;
    int retry = 0;

    while (true) {
      try {
        result = doCreateClass(database, className, clusterIds, retry, superClasses);
        break;
      } catch (ClusterIdsAreEmptyException ignore) {
        classes.remove(className.toLowerCase(Locale.ENGLISH));
        clusterIds = createClusters(database, className);
        retry++;
      }
    }
    return result;
  }

  public SchemaClass createClass(
      DatabaseSessionInternal database,
      final String className,
      int clusters,
      SchemaClass... superClasses) {
    final Character wrongCharacter = SchemaShared.checkClassNameIfValid(className);
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new SchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + className
              + "'");
    }

    return doCreateClass(database, className, clusters, superClasses);
  }

  private SchemaClass doCreateClass(
      DatabaseSessionInternal database,
      final String className,
      final int clusters,
      SchemaClass... superClasses) {
    SchemaClass result;

    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_CREATE);
    if (superClasses != null) {
      SchemaClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    }
    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key)) {
        throw new SchemaException("Class '" + className + "' already exists in current database");
      }
      List<SchemaClass> superClassesList = new ArrayList<>();
      if (superClasses != null) {
        for (SchemaClass superClass : superClasses) {
          // Filtering for null
          if (superClass != null) {
            superClassesList.add(superClass);
          }
        }
      }

      final int[] clusterIds;
      if (clusters > 0) {
        clusterIds = createClusters(database, className, clusters);
      } else {
        // ABSTRACT
        clusterIds = new int[]{-1};
      }

      doRealCreateClass(database, className, superClassesList, clusterIds);

      result = classes.get(className.toLowerCase(Locale.ENGLISH));
      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<DatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        //noinspection deprecation
        it.next().onCreateClass(database, result);
      }

      for (DatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onCreateClass(database, result);
      }

    } catch (ClusterIdsAreEmptyException e) {
      throw BaseException.wrapException(
          new SchemaException("Cannot create class '" + className + "'"), e);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  protected void doRealCreateClass(
      DatabaseSessionInternal database,
      String className,
      List<SchemaClass> superClassesList,
      int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    createClassInternal(database, className, clusterIds, superClassesList);
  }

  protected void createClassInternal(
      DatabaseSessionInternal database,
      final String className,
      final int[] clusterIdsToAdd,
      final List<SchemaClass> superClasses)
      throws ClusterIdsAreEmptyException {
    acquireSchemaWriteLock(database);
    try {
      if (className == null || className.isEmpty()) {
        throw new SchemaException("Found class name null or empty");
      }

      checkEmbedded();

      checkClustersAreAbsent(clusterIdsToAdd);

      final int[] clusterIds;
      if (clusterIdsToAdd == null || clusterIdsToAdd.length == 0) {
        throw new ClusterIdsAreEmptyException();

      } else {
        clusterIds = clusterIdsToAdd;
      }

      database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_CREATE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      if (classes.containsKey(key)) {
        throw new SchemaException("Class '" + className + "' already exists in current database");
      }

      SchemaClassImpl cls = createClassInstance(className, clusterIds);

      classes.put(key, cls);

      if (superClasses != null && !superClasses.isEmpty()) {
        cls.setSuperClassesInternal(database, superClasses);
        for (SchemaClass superClass : superClasses) {
          // UPDATE INDEXES
          final int[] clustersToIndex = superClass.getPolymorphicClusterIds();
          final String[] clusterNames = new String[clustersToIndex.length];
          for (int i = 0; i < clustersToIndex.length; i++) {
            clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);
          }

          for (Index index : superClass.getIndexes(database)) {
            for (String clusterName : clusterNames) {
              if (clusterName != null) {
                database
                    .getMetadata()
                    .getIndexManagerInternal()
                    .addClusterToIndex(database, clusterName, index.getName());
              }
            }
          }
        }
      }

      addClusterClassMap(cls);

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public SchemaView createView(
      DatabaseSessionInternal database,
      String viewName,
      String statement,
      Map<String, Object> metadata) {
    ViewConfig cfg = new ViewConfig(viewName, statement);
    if (metadata != null) {
      cfg.setUpdatable(Boolean.TRUE.equals(metadata.get("updatable")));

      Object updateInterval = metadata.get("updateIntervalSeconds");
      if (updateInterval instanceof Integer) {
        cfg.setUpdateIntervalSeconds((Integer) updateInterval);
      }

      Object updateStrategy = metadata.get("updateStrategy");
      if (updateStrategy instanceof String) {
        cfg.setUpdateStrategy((String) updateStrategy);
      }

      Object watchClasses = metadata.get("watchClasses");
      if (watchClasses instanceof List) {
        //noinspection unchecked
        cfg.setWatchClasses((List<String>) watchClasses);
      }

      Object nodes = metadata.get("nodes");
      if (nodes instanceof List) {
        //noinspection unchecked
        cfg.setNodes((List<String>) nodes);
      }

      Object originRidField = metadata.get("originRidField");
      if (originRidField instanceof String) {
        cfg.setOriginRidField((String) originRidField);
      }

      Object indexes = metadata.get("indexes");
      if (indexes instanceof Collection) {
        //noinspection unchecked
        for (Object index : (Collection<Object>) indexes) {
          if (index instanceof Map) {
            ViewIndexConfig idxConfig =
                cfg.addIndex(
                    (String) ((Map<?, ?>) index).get("type"),
                    (String) ((Map<?, ?>) index).get("engine"));
            //noinspection unchecked
            for (Map.Entry<String, Object> entry :
                ((Map<String, Object>) ((Map<?, ?>) index).get("properties")).entrySet()) {
              PropertyType val = null;
              PropertyType linkedType = null;
              String collate = null;
              INDEX_BY indexBy = null;
              if (entry.getValue() == null || entry.getKey() == null) {
                throw new DatabaseException(
                    "Invalid properties " + ((Map<?, ?>) index).get("properties"));
              }
              if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> listVal = (Map<String, Object>) entry.getValue();
                if (listVal.get("type") != null) {
                  val = PropertyType.valueOf(
                      listVal.get("type").toString().toUpperCase(Locale.ENGLISH));
                }
                if (listVal.get("type") != null) {
                  linkedType =
                      PropertyType.valueOf(
                          listVal.get("linkedType").toString().toUpperCase(Locale.ENGLISH));
                }
                if (listVal.get("collate") != null) {
                  collate = listVal.get("collate").toString();
                }
                if (listVal.get("indexBy") != null) {
                  indexBy =
                      INDEX_BY.valueOf(
                          listVal.get("indexBy").toString().toUpperCase(Locale.ENGLISH));
                }

              } else {
                val = PropertyType.valueOf(entry.getValue().toString().toUpperCase(Locale.ENGLISH));
              }
              if (val == null) {
                throw new IllegalArgumentException(
                    "Invalid value for index key type: " + entry.getValue());
              }
              idxConfig.addProperty(
                  entry.getKey(), val, linkedType, SQLEngine.getCollate(collate), indexBy);
            }
          }
        }
      }
    }
    return createView(database, cfg);
  }

  @Override
  public SchemaView createView(DatabaseSessionInternal database, ViewConfig cfg) {
    return createView(database, cfg, null);
  }

  public SchemaView createView(
      DatabaseSessionInternal database, ViewConfig cfg, ViewCreationListener listener) {
    final Character wrongCharacter = SchemaShared.checkClassNameIfValid(cfg.getName());
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new SchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in view name '"
              + cfg.getName()
              + "'");
    }

    return doCreateView(database, cfg, listener);
  }

  private SchemaView doCreateView(
      DatabaseSessionInternal database, final ViewConfig config, ViewCreationListener listener) {
    SchemaView result;

    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_CREATE);
    acquireSchemaWriteLock(database);
    try {

      final String key = config.getName().toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key) || views.containsKey(key)) {
        throw new SchemaException(
            "View (or class) '" + config.getName() + "' already exists in current database");
      }

      final int[] clusterIds = createClusters(database, config.getName(), 1);

      doRealCreateView(database, config, clusterIds);

      result = views.get(config.getName().toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<DatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onCreateView(database, result);
      }

      for (DatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onCreateView(database, result);
      }

      ViewManager viewMgr = database.getSharedContext().getViewManager();
      viewMgr.updateViewAsync(
          result.getName(),
          new ViewCreationListener() {
            @Override
            public void afterCreate(DatabaseSession database, String viewName) {
              try {
                viewMgr.registerLiveUpdateFor(database, viewName);
              } catch (Exception e) {
                if (listener != null) {
                  listener.onError(viewName, e);
                }
                return;
              }
              if (listener != null) {
                listener.afterCreate(database, viewName);
              }
            }

            @Override
            public void onError(String viewName, Exception exception) {
              if (listener != null) {
                listener.onError(viewName, exception);
              }
            }
          });

    } catch (ClusterIdsAreEmptyException e) {
      throw BaseException.wrapException(
          new SchemaException("Cannot create view '" + config.getName() + "'"), e);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  protected void doRealCreateView(
      DatabaseSessionInternal database, ViewConfig config, int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    createViewInternal(database, config, clusterIds);
  }

  protected void createViewInternal(
      DatabaseSessionInternal database, final ViewConfig cfg, final int[] clusterIdsToAdd)
      throws ClusterIdsAreEmptyException {
    acquireSchemaWriteLock(database);
    try {
      if (cfg.getName() == null || cfg.getName().isEmpty()) {
        throw new SchemaException("Found view name null or empty");
      }

      checkEmbedded();

      checkClustersAreAbsent(clusterIdsToAdd);

      final int[] clusterIds;
      if (clusterIdsToAdd == null || clusterIdsToAdd.length == 0) {
        throw new ClusterIdsAreEmptyException();

      } else {
        clusterIds = clusterIdsToAdd;
      }

      database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_CREATE);

      final String key = cfg.getName().toLowerCase(Locale.ENGLISH);

      if (views.containsKey(key)) {
        throw new SchemaException(
            "View '" + cfg.getName() + "' already exists in current database");
      }

      // TODO updatable and the
      SchemaViewImpl cls = createViewInstance(cfg, clusterIds);

      views.put(key, cls);

      addClusterViewMap(cls);

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected SchemaClassImpl createClassInstance(String className, int[] clusterIds) {
    return new SchemaClassEmbedded(this, className, clusterIds);
  }

  protected SchemaViewImpl createViewInstance(ViewConfig cfg, int[] clusterIds) {
    if (cfg.getQuery() == null) {
      throw new IllegalArgumentException("Invalid view configuration: no query defined");
    }
    return new SchemaViewEmbedded(this, cfg.getName(), cfg, clusterIds);
  }

  public SchemaClass getOrCreateClass(
      DatabaseSessionInternal database, final String iClassName,
      final SchemaClass... superClasses) {
    if (iClassName == null) {
      return null;
    }

    acquireSchemaReadLock();
    try {
      SchemaClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null) {
        return cls;
      }
    } finally {
      releaseSchemaReadLock();
    }

    SchemaClass cls;

    int[] clusterIds = null;
    int retry = 0;

    while (true) {
      try {
        acquireSchemaWriteLock(database);
        try {
          cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
          if (cls != null) {
            return cls;
          }

          cls = doCreateClass(database, iClassName, clusterIds, retry, superClasses);
          addClusterClassMap(cls);
        } finally {
          releaseSchemaWriteLock(database);
        }
        break;
      } catch (ClusterIdsAreEmptyException ignore) {
        clusterIds = createClusters(database, iClassName);
        retry++;
      }
    }

    return cls;
  }

  protected SchemaClass doCreateClass(
      DatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      int retry,
      SchemaClass... superClasses)
      throws ClusterIdsAreEmptyException {
    SchemaClass result;
    database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_CREATE);
    if (superClasses != null) {
      SchemaClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    }

    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key) && retry == 0) {
        throw new SchemaException("Class '" + className + "' already exists in current database");
      }

      if (!executeThroughDistributedStorage(database)) {
        checkClustersAreAbsent(clusterIds);
      }

      if (clusterIds == null || clusterIds.length == 0) {
        clusterIds =
            createClusters(
                database,
                className,
                database.getStorageInfo().getConfiguration().getMinimumClusters());
      }
      List<SchemaClass> superClassesList = new ArrayList<>();
      if (superClasses != null) {
        for (SchemaClass superClass : superClasses) {
          if (superClass != null) {
            superClassesList.add(superClass);
          }
        }
      }

      doRealCreateClass(database, className, superClassesList, clusterIds);

      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<DatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        //noinspection deprecation
        it.next().onCreateClass(database, result);
      }

      for (DatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onCreateClass(database, result);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  private int[] createClusters(DatabaseSessionInternal database, final String iClassName) {
    return createClusters(
        database, iClassName, database.getStorageInfo().getConfiguration().getMinimumClusters());
  }

  protected int[] createClusters(
      DatabaseSessionInternal database, String className, int minimumClusters) {
    className = className.toLowerCase(Locale.ENGLISH);

    int[] clusterIds;

    if (internalClasses.contains(className.toLowerCase(Locale.ENGLISH))) {
      // INTERNAL CLASS, SET TO 1
      minimumClusters = 1;
    }

    clusterIds = new int[minimumClusters];
    clusterIds[0] = database.getClusterIdByName(className);
    if (clusterIds[0] > -1) {
      // CHECK THE CLUSTER HAS NOT BEEN ALREADY ASSIGNED
      final SchemaClass cls = clustersToClasses.get(clusterIds[0]);
      if (cls != null) {
        clusterIds[0] = database.addCluster(getNextAvailableClusterName(database, className));
      }
    } else
    // JUST KEEP THE CLASS NAME. THIS IS FOR LEGACY REASONS
    {
      clusterIds[0] = database.addCluster(className);
    }

    for (int i = 1; i < minimumClusters; ++i) {
      clusterIds[i] = database.addCluster(getNextAvailableClusterName(database, className));
    }

    return clusterIds;
  }

  private static String getNextAvailableClusterName(
      DatabaseSessionInternal database, final String className) {
    for (int i = 1; ; ++i) {
      final String clusterName = className + "_" + i;
      if (database.getClusterIdByName(clusterName) < 0)
      // FREE NAME
      {
        return clusterName;
      }
    }
  }

  protected void checkClustersAreAbsent(final int[] iClusterIds) {
    if (iClusterIds == null) {
      return;
    }

    for (int clusterId : iClusterIds) {
      if (clusterId < 0) {
        continue;
      }

      if (clustersToClasses.containsKey(clusterId)) {
        throw new SchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to class "
                + clustersToClasses.get(clusterId));
      }
    }
  }

  public void dropClass(DatabaseSessionInternal database, final String className) {
    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (className == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      SchemaClass cls = classes.get(key);

      if (cls == null) {
        throw new SchemaException("Class '" + className + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new SchemaException(
            "Class '"
                + className
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      doDropClass(database, className);

      // FREE THE RECORD CACHE
      database.getLocalCache().freeCluster(cls.getDefaultClusterId());

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void doDropClass(DatabaseSessionInternal database, String className) {
    dropClassInternal(database, className);
  }

  protected void dropClassInternal(DatabaseSessionInternal database, final String className) {
    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (className == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      final SchemaClass cls = classes.get(key);
      if (cls == null) {
        throw new SchemaException("Class '" + className + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new SchemaException(
            "Class '"
                + className
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      checkEmbedded();

      for (SchemaClass superClass : cls.getSuperClasses()) {
        // REMOVE DEPENDENCY FROM SUPERCLASS
        ((SchemaClassImpl) superClass).removeBaseClassInternal(database, cls);
      }
      for (int id : cls.getClusterIds()) {
        if (id != -1) {
          deleteCluster(database, id);
        }
      }

      dropClassIndexes(database, cls);

      classes.remove(key);

      if (cls.getShortName() != null)
      // REMOVE THE ALIAS TOO
      {
        classes.remove(cls.getShortName().toLowerCase(Locale.ENGLISH));
      }

      removeClusterClassMap(cls);

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<DatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        //noinspection deprecation
        it.next().onDropClass(database, cls);
      }

      for (DatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onDropClass(database, cls);
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void dropView(DatabaseSessionInternal database, final String name) {
    try {
      acquireSchemaWriteLock(database);
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (name == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

      final String key = name.toLowerCase(Locale.ENGLISH);

      SchemaView cls = views.get(key);

      if (cls == null) {
        throw new SchemaException("View '" + name + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new SchemaException(
            "View '"
                + name
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      doDropView(database, name);

      // FREE THE RECORD CACHE
      database.getLocalCache().freeCluster(cls.getDefaultClusterId());

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected void doDropView(DatabaseSessionInternal database, String name) {
    dropViewInternal(database, name);
  }

  protected void dropViewInternal(DatabaseSessionInternal database, final String view) {
    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (view == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_DELETE);

      final String key = view.toLowerCase(Locale.ENGLISH);

      final SchemaView cls = views.get(key);
      if (cls == null) {
        throw new SchemaException("View '" + view + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new SchemaException(
            "View '"
                + view
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      checkEmbedded();

      for (int id : cls.getClusterIds()) {
        if (id != -1) {
          deleteCluster(database, id);
        }
      }

      dropClassIndexes(database, cls);

      views.remove(key);

      if (cls.getShortName() != null)
      // REMOVE THE ALIAS TOO
      {
        views.remove(cls.getShortName().toLowerCase(Locale.ENGLISH));
      }

      removeClusterViewMap(cls);

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<DatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDropView(database, cls);
      }

      for (DatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onDropView(database, cls);
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected SchemaClassImpl createClassInstance(String name) {
    return new SchemaClassEmbedded(this, name);
  }

  protected SchemaViewImpl createViewInstance(String name) {
    return new SchemaViewEmbedded(this, name);
  }

  private static void dropClassIndexes(DatabaseSessionInternal database, final SchemaClass cls) {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    for (final Index index : indexManager.getClassIndexes(database, cls.getName())) {
      indexManager.dropIndex(database, index.getName());
    }
  }

  private static void deleteCluster(final DatabaseSessionInternal db, final int clusterId) {
    final String clusterName = db.getClusterNameById(clusterId);
    if (clusterName != null) {
      final RecordIteratorCluster<Record> iteratorCluster = db.browseCluster(clusterName);
      if (iteratorCluster != null) {
        db.executeInTxBatches(
            (Iterable<Record>) iteratorCluster, (session, record) -> record.delete());
        db.dropClusterInternal(clusterId);
      }
    }

    db.getLocalCache().freeCluster(clusterId);
  }

  private void removeClusterClassMap(final SchemaClass cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToClasses.remove(clusterId);
    }
  }

  private void removeClusterViewMap(final SchemaView cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToViews.remove(clusterId);
    }
  }

  public void checkEmbedded() {
  }

  void addClusterForClass(
      DatabaseSessionInternal database, final int clusterId, final SchemaClass cls) {
    acquireSchemaWriteLock(database);
    try {
      if (clusterId < 0) {
        return;
      }

      checkEmbedded();

      final SchemaClass existingCls = clustersToClasses.get(clusterId);
      if (existingCls != null && !cls.equals(existingCls)) {
        throw new SchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to class "
                + clustersToClasses.get(clusterId));
      }

      clustersToClasses.put(clusterId, cls);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  void addClusterForView(DatabaseSessionInternal database, final int clusterId,
      final SchemaView view) {
    acquireSchemaWriteLock(database);
    try {
      if (clusterId < 0) {
        return;
      }

      checkEmbedded();

      final SchemaView existingView = clustersToViews.get(clusterId);
      if (existingView != null && !view.equals(existingView)) {
        throw new SchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to view "
                + clustersToViews.get(clusterId));
      }

      clustersToViews.put(clusterId, view);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  void removeClusterForClass(DatabaseSessionInternal database, int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      if (clusterId < 0) {
        return;
      }

      checkEmbedded();

      clustersToClasses.remove(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  void removeClusterForView(DatabaseSessionInternal database, int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      if (clusterId < 0) {
        return;
      }

      checkEmbedded();

      clustersToViews.remove(clusterId);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }
}
