package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseLifecycleListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewCreationListener;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewManager;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSchemaException;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.OPropertyMapIndexDefinition.INDEX_BY;
import com.jetbrains.youtrack.db.internal.core.iterator.ORecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class OSchemaEmbedded extends OSchemaShared {

  public OSchemaEmbedded() {
    super();
  }

  public YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      YTClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + className
              + "'");
    }

    YTClass result;
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

  public YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      int clusters,
      YTClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + className
              + "'");
    }

    return doCreateClass(database, className, clusters, superClasses);
  }

  private YTClass doCreateClass(
      YTDatabaseSessionInternal database,
      final String className,
      final int clusters,
      YTClass... superClasses) {
    YTClass result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null) {
      YTClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    }
    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key)) {
        throw new YTSchemaException("Class '" + className + "' already exists in current database");
      }
      List<YTClass> superClassesList = new ArrayList<>();
      if (superClasses != null) {
        for (YTClass superClass : superClasses) {
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
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        //noinspection deprecation
        it.next().onCreateClass(database, result);
      }

      for (YTDatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onCreateClass(database, result);
      }

    } catch (ClusterIdsAreEmptyException e) {
      throw YTException.wrapException(
          new YTSchemaException("Cannot create class '" + className + "'"), e);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  protected void doRealCreateClass(
      YTDatabaseSessionInternal database,
      String className,
      List<YTClass> superClassesList,
      int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    createClassInternal(database, className, clusterIds, superClassesList);
  }

  protected void createClassInternal(
      YTDatabaseSessionInternal database,
      final String className,
      final int[] clusterIdsToAdd,
      final List<YTClass> superClasses)
      throws ClusterIdsAreEmptyException {
    acquireSchemaWriteLock(database);
    try {
      if (className == null || className.isEmpty()) {
        throw new YTSchemaException("Found class name null or empty");
      }

      checkEmbedded();

      checkClustersAreAbsent(clusterIdsToAdd);

      final int[] clusterIds;
      if (clusterIdsToAdd == null || clusterIdsToAdd.length == 0) {
        throw new ClusterIdsAreEmptyException();

      } else {
        clusterIds = clusterIdsToAdd;
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      if (classes.containsKey(key)) {
        throw new YTSchemaException("Class '" + className + "' already exists in current database");
      }

      YTClassImpl cls = createClassInstance(className, clusterIds);

      classes.put(key, cls);

      if (superClasses != null && !superClasses.isEmpty()) {
        cls.setSuperClassesInternal(database, superClasses);
        for (YTClass superClass : superClasses) {
          // UPDATE INDEXES
          final int[] clustersToIndex = superClass.getPolymorphicClusterIds();
          final String[] clusterNames = new String[clustersToIndex.length];
          for (int i = 0; i < clustersToIndex.length; i++) {
            clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);
          }

          for (OIndex index : superClass.getIndexes(database)) {
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
  public YTView createView(
      YTDatabaseSessionInternal database,
      String viewName,
      String statement,
      Map<String, Object> metadata) {
    OViewConfig cfg = new OViewConfig(viewName, statement);
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
            OViewIndexConfig idxConfig =
                cfg.addIndex(
                    (String) ((Map<?, ?>) index).get("type"),
                    (String) ((Map<?, ?>) index).get("engine"));
            //noinspection unchecked
            for (Map.Entry<String, Object> entry :
                ((Map<String, Object>) ((Map<?, ?>) index).get("properties")).entrySet()) {
              YTType val = null;
              YTType linkedType = null;
              String collate = null;
              INDEX_BY indexBy = null;
              if (entry.getValue() == null || entry.getKey() == null) {
                throw new YTDatabaseException(
                    "Invalid properties " + ((Map<?, ?>) index).get("properties"));
              }
              if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> listVal = (Map<String, Object>) entry.getValue();
                if (listVal.get("type") != null) {
                  val = YTType.valueOf(listVal.get("type").toString().toUpperCase(Locale.ENGLISH));
                }
                if (listVal.get("type") != null) {
                  linkedType =
                      YTType.valueOf(
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
                val = YTType.valueOf(entry.getValue().toString().toUpperCase(Locale.ENGLISH));
              }
              if (val == null) {
                throw new IllegalArgumentException(
                    "Invalid value for index key type: " + entry.getValue());
              }
              idxConfig.addProperty(
                  entry.getKey(), val, linkedType, OSQLEngine.getCollate(collate), indexBy);
            }
          }
        }
      }
    }
    return createView(database, cfg);
  }

  @Override
  public YTView createView(YTDatabaseSessionInternal database, OViewConfig cfg) {
    return createView(database, cfg, null);
  }

  public YTView createView(
      YTDatabaseSessionInternal database, OViewConfig cfg, ViewCreationListener listener) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(cfg.getName());
    //noinspection ConstantValue
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in view name '"
              + cfg.getName()
              + "'");
    }

    return doCreateView(database, cfg, listener);
  }

  private YTView doCreateView(
      YTDatabaseSessionInternal database, final OViewConfig config, ViewCreationListener listener) {
    YTView result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    acquireSchemaWriteLock(database);
    try {

      final String key = config.getName().toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key) || views.containsKey(key)) {
        throw new YTSchemaException(
            "View (or class) '" + config.getName() + "' already exists in current database");
      }

      final int[] clusterIds = createClusters(database, config.getName(), 1);

      doRealCreateView(database, config, clusterIds);

      result = views.get(config.getName().toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onCreateView(database, result);
      }

      for (YTDatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onCreateView(database, result);
      }

      ViewManager viewMgr = database.getSharedContext().getViewManager();
      viewMgr.updateViewAsync(
          result.getName(),
          new ViewCreationListener() {
            @Override
            public void afterCreate(YTDatabaseSession database, String viewName) {
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
      throw YTException.wrapException(
          new YTSchemaException("Cannot create view '" + config.getName() + "'"), e);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  protected void doRealCreateView(
      YTDatabaseSessionInternal database, OViewConfig config, int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    createViewInternal(database, config, clusterIds);
  }

  protected void createViewInternal(
      YTDatabaseSessionInternal database, final OViewConfig cfg, final int[] clusterIdsToAdd)
      throws ClusterIdsAreEmptyException {
    acquireSchemaWriteLock(database);
    try {
      if (cfg.getName() == null || cfg.getName().isEmpty()) {
        throw new YTSchemaException("Found view name null or empty");
      }

      checkEmbedded();

      checkClustersAreAbsent(clusterIdsToAdd);

      final int[] clusterIds;
      if (clusterIdsToAdd == null || clusterIdsToAdd.length == 0) {
        throw new ClusterIdsAreEmptyException();

      } else {
        clusterIds = clusterIdsToAdd;
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);

      final String key = cfg.getName().toLowerCase(Locale.ENGLISH);

      if (views.containsKey(key)) {
        throw new YTSchemaException(
            "View '" + cfg.getName() + "' already exists in current database");
      }

      // TODO updatable and the
      YTViewImpl cls = createViewInstance(cfg, clusterIds);

      views.put(key, cls);

      addClusterViewMap(cls);

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected YTClassImpl createClassInstance(String className, int[] clusterIds) {
    return new YTClassEmbedded(this, className, clusterIds);
  }

  protected YTViewImpl createViewInstance(OViewConfig cfg, int[] clusterIds) {
    if (cfg.getQuery() == null) {
      throw new IllegalArgumentException("Invalid view configuration: no query defined");
    }
    return new YTViewEmbedded(this, cfg.getName(), cfg, clusterIds);
  }

  public YTClass getOrCreateClass(
      YTDatabaseSessionInternal database, final String iClassName, final YTClass... superClasses) {
    if (iClassName == null) {
      return null;
    }

    acquireSchemaReadLock();
    try {
      YTClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null) {
        return cls;
      }
    } finally {
      releaseSchemaReadLock();
    }

    YTClass cls;

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

  protected YTClass doCreateClass(
      YTDatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      int retry,
      YTClass... superClasses)
      throws ClusterIdsAreEmptyException {
    YTClass result;
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null) {
      YTClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    }

    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key) && retry == 0) {
        throw new YTSchemaException("Class '" + className + "' already exists in current database");
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
      List<YTClass> superClassesList = new ArrayList<>();
      if (superClasses != null) {
        for (YTClass superClass : superClasses) {
          if (superClass != null) {
            superClassesList.add(superClass);
          }
        }
      }

      doRealCreateClass(database, className, superClassesList, clusterIds);

      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        //noinspection deprecation
        it.next().onCreateClass(database, result);
      }

      for (YTDatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onCreateClass(database, result);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  private int[] createClusters(YTDatabaseSessionInternal database, final String iClassName) {
    return createClusters(
        database, iClassName, database.getStorageInfo().getConfiguration().getMinimumClusters());
  }

  protected int[] createClusters(
      YTDatabaseSessionInternal database, String className, int minimumClusters) {
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
      final YTClass cls = clustersToClasses.get(clusterIds[0]);
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
      YTDatabaseSessionInternal database, final String className) {
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
        throw new YTSchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to class "
                + clustersToClasses.get(clusterId));
      }
    }
  }

  public void dropClass(YTDatabaseSessionInternal database, final String className) {
    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (className == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      YTClass cls = classes.get(key);

      if (cls == null) {
        throw new YTSchemaException("Class '" + className + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new YTSchemaException(
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

  protected void doDropClass(YTDatabaseSessionInternal database, String className) {
    dropClassInternal(database, className);
  }

  protected void dropClassInternal(YTDatabaseSessionInternal database, final String className) {
    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (className == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      final YTClass cls = classes.get(key);
      if (cls == null) {
        throw new YTSchemaException("Class '" + className + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new YTSchemaException(
            "Class '"
                + className
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      checkEmbedded();

      for (YTClass superClass : cls.getSuperClasses()) {
        // REMOVE DEPENDENCY FROM SUPERCLASS
        ((YTClassImpl) superClass).removeBaseClassInternal(database, cls);
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
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        //noinspection deprecation
        it.next().onDropClass(database, cls);
      }

      for (YTDatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onDropClass(database, cls);
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void dropView(YTDatabaseSessionInternal database, final String name) {
    try {
      acquireSchemaWriteLock(database);
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (name == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = name.toLowerCase(Locale.ENGLISH);

      YTView cls = views.get(key);

      if (cls == null) {
        throw new YTSchemaException("View '" + name + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new YTSchemaException(
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

  protected void doDropView(YTDatabaseSessionInternal database, String name) {
    dropViewInternal(database, name);
  }

  protected void dropViewInternal(YTDatabaseSessionInternal database, final String view) {
    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (view == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = view.toLowerCase(Locale.ENGLISH);

      final YTView cls = views.get(key);
      if (cls == null) {
        throw new YTSchemaException("View '" + view + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new YTSchemaException(
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
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDropView(database, cls);
      }

      for (YTDatabaseListener oDatabaseListener : database.getListeners()) {
        oDatabaseListener.onDropView(database, cls);
      }
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  protected YTClassImpl createClassInstance(String name) {
    return new YTClassEmbedded(this, name);
  }

  protected YTViewImpl createViewInstance(String name) {
    return new YTViewEmbedded(this, name);
  }

  private static void dropClassIndexes(YTDatabaseSessionInternal database, final YTClass cls) {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    for (final OIndex index : indexManager.getClassIndexes(database, cls.getName())) {
      indexManager.dropIndex(database, index.getName());
    }
  }

  private static void deleteCluster(final YTDatabaseSessionInternal db, final int clusterId) {
    final String clusterName = db.getClusterNameById(clusterId);
    if (clusterName != null) {
      final ORecordIteratorCluster<Record> iteratorCluster = db.browseCluster(clusterName);
      if (iteratorCluster != null) {
        db.executeInTxBatches(
            (Iterable<Record>) iteratorCluster, (session, record) -> record.delete());
        db.dropClusterInternal(clusterId);
      }
    }

    db.getLocalCache().freeCluster(clusterId);
  }

  private void removeClusterClassMap(final YTClass cls) {
    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0) {
        continue;
      }

      clustersToClasses.remove(clusterId);
    }
  }

  private void removeClusterViewMap(final YTView cls) {
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
      YTDatabaseSessionInternal database, final int clusterId, final YTClass cls) {
    acquireSchemaWriteLock(database);
    try {
      if (clusterId < 0) {
        return;
      }

      checkEmbedded();

      final YTClass existingCls = clustersToClasses.get(clusterId);
      if (existingCls != null && !cls.equals(existingCls)) {
        throw new YTSchemaException(
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

  void addClusterForView(YTDatabaseSessionInternal database, final int clusterId,
      final YTView view) {
    acquireSchemaWriteLock(database);
    try {
      if (clusterId < 0) {
        return;
      }

      checkEmbedded();

      final YTView existingView = clustersToViews.get(clusterId);
      if (existingView != null && !view.equals(existingView)) {
        throw new YTSchemaException(
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

  void removeClusterForClass(YTDatabaseSessionInternal database, int clusterId) {
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

  void removeClusterForView(YTDatabaseSessionInternal database, int clusterId) {
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
