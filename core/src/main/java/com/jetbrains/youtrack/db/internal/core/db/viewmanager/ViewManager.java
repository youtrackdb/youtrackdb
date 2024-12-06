package com.jetbrains.youtrack.db.internal.core.db.viewmanager;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.collate.Collate;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.LiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.core.db.ScenarioThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseSessionEmbedded;
import com.jetbrains.youtrack.db.internal.core.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.index.ClassIndexManager;
import com.jetbrains.youtrack.db.internal.core.index.CompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinitionFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition.INDEX_BY;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.IndexConfigProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaView;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaViewImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ViewConfig;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ViewIndexConfig;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ViewRemovedMetadata;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjectionItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.StatementCache;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ViewManager {

  private final YouTrackDBInternal youTrackDb;
  private final String dbName;
  private final boolean viewsExist = false;

  /**
   * To retain clusters that are being used in queries until the queries are closed.
   *
   * <p>view -> cluster -> number of visitors
   */
  private final ConcurrentMap<Integer, AtomicInteger> viewCluserVisitors =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<Integer, String> oldClustersPerViews = new ConcurrentHashMap<>();
  private final IntArrayList clustersToDrop = IntArrayList.of();

  /**
   * To retain indexes that are being used in queries until the queries are closed.
   *
   * <p>view -> index -> number of visitors
   */
  private final ConcurrentMap<String, AtomicInteger> viewIndexVisitors = new ConcurrentHashMap<>();

  private final List<String> indexesToDrop = Collections.synchronizedList(new ArrayList<>());

  private final ConcurrentMap<String, Long> lastChangePerClass = new ConcurrentHashMap<>();
  private final Set<String> refreshing = Collections.synchronizedSet(new HashSet<>());

  private volatile TimerTask timerTask;
  private volatile boolean closed = false;

  public ViewManager(YouTrackDBInternal youTrackDb, String dbName) {
    this.youTrackDb = youTrackDb;
    this.dbName = dbName;
  }

  protected void init() {
    youTrackDb.executeNoAuthorizationAsync(
        dbName,
        (db) -> {
          // do this to make sure that the storage is already initialized and so is the shared
          // context.
          // you just don't need the db passed as a param here
          registerLiveUpdates(db);
          return null;
        });
  }

  private void registerLiveUpdates(DatabaseSessionInternal db) {
    boolean liveViewsExist = false;
    Collection<SchemaView> views = db.getMetadata().getSchema().getViews();
    for (SchemaView view : views) {
      liveViewsExist = registerLiveUpdateFor(db, view.getName()) || liveViewsExist;
    }
  }

  public boolean registerLiveUpdateFor(DatabaseSession db, String viewName) {
    return false;
  }

  public void load() {
    closed = false;
    init();
    start();
  }

  public void create() {
    closed = false;
    init();
    start();
  }

  public void start() {
    schedule();
  }

  private void schedule() {
  }

  private void updateViews(DatabaseSessionInternal db) {
    try {
      SchemaView view;
      do {
        cleanUnusedViewClusters(db);
        cleanUnusedViewIndexes(db);
        view = getNextViewToUpdate(db);
        if (view != null) {
          updateView(view, db);
        }
      } while (view != null && !closed);

    } catch (Exception e) {
      LogManager.instance().warn(this, "Failed to update views", e);
    }
  }

  public void close() {
    closed = true;
    if (timerTask != null) {
      timerTask.cancel();
    }
  }

  public void cleanUnusedViewClusters(DatabaseSession db) {
    if (((DatabaseSessionEmbedded) db).getStorage().isIcrementalBackupRunning()) {
      // backup is running handle delete the next run
      return;
    }
    IntArrayList clusters = new IntArrayList();

    synchronized (this) {
      IntIterator iter = clustersToDrop.intIterator();
      while (iter.hasNext()) {
        int cluster = iter.nextInt();

        AtomicInteger visitors = viewCluserVisitors.get(cluster);
        if (visitors == null || visitors.get() <= 0) {
          iter.remove();
          clusters.add(cluster);
        }
      }
    }

    for (int i = 0; i < clusters.size(); i++) {
      var cluster = clusters.getInt(i);
      db.dropCluster(cluster);
      viewCluserVisitors.remove(cluster);
      oldClustersPerViews.remove(cluster);
    }
  }

  public void cleanUnusedViewIndexes(DatabaseSessionInternal db) {
    if (db.getStorage().isIcrementalBackupRunning()) {
      // backup is running handle delete the next run
      return;
    }
    List<String> indexes = new ArrayList<>();
    synchronized (this) {
      Iterator<String> iter = indexesToDrop.iterator();
      while (iter.hasNext()) {
        String index = iter.next();
        AtomicInteger visitors = viewIndexVisitors.get(index);
        if (visitors == null || visitors.get() <= 0) {
          iter.remove();
          indexes.add(index);
        }
      }
    }
    for (String index : indexes) {
      db.getMetadata().getIndexManagerInternal().dropIndex(db, index);
      viewIndexVisitors.remove(index);
    }
  }

  public SchemaView getNextViewToUpdate(DatabaseSessionInternal db) {
    Schema schema = db.getMetadata().getImmutableSchemaSnapshot();

    Collection<SchemaView> views = schema.getViews();

    if (views.isEmpty()) {
      return null;
    }
    for (SchemaView view : views) {
      if (!buildOnThisNode(db, view)) {
        continue;
      }
      if (isLiveUpdate(db, view)) {
        continue;
      }
      if (!isUpdateExpiredFor(view, db)) {
        continue;
      }
      if (!needsUpdateBasedOnWatchRules(view, db)) {
        continue;
      }
      if (isViewRefreshing(view)) {
        continue;
      }
      return db.getMetadata().getSchema().getView(view.getName());
    }

    return null;
  }

  private boolean isViewRefreshing(SchemaView view) {
    return this.refreshing.contains(view.getName());
  }

  private boolean isLiveUpdate(DatabaseSessionInternal db, SchemaView view) {
    return ViewConfig.UPDATE_STRATEGY_LIVE.equalsIgnoreCase(view.getUpdateStrategy());
  }

  protected boolean buildOnThisNode(DatabaseSessionInternal db, SchemaView name) {
    return true;
  }

  /**
   * Checks if the view could need an update based on watch rules
   *
   * @param view view name
   * @param db   db instance
   * @return true if there are no watch rules for this view; true if there are watch rules and some
   * of them happened since last update; true if the view was never updated; false otherwise.
   */
  private boolean needsUpdateBasedOnWatchRules(SchemaView view, DatabaseSessionInternal db) {
    if (view == null) {
      return false;
    }

    long lastViewUpdate = view.getLastRefreshTime();

    List<String> watchRules = view.getWatchClasses();
    if (watchRules == null || watchRules.size() == 0) {
      return true;
    }

    for (String watchRule : watchRules) {
      Long classChangeTimestamp = lastChangePerClass.get(watchRule.toLowerCase(Locale.ENGLISH));
      if (classChangeTimestamp == null) {
        continue;
      }
      if (classChangeTimestamp >= lastViewUpdate) {
        return true;
      }
    }

    return false;
  }

  private boolean isUpdateExpiredFor(SchemaView view, DatabaseSessionInternal db) {
    long lastUpdate = view.getLastRefreshTime();
    int updateInterval = view.getUpdateIntervalSeconds();
    return lastUpdate + (updateInterval * 1000L) < System.currentTimeMillis();
  }

  public void updateView(SchemaView view, DatabaseSessionInternal db) {
    ScenarioThreadLocal.executeAsDistributed(
        () -> {
          this.updateViewInternal(view, db);
          return null;
        });
  }

  public void updateViewInternal(SchemaView view, DatabaseSessionInternal db) {
    if (db.getStorage().isIcrementalBackupRunning() || closed) {
      // backup is running handle rebuild the next run
      return;
    }
    String viewName = view.getName();
    try {
      synchronized (this) {
        if (refreshing.contains(viewName)) {
          return;
        }
        refreshing.add(viewName);
      }

      LogManager.instance().info(this, "Starting refresh of view '%s'", viewName);
      long lastRefreshTime = System.currentTimeMillis();
      String clusterName = createNextClusterNameFor(view, db);
      int cluster = db.getClusterIdByName(clusterName);

      String query = view.getQuery();
      String originRidField = view.getOriginRidField();
      List<Index> indexes = createNewIndexesForView(view, cluster, db);

      try {
        fillView(db, viewName, query, originRidField, clusterName, indexes);
      } catch (RuntimeException e) {
        for (Index index : indexes) {
          db.getMetadata().getIndexManagerInternal().dropIndex(db, index.getName());
        }
        db.dropCluster(cluster);
        throw e;
      }

      view = db.getMetadata().getSchema().getView(viewName);
      if (view == null) {
        // the view was dropped in the meantime
        clustersToDrop.add(cluster);
        indexes.forEach(x -> indexesToDrop.add(x.getName()));
        return;
      }
      ViewRemovedMetadata oldMetadata =
          ((SchemaViewImpl) view).replaceViewClusterAndIndex(db, cluster, indexes, lastRefreshTime);
      LogManager.instance()
          .info(
              this,
              "Replaced for view '%s' clusters '%s' with '%s'",
              viewName,
              Arrays.stream(oldMetadata.getClusters())
                  .mapToObj((i) -> i + " => " + db.getClusterNameById(i))
                  .collect(Collectors.toList())
                  .toString(),
              cluster + " => " + db.getClusterNameById(cluster));
      LogManager.instance()
          .info(
              this,
              "Replaced for view '%s' indexes '%s' with '%s'",
              viewName,
              oldMetadata.getIndexes().toString(),
              indexes.stream().map((i) -> i.getName()).collect(Collectors.toList()).toString());
      for (int i : oldMetadata.getClusters()) {
        clustersToDrop.add(i);
        oldClustersPerViews.put(i, viewName);
      }
      oldMetadata
          .getIndexes()
          .forEach(
              idx -> {
                indexesToDrop.add(idx);
              });

      LogManager.instance().info(this, "Finished refresh of view '%s'", viewName);
    } finally {
      refreshing.remove(viewName);
    }
    cleanUnusedViewIndexes(db);
    cleanUnusedViewClusters(db);
  }

  private void fillView(
      DatabaseSessionInternal db,
      String viewName,
      String query,
      String originRidField,
      String clusterName,
      List<Index> indexes) {
    int iterationCount = 0;
    try (ResultSet rs = db.query(query)) {
      db.begin();
      while (rs.hasNext()) {
        Result item = rs.next();
        addItemToView(item, db, originRidField, viewName, clusterName, indexes);
        if (iterationCount % 100 == 0) {
          db.commit();
          db.begin();
        }
        iterationCount++;
      }
      db.commit();
    }
  }

  private void addItemToView(
      Result item,
      DatabaseSessionInternal db,
      String originRidField,
      String viewName,
      String clusterName,
      List<Index> indexes) {
    db.begin();
    Entity newRow = copyElement(item, db);
    if (originRidField != null) {
      newRow.setProperty(originRidField, item.getIdentity().orElse(item.getProperty("@rid")));
      newRow.setProperty("@view", viewName);
    }
    db.save(newRow, clusterName);
    ClassIndexManager.addIndexesEntries(db, (EntityImpl) newRow, indexes);
    db.commit();
  }

  private List<Index> createNewIndexesForView(
      SchemaView view, int cluster, DatabaseSessionInternal db) {
    try {
      List<Index> result = new ArrayList<>();
      IndexManagerAbstract idxMgr = db.getMetadata().getIndexManagerInternal();
      for (ViewIndexConfig cfg : view.getRequiredIndexesInfo()) {
        IndexDefinition definition = createIndexDefinition(view.getName(), cfg.getProperties());
        String indexName = view.getName() + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
        String type = cfg.getType();
        String engine = cfg.getEngine();
        Index idx =
            idxMgr.createIndex(
                db, indexName, type, definition, new int[]{cluster}, null, null, engine);
        result.add(idx);
      }
      return result;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException("Error on creating indexes for view " + view.getName()), e);
    }
  }

  private IndexDefinition createIndexDefinition(
      String viewName, List<IndexConfigProperty> requiredIndexesInfo) {
    if (requiredIndexesInfo.size() == 1) {
      String fieldName = requiredIndexesInfo.get(0).getName();
      PropertyType fieldType = requiredIndexesInfo.get(0).getType();
      PropertyType linkedType = requiredIndexesInfo.get(0).getLinkedType();
      Collate collate = requiredIndexesInfo.get(0).getCollate();
      INDEX_BY index_by = requiredIndexesInfo.get(0).getIndexBy();
      return IndexDefinitionFactory.createSingleFieldIndexDefinition(
          viewName, fieldName, fieldType, linkedType, collate, null, index_by);
    }
    CompositeIndexDefinition result = new CompositeIndexDefinition(viewName);
    for (IndexConfigProperty pair : requiredIndexesInfo) {
      String fieldName = pair.getName();
      PropertyType fieldType = pair.getType();
      PropertyType linkedType = pair.getLinkedType();
      Collate collate = pair.getCollate();
      INDEX_BY index_by = pair.getIndexBy();
      IndexDefinition definition =
          IndexDefinitionFactory.createSingleFieldIndexDefinition(
              viewName, fieldName, fieldType, linkedType, collate, null, index_by);
      result.addIndex(definition);
    }
    return result;
  }

  private String createNextClusterNameFor(SchemaView view, DatabaseSessionInternal db) {
    int i = 0;
    String viewName = view.getName();
    while (true) {
      String clusterName = "v_" + i++ + "_" + viewName.toLowerCase(Locale.ENGLISH);

      if (!db.existsCluster(clusterName)) {
        try {
          db.addCluster(clusterName);
          return clusterName;
        } catch (ConfigurationException e) {
          // Ignore and retry
        }
      }
    }
  }

  private Entity copyElement(Result item, DatabaseSession db) {
    Entity newRow = db.newEntity();
    for (String prop : item.getPropertyNames()) {
      if (!prop.equalsIgnoreCase("@rid") && !prop.equalsIgnoreCase("@class")) {
        newRow.setProperty(prop, item.getProperty(prop));
      }
    }
    return newRow;
  }

  public void updateViewAsync(String name, ViewCreationListener listener) {
    youTrackDb.executeNoAuthorizationAsync(
        dbName,
        (databaseSession) -> {
          if (!buildOnThisNode(
              databaseSession, databaseSession.getMetadata().getSchema().getView(name))) {
            return null;
          }
          try {
            SchemaView view = databaseSession.getMetadata().getSchema().getView(name);
            if (view != null) {
              updateView(view, databaseSession);
            }
            if (listener != null) {
              listener.afterCreate(databaseSession, name);
            }
          } catch (Exception e) {
            if (listener != null) {
              listener.onError(name, e);
            }
            LogManager.instance().warn(this, "Failed to update views", e);
          }
          return null;
        });
  }

  public void startUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      item = new AtomicInteger(0);
      viewCluserVisitors.put(cluster, item);
    }
    item.incrementAndGet();
  }

  public void endUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      return;
    }
    item.decrementAndGet();
  }

  public void recordAdded(
      SchemaImmutableClass clazz, EntityImpl doc,
      DatabaseSessionEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public void recordUpdated(
      SchemaImmutableClass clazz, EntityImpl doc,
      DatabaseSessionEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public void recordDeleted(
      SchemaImmutableClass clazz, EntityImpl doc,
      DatabaseSessionEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public String getViewFromOldCluster(int clusterId) {
    return oldClustersPerViews.get(clusterId);
  }

  public void endUsingViewIndex(String indexName) {
    AtomicInteger item = viewIndexVisitors.get(indexName);
    if (item == null) {
      return;
    }
    item.decrementAndGet();
  }

  public void startUsingViewIndex(String indexName) {
    AtomicInteger item = viewIndexVisitors.get(indexName);
    if (item == null) {
      item = new AtomicInteger(0);
      viewIndexVisitors.put(indexName, item);
    }
    item.incrementAndGet();
  }

  private class ViewUpdateListener implements LiveQueryResultListener {

    private final String viewName;

    public ViewUpdateListener(String name) {
      this.viewName = name;
    }

    @Override
    public void onCreate(DatabaseSession db, Result data) {
//      SchemaView view = db.getMetadata().getSchema().getView(viewName);
//      var dbInternal = (DatabaseSessionInternal) db;
//      if (view != null) {
//        int cluster = view.getClusterIds()[0];
//        addItemToView(
//            data,
//            dbInternal,
//            view.getOriginRidField(),
//            view.getName(),
//            dbInternal.getClusterNameById(cluster),
//            new ArrayList<>(view.getIndexes(db)));
//      }
    }

    @Override
    public void onUpdate(DatabaseSession db, Result before, Result after) {
//      SchemaView view = db.getMetadata().getSchema().getView(viewName);
//      if (view != null && view.getOriginRidField() != null) {
//        try (ResultSet rs =
//            db.query(
//                "SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?",
//                (Object) after.getProperty("@rid"))) {
//          while (rs.hasNext()) {
//            Result row = rs.next();
//            row.getEntity()
//                .ifPresent(elem -> updateViewRow(elem, after, view, (DatabaseSessionInternal) db));
//          }
//        }
//      }
    }

    private void updateViewRow(
        Entity viewRow, Result origin, SchemaView view, DatabaseSessionInternal db) {
      db.executeInTx(
          () -> {
            var boundRow = db.bindToSession(viewRow);
            SQLStatement stm = StatementCache.get(view.getQuery(), db);
            if (stm instanceof SQLSelectStatement) {
              SQLProjection projection = ((SQLSelectStatement) stm).getProjection();
              if (projection == null
                  || (projection.getItems().isEmpty() && projection.getItems().get(0).isAll())) {
                for (String s : origin.getPropertyNames()) {
                  if ("@rid".equalsIgnoreCase(s)
                      || "@class".equalsIgnoreCase(s)
                      || "@version".equalsIgnoreCase(s)) {
                    continue;
                  }
                  Object value = origin.getProperty(s);
                  boundRow.setProperty(s, value);
                }
              } else {
                for (SQLProjectionItem oProjectionItem : projection.getItems()) {
                  Object value = oProjectionItem.execute(origin, new BasicCommandContext());
                  boundRow.setProperty(oProjectionItem.getProjectionAliasAsString(), value);
                }
              }
              boundRow.save();
            }
          });
    }

    @Override
    public void onDelete(DatabaseSession db, Result data) {
//      SchemaView view = db.getMetadata().getSchema().getView(viewName);
//      if (view != null && view.getOriginRidField() != null) {
//        try (ResultSet rs =
//            db.query(
//                "SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?",
//                (Object) data.getProperty("@rid"))) {
//          while (rs.hasNext()) {
//            rs.next().getEntity().ifPresent(x -> x.delete());
//          }
//        }
//      }
    }

    @Override
    public void onError(DatabaseSession database, BaseException exception) {
      LogManager.instance().error(ViewManager.this, "Error updating view " + viewName, exception);
    }

    @Override
    public void onEnd(DatabaseSession database) {
    }
  }
}
