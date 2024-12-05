package com.orientechnologies.core.db.viewmanager;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.collate.OCollate;
import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.db.OScenarioThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YTLiveQueryResultListener;
import com.orientechnologies.core.db.YouTrackDBInternal;
import com.orientechnologies.core.db.document.YTDatabaseSessionEmbedded;
import com.orientechnologies.core.exception.YTConfigurationException;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.index.OClassIndexManager;
import com.orientechnologies.core.index.OCompositeIndexDefinition;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexDefinition;
import com.orientechnologies.core.index.OIndexDefinitionFactory;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.index.OPropertyMapIndexDefinition.INDEX_BY;
import com.orientechnologies.core.metadata.schema.OIndexConfigProperty;
import com.orientechnologies.core.metadata.schema.OViewConfig;
import com.orientechnologies.core.metadata.schema.OViewIndexConfig;
import com.orientechnologies.core.metadata.schema.OViewRemovedMetadata;
import com.orientechnologies.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.metadata.schema.YTView;
import com.orientechnologies.core.metadata.schema.YTViewImpl;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.sql.parser.OProjection;
import com.orientechnologies.core.sql.parser.OProjectionItem;
import com.orientechnologies.core.sql.parser.OSelectStatement;
import com.orientechnologies.core.sql.parser.OStatement;
import com.orientechnologies.core.sql.parser.OStatementCache;
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

  private final YouTrackDBInternal oxygenDB;
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

  public ViewManager(YouTrackDBInternal orientDb, String dbName) {
    this.oxygenDB = orientDb;
    this.dbName = dbName;
  }

  protected void init() {
    oxygenDB.executeNoAuthorizationAsync(
        dbName,
        (db) -> {
          // do this to make sure that the storage is already initialized and so is the shared
          // context.
          // you just don't need the db passed as a param here
          registerLiveUpdates(db);
          return null;
        });
  }

  private void registerLiveUpdates(YTDatabaseSessionInternal db) {
    boolean liveViewsExist = false;
    Collection<YTView> views = db.getMetadata().getSchema().getViews();
    for (YTView view : views) {
      liveViewsExist = registerLiveUpdateFor(db, view.getName()) || liveViewsExist;
    }
  }

  public boolean registerLiveUpdateFor(YTDatabaseSession db, String viewName) {
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

  private void updateViews(YTDatabaseSessionInternal db) {
    try {
      YTView view;
      do {
        cleanUnusedViewClusters(db);
        cleanUnusedViewIndexes(db);
        view = getNextViewToUpdate(db);
        if (view != null) {
          updateView(view, db);
        }
      } while (view != null && !closed);

    } catch (Exception e) {
      OLogManager.instance().warn(this, "Failed to update views", e);
    }
  }

  public void close() {
    closed = true;
    if (timerTask != null) {
      timerTask.cancel();
    }
  }

  public void cleanUnusedViewClusters(YTDatabaseSession db) {
    if (((YTDatabaseSessionEmbedded) db).getStorage().isIcrementalBackupRunning()) {
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

  public void cleanUnusedViewIndexes(YTDatabaseSessionInternal db) {
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

  public YTView getNextViewToUpdate(YTDatabaseSessionInternal db) {
    YTSchema schema = db.getMetadata().getImmutableSchemaSnapshot();

    Collection<YTView> views = schema.getViews();

    if (views.isEmpty()) {
      return null;
    }
    for (YTView view : views) {
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

  private boolean isViewRefreshing(YTView view) {
    return this.refreshing.contains(view.getName());
  }

  private boolean isLiveUpdate(YTDatabaseSessionInternal db, YTView view) {
    return OViewConfig.UPDATE_STRATEGY_LIVE.equalsIgnoreCase(view.getUpdateStrategy());
  }

  protected boolean buildOnThisNode(YTDatabaseSessionInternal db, YTView name) {
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
  private boolean needsUpdateBasedOnWatchRules(YTView view, YTDatabaseSessionInternal db) {
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

  private boolean isUpdateExpiredFor(YTView view, YTDatabaseSessionInternal db) {
    long lastUpdate = view.getLastRefreshTime();
    int updateInterval = view.getUpdateIntervalSeconds();
    return lastUpdate + (updateInterval * 1000L) < System.currentTimeMillis();
  }

  public void updateView(YTView view, YTDatabaseSessionInternal db) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          this.updateViewInternal(view, db);
          return null;
        });
  }

  public void updateViewInternal(YTView view, YTDatabaseSessionInternal db) {
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

      OLogManager.instance().info(this, "Starting refresh of view '%s'", viewName);
      long lastRefreshTime = System.currentTimeMillis();
      String clusterName = createNextClusterNameFor(view, db);
      int cluster = db.getClusterIdByName(clusterName);

      String query = view.getQuery();
      String originRidField = view.getOriginRidField();
      List<OIndex> indexes = createNewIndexesForView(view, cluster, db);

      try {
        fillView(db, viewName, query, originRidField, clusterName, indexes);
      } catch (RuntimeException e) {
        for (OIndex index : indexes) {
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
      OViewRemovedMetadata oldMetadata =
          ((YTViewImpl) view).replaceViewClusterAndIndex(db, cluster, indexes, lastRefreshTime);
      OLogManager.instance()
          .info(
              this,
              "Replaced for view '%s' clusters '%s' with '%s'",
              viewName,
              Arrays.stream(oldMetadata.getClusters())
                  .mapToObj((i) -> i + " => " + db.getClusterNameById(i))
                  .collect(Collectors.toList())
                  .toString(),
              cluster + " => " + db.getClusterNameById(cluster));
      OLogManager.instance()
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

      OLogManager.instance().info(this, "Finished refresh of view '%s'", viewName);
    } finally {
      refreshing.remove(viewName);
    }
    cleanUnusedViewIndexes(db);
    cleanUnusedViewClusters(db);
  }

  private void fillView(
      YTDatabaseSessionInternal db,
      String viewName,
      String query,
      String originRidField,
      String clusterName,
      List<OIndex> indexes) {
    int iterationCount = 0;
    try (YTResultSet rs = db.query(query)) {
      db.begin();
      while (rs.hasNext()) {
        YTResult item = rs.next();
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
      YTResult item,
      YTDatabaseSessionInternal db,
      String originRidField,
      String viewName,
      String clusterName,
      List<OIndex> indexes) {
    db.begin();
    YTEntity newRow = copyElement(item, db);
    if (originRidField != null) {
      newRow.setProperty(originRidField, item.getIdentity().orElse(item.getProperty("@rid")));
      newRow.setProperty("@view", viewName);
    }
    db.save(newRow, clusterName);
    OClassIndexManager.addIndexesEntries(db, (YTEntityImpl) newRow, indexes);
    db.commit();
  }

  private List<OIndex> createNewIndexesForView(
      YTView view, int cluster, YTDatabaseSessionInternal db) {
    try {
      List<OIndex> result = new ArrayList<>();
      OIndexManagerAbstract idxMgr = db.getMetadata().getIndexManagerInternal();
      for (OViewIndexConfig cfg : view.getRequiredIndexesInfo()) {
        OIndexDefinition definition = createIndexDefinition(view.getName(), cfg.getProperties());
        String indexName = view.getName() + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
        String type = cfg.getType();
        String engine = cfg.getEngine();
        OIndex idx =
            idxMgr.createIndex(
                db, indexName, type, definition, new int[]{cluster}, null, null, engine);
        result.add(idx);
      }
      return result;
    } catch (Exception e) {
      throw YTException.wrapException(
          new YTDatabaseException("Error on creating indexes for view " + view.getName()), e);
    }
  }

  private OIndexDefinition createIndexDefinition(
      String viewName, List<OIndexConfigProperty> requiredIndexesInfo) {
    if (requiredIndexesInfo.size() == 1) {
      String fieldName = requiredIndexesInfo.get(0).getName();
      YTType fieldType = requiredIndexesInfo.get(0).getType();
      YTType linkedType = requiredIndexesInfo.get(0).getLinkedType();
      OCollate collate = requiredIndexesInfo.get(0).getCollate();
      INDEX_BY index_by = requiredIndexesInfo.get(0).getIndexBy();
      return OIndexDefinitionFactory.createSingleFieldIndexDefinition(
          viewName, fieldName, fieldType, linkedType, collate, null, index_by);
    }
    OCompositeIndexDefinition result = new OCompositeIndexDefinition(viewName);
    for (OIndexConfigProperty pair : requiredIndexesInfo) {
      String fieldName = pair.getName();
      YTType fieldType = pair.getType();
      YTType linkedType = pair.getLinkedType();
      OCollate collate = pair.getCollate();
      INDEX_BY index_by = pair.getIndexBy();
      OIndexDefinition definition =
          OIndexDefinitionFactory.createSingleFieldIndexDefinition(
              viewName, fieldName, fieldType, linkedType, collate, null, index_by);
      result.addIndex(definition);
    }
    return result;
  }

  private String createNextClusterNameFor(YTView view, YTDatabaseSessionInternal db) {
    int i = 0;
    String viewName = view.getName();
    while (true) {
      String clusterName = "v_" + i++ + "_" + viewName.toLowerCase(Locale.ENGLISH);

      if (!db.existsCluster(clusterName)) {
        try {
          db.addCluster(clusterName);
          return clusterName;
        } catch (YTConfigurationException e) {
          // Ignore and retry
        }
      }
    }
  }

  private YTEntity copyElement(YTResult item, YTDatabaseSession db) {
    YTEntity newRow = db.newEntity();
    for (String prop : item.getPropertyNames()) {
      if (!prop.equalsIgnoreCase("@rid") && !prop.equalsIgnoreCase("@class")) {
        newRow.setProperty(prop, item.getProperty(prop));
      }
    }
    return newRow;
  }

  public void updateViewAsync(String name, ViewCreationListener listener) {
    oxygenDB.executeNoAuthorizationAsync(
        dbName,
        (databaseSession) -> {
          if (!buildOnThisNode(
              databaseSession, databaseSession.getMetadata().getSchema().getView(name))) {
            return null;
          }
          try {
            YTView view = databaseSession.getMetadata().getSchema().getView(name);
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
            OLogManager.instance().warn(this, "Failed to update views", e);
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
      YTImmutableClass clazz, YTEntityImpl doc,
      YTDatabaseSessionEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public void recordUpdated(
      YTImmutableClass clazz, YTEntityImpl doc,
      YTDatabaseSessionEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public void recordDeleted(
      YTImmutableClass clazz, YTEntityImpl doc,
      YTDatabaseSessionEmbedded oDatabaseDocumentEmbedded) {
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

  private class ViewUpdateListener implements YTLiveQueryResultListener {

    private final String viewName;

    public ViewUpdateListener(String name) {
      this.viewName = name;
    }

    @Override
    public void onCreate(YTDatabaseSession db, YTResult data) {
//      YTView view = db.getMetadata().getSchema().getView(viewName);
//      var dbInternal = (YTDatabaseSessionInternal) db;
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
    public void onUpdate(YTDatabaseSession db, YTResult before, YTResult after) {
//      YTView view = db.getMetadata().getSchema().getView(viewName);
//      if (view != null && view.getOriginRidField() != null) {
//        try (YTResultSet rs =
//            db.query(
//                "SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?",
//                (Object) after.getProperty("@rid"))) {
//          while (rs.hasNext()) {
//            YTResult row = rs.next();
//            row.getEntity()
//                .ifPresent(elem -> updateViewRow(elem, after, view, (YTDatabaseSessionInternal) db));
//          }
//        }
//      }
    }

    private void updateViewRow(
        YTEntity viewRow, YTResult origin, YTView view, YTDatabaseSessionInternal db) {
      db.executeInTx(
          () -> {
            var boundRow = db.bindToSession(viewRow);
            OStatement stm = OStatementCache.get(view.getQuery(), db);
            if (stm instanceof OSelectStatement) {
              OProjection projection = ((OSelectStatement) stm).getProjection();
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
                for (OProjectionItem oProjectionItem : projection.getItems()) {
                  Object value = oProjectionItem.execute(origin, new OBasicCommandContext());
                  boundRow.setProperty(oProjectionItem.getProjectionAliasAsString(), value);
                }
              }
              boundRow.save();
            }
          });
    }

    @Override
    public void onDelete(YTDatabaseSession db, YTResult data) {
//      YTView view = db.getMetadata().getSchema().getView(viewName);
//      if (view != null && view.getOriginRidField() != null) {
//        try (YTResultSet rs =
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
    public void onError(YTDatabaseSession database, YTException exception) {
      OLogManager.instance().error(ViewManager.this, "Error updating view " + viewName, exception);
    }

    @Override
    public void onEnd(YTDatabaseSession database) {
    }
  }
}
