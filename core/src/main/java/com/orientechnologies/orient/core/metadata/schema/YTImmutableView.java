package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class YTImmutableView extends YTImmutableClass implements YTView {

  private final int updateIntervalSeconds;
  private final List<String> watchClasses;
  private final List<String> nodes;
  private final List<OViewIndexConfig> requiredIndexesInfo;
  private final String query;
  private final String originRidField;
  private final boolean updatable;
  private final String updateStrategy;
  private final Set<String> activeIndexNames;
  private final long lastRefreshTime;

  public YTImmutableView(YTDatabaseSessionInternal session, YTView view, YTImmutableSchema schema) {
    super(session, view, schema);
    this.query = view.getQuery();
    this.updateIntervalSeconds = view.getUpdateIntervalSeconds();
    this.watchClasses =
        view.getWatchClasses() == null ? null : new ArrayList<>(view.getWatchClasses());
    this.originRidField = view.getOriginRidField();
    this.updatable = view.isUpdatable();
    this.nodes = view.getNodes() == null ? null : new ArrayList<>(view.getNodes());
    this.requiredIndexesInfo =
        view.getRequiredIndexesInfo() == null
            ? null
            : new ArrayList<>(view.getRequiredIndexesInfo());
    this.updateStrategy = view.getUpdateStrategy();
    this.activeIndexNames = view.getActiveIndexNames();
    this.lastRefreshTime = view.getLastRefreshTime();
  }

  public void getRawClassIndexes(final Collection<OIndex> indexes) {
    YTDatabaseSessionInternal database = getDatabase();
    OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    for (String indexName : activeIndexNames) {
      OIndex index = indexManager.getIndex(database, indexName);
      indexes.add(index);
    }
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public int getUpdateIntervalSeconds() {
    return updateIntervalSeconds;
  }

  @Override
  public List<String> getWatchClasses() {
    return watchClasses;
  }

  public String getOriginRidField() {
    return originRidField;
  }

  public boolean isUpdatable() {
    return updatable;
  }

  @Override
  public List<String> getNodes() {
    return nodes;
  }

  @Override
  public List<OViewIndexConfig> getRequiredIndexesInfo() {
    return requiredIndexesInfo;
  }

  @Override
  public String getUpdateStrategy() {
    return updateStrategy;
  }

  @Override
  public long count(YTDatabaseSession session, boolean isPolymorphic) {
    return getDatabase().countView(getName());
  }

  @Override
  public Set<String> getActiveIndexNames() {
    return activeIndexNames;
  }

  @Override
  public long getLastRefreshTime() {
    return lastRefreshTime;
  }
}
