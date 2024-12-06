package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewManager;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;

public class QueryDatabaseState {

  private ResultSet resultSet = null;
  private final IntArrayList usedClusters = new IntArrayList();
  private final ArrayList<String> usedIndexes = new ArrayList<>();

  public QueryDatabaseState() {
  }

  public QueryDatabaseState(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public void setResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public void close(DatabaseSessionInternal database) {
    if (resultSet != null) {
      resultSet.close();
    }
    this.closeInternal(database);
  }

  public void closeInternal(DatabaseSessionInternal database) {
    if (database.isRemote()) {
      return;
    }
    ViewManager views = database.getSharedContext().getViewManager();
    for (var i = 0; i < usedClusters.size(); i++) {
      views.endUsingViewCluster(usedClusters.getInt(i));
    }
    this.usedClusters.clear();
    for (String index : this.usedIndexes) {
      views.endUsingViewIndex(index);
    }
    this.usedIndexes.clear();
  }

  public void addViewUseCluster(int clusterId) {
    this.usedClusters.add(clusterId);
  }

  public void addViewUseIndex(String index) {
    this.usedIndexes.add(index);
  }
}
