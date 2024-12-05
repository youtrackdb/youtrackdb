package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;

public class OQueryDatabaseState {

  private YTResultSet resultSet = null;
  private final IntArrayList usedClusters = new IntArrayList();
  private final ArrayList<String> usedIndexes = new ArrayList<>();

  public OQueryDatabaseState() {
  }

  public OQueryDatabaseState(YTResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public void setResultSet(YTResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public YTResultSet getResultSet() {
    return resultSet;
  }

  public void close(YTDatabaseSessionInternal database) {
    if (resultSet != null) {
      resultSet.close();
    }
    this.closeInternal(database);
  }

  public void closeInternal(YTDatabaseSessionInternal database) {
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
