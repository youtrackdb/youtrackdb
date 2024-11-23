package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;

public class OQueryDatabaseState {

  private OResultSet resultSet = null;
  private final IntArrayList usedClusters = new IntArrayList();
  private final ArrayList<String> usedIndexes = new ArrayList<>();

  public OQueryDatabaseState() {
  }

  public OQueryDatabaseState(OResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public void setResultSet(OResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public OResultSet getResultSet() {
    return resultSet;
  }

  public void close(ODatabaseSessionInternal database) {
    if (resultSet != null) {
      resultSet.close();
    }
    this.closeInternal(database);
  }

  public void closeInternal(ODatabaseSessionInternal database) {
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
