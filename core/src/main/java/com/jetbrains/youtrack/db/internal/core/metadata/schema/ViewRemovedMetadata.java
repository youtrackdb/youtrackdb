package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import java.util.List;

public class ViewRemovedMetadata {

  int[] clusters;
  List<String> indexes;

  public ViewRemovedMetadata(int[] clusters, List<String> oldIndexes) {
    this.clusters = clusters;
    this.indexes = oldIndexes;
  }

  public int[] getClusters() {
    return clusters;
  }

  public List<String> getIndexes() {
    return indexes;
  }
}
