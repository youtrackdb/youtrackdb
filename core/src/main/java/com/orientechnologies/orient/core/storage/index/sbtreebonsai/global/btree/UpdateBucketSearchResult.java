package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public final class UpdateBucketSearchResult {

  private final IntList insertionIndexes;
  private final IntArrayList path;
  private final int itemIndex;

  public UpdateBucketSearchResult(
      final IntList insertionIndexes, final IntArrayList path, final int itemIndex) {
    this.insertionIndexes = insertionIndexes;
    this.path = path;
    this.itemIndex = itemIndex;
  }

  public long getLastPathItem() {
    return getPath().get(getPath().size() - 1);
  }

  public IntList getInsertionIndexes() {
    return insertionIndexes;
  }

  public IntArrayList getPath() {
    return path;
  }

  public int getItemIndex() {
    return itemIndex;
  }
}
