package com.jetbrains.youtrack.db.internal.core.index.comparator;

import com.jetbrains.youtrack.db.internal.common.comparator.ODefaultComparator;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.util.Comparator;

public class DescComparator implements Comparator<ORawPair<Object, YTRID>> {

  public static final DescComparator INSTANCE = new DescComparator();

  @Override
  public int compare(ORawPair<Object, YTRID> entryOne, ORawPair<Object, YTRID> entryTwo) {
    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
