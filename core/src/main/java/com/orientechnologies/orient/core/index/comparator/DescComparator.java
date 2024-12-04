package com.orientechnologies.orient.core.index.comparator;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.YTRID;
import java.util.Comparator;

public class DescComparator implements Comparator<ORawPair<Object, YTRID>> {

  public static final DescComparator INSTANCE = new DescComparator();

  @Override
  public int compare(ORawPair<Object, YTRID> entryOne, ORawPair<Object, YTRID> entryTwo) {
    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
