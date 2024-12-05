package com.orientechnologies.core.index.comparator;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.id.YTRID;
import java.util.Comparator;

public class AscComparator implements Comparator<ORawPair<Object, YTRID>> {

  public static final AscComparator INSTANCE = new AscComparator();

  @Override
  public int compare(ORawPair<Object, YTRID> entryOne, ORawPair<Object, YTRID> entryTwo) {
    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
