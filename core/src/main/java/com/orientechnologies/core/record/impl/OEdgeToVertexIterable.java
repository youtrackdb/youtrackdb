package com.orientechnologies.core.record.impl;

import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.YTEdge;
import com.orientechnologies.core.record.YTVertex;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 */
public class OEdgeToVertexIterable implements Iterable<YTVertex>, OSizeable {

  private final Iterable<YTEdge> edges;
  private final ODirection direction;

  public OEdgeToVertexIterable(Iterable<YTEdge> edges, ODirection direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<YTVertex> iterator() {
    return new OEdgeToVertexIterator(edges.iterator(), direction);
  }

  @Override
  public int size() {
    if (edges == null) {
      return 0;
    }
    if (edges instanceof OSizeable) {
      return ((OSizeable) edges).size();
    }
    if (edges instanceof Collection) {
      return ((Collection) edges).size();
    }
    Iterator<YTEdge> iterator = edges.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      count++;
    }
    return count;
  }
}
