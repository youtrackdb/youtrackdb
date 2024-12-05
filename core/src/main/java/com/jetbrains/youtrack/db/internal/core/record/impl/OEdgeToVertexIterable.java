package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.common.util.OSizeable;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 */
public class OEdgeToVertexIterable implements Iterable<Vertex>, OSizeable {

  private final Iterable<Edge> edges;
  private final ODirection direction;

  public OEdgeToVertexIterable(Iterable<Edge> edges, ODirection direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<Vertex> iterator() {
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
    Iterator<Edge> iterator = edges.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      count++;
    }
    return count;
  }
}
