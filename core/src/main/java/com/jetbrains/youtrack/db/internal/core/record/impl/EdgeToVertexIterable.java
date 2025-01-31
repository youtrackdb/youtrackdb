package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 */
public class EdgeToVertexIterable implements Iterable<Vertex>, Sizeable {

  private final Iterable<Edge> edges;
  private final Direction direction;

  public EdgeToVertexIterable(Iterable<Edge> edges, Direction direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<Vertex> iterator() {
    return new EdgeToVertexIterator(edges.iterator(), direction);
  }

  @Override
  public int size() {
    if (edges == null) {
      return 0;
    }
    if (edges instanceof Sizeable) {
      return ((Sizeable) edges).size();
    }
    if (edges instanceof Collection) {
      return ((Collection) edges).size();
    }
    var iterator = edges.iterator();
    var count = 0;
    while (iterator.hasNext()) {
      count++;
    }
    return count;
  }
}
