package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.Iterator;

/**
 *
 */
public class EdgeToVertexIterator implements Iterator<Vertex> {

  private final Iterator<Edge> edgeIterator;
  private final Direction direction;

  public EdgeToVertexIterator(Iterator<Edge> iterator, Direction direction) {
    if (direction == Direction.BOTH) {
      throw new IllegalArgumentException(
          "edge to vertex iterator does not support BOTH as direction");
    }
    this.edgeIterator = iterator;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    return edgeIterator.hasNext();
  }

  @Override
  public Vertex next() {
    Edge edge = edgeIterator.next();
    switch (direction) {
      case OUT:
        return edge.getTo();
      case IN:
        return edge.getFrom();
    }
    return null;
  }
}
