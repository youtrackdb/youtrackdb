package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Iterator;

/**
 *
 */
public class OEdgeToVertexIterator implements Iterator<Vertex> {

  private final Iterator<Edge> edgeIterator;
  private final ODirection direction;

  public OEdgeToVertexIterator(Iterator<Edge> iterator, ODirection direction) {
    if (direction == ODirection.BOTH) {
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
