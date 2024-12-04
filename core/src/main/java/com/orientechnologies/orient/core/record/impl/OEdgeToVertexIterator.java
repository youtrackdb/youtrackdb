package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTVertex;
import java.util.Iterator;

/**
 *
 */
public class OEdgeToVertexIterator implements Iterator<YTVertex> {

  private final Iterator<YTEdge> edgeIterator;
  private final ODirection direction;

  public OEdgeToVertexIterator(Iterator<YTEdge> iterator, ODirection direction) {
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
  public YTVertex next() {
    YTEdge edge = edgeIterator.next();
    switch (direction) {
      case OUT:
        return edge.getTo();
      case IN:
        return edge.getFrom();
    }
    return null;
  }
}
