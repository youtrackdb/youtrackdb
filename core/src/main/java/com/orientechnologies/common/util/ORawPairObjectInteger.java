package com.orientechnologies.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class ORawPairObjectInteger<V> {

  public final V first;
  public final int second;

  public ORawPairObjectInteger(V first, int second) {
    this.first = first;
    this.second = second;
  }

  public V getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ORawPairObjectInteger<?> oRawPair = (ORawPairObjectInteger<?>) o;

    if (!first.equals(oRawPair.first)) {
      return false;
    }
    return second == oRawPair.second;
  }

  @Override
  public int hashCode() {
    int result = first.hashCode();
    result = 31 * result + HashCommon.mix(second);
    return result;
  }
}
