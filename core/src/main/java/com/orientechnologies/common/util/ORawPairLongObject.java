package com.orientechnologies.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class ORawPairLongObject<V> {

  public final long first;
  public final V second;

  public ORawPairLongObject(long first, V second) {
    this.first = first;
    this.second = second;
  }

  public long getFirst() {
    return first;
  }

  public V getSecond() {
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

    ORawPairIntegerObject<?> oRawPair = (ORawPairIntegerObject<?>) o;

    if (first != oRawPair.first) {
      return false;
    }
    return second.equals(oRawPair.second);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(HashCommon.mix(first));
    result = 31 * result + second.hashCode();
    return result;
  }
}
