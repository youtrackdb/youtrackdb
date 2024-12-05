package com.jetbrains.youtrack.db.internal.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class ORawPairLongInteger {

  public final long first;
  public final int second;

  public ORawPairLongInteger(long first, int second) {
    this.first = first;
    this.second = second;
  }

  public long getFirst() {
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

    ORawPairLongInteger oRawPair = (ORawPairLongInteger) o;

    if (first != oRawPair.first) {
      return false;
    }
    return second == oRawPair.second;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(HashCommon.mix(first));
    result = 31 * result + HashCommon.mix(second);
    return result;
  }
}
