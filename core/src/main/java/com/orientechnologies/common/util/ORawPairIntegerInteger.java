package com.orientechnologies.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class ORawPairIntegerInteger {

  public final int first;
  public final int second;

  public ORawPairIntegerInteger(int first, int second) {
    this.first = first;
    this.second = second;
  }

  public int getFirst() {
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

    ORawPairIntegerInteger oRawPair = (ORawPairIntegerInteger) o;

    if (first != oRawPair.first) {
      return false;
    }
    return second == oRawPair.second;
  }

  @Override
  public int hashCode() {
    int result = HashCommon.mix(first);
    result = 31 * result + HashCommon.mix(second);
    return result;
  }
}
