package com.jetbrains.youtrack.db.internal.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class ORawPairIntegerBoolean {

  public final int first;
  public final boolean second;

  public ORawPairIntegerBoolean(int first, boolean second) {
    this.first = first;
    this.second = second;
  }

  public int getFirst() {
    return first;
  }

  public boolean getSecond() {
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

    ORawPairIntegerBoolean oRawPair = (ORawPairIntegerBoolean) o;

    if (first != oRawPair.first) {
      return false;
    }
    return second == oRawPair.second;
  }

  @Override
  public int hashCode() {
    int result = HashCommon.mix(first);
    result = 31 * result + (second ? 1 : 0);
    return result;
  }
}
