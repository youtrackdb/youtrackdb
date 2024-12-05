package com.jetbrains.youtrack.db.internal.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class OPairLongObject<V> implements Comparable<OPairLongObject<V>> {

  public final long key;
  public final V value;

  public OPairLongObject(long key, V value) {
    this.key = key;
    this.value = value;
  }

  public long getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public int compareTo(OPairLongObject<V> o) {
    return Long.compare(key, o.key);
  }

  @Override
  public String toString() {
    return "OPairLongObject [first=" + key + ", second=" + value + "]";
  }

  @Override
  public int hashCode() {
    return Long.hashCode(HashCommon.mix(key));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    OPairLongObject<?> other = (OPairLongObject<?>) obj;
    return key == other.key;
  }
}
