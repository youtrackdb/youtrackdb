package com.orientechnologies.common.util;

import it.unimi.dsi.fastutil.HashCommon;

public final class OPairIntegerObject<V> implements Comparable<OPairIntegerObject<V>> {

  public final int key;
  public final V value;

  public OPairIntegerObject(int key, V value) {
    this.key = key;
    this.value = value;
  }

  public int getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public int compareTo(OPairIntegerObject<V> o) {
    return Integer.compare(key, o.key);
  }

  @Override
  public String toString() {
    return "OPairIntegerObject [first=" + key + ", second=" + value + "]";
  }

  @Override
  public int hashCode() {
    return HashCommon.mix(key);
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
    OPairIntegerObject<?> other = (OPairIntegerObject<?>) obj;
    return key == other.key;
  }
}
