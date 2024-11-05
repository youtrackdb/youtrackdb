package com.orientechnologies.orient.core.db;

public interface ODatabaseTask<X> {
  X call(ODatabaseSessionInternal session);
}
