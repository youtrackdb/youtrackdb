package com.orientechnologies.core.db;

public interface ODatabaseTask<X> {

  X call(YTDatabaseSessionInternal session);
}
