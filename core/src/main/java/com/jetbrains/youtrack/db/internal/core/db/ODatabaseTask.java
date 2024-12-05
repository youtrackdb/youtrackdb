package com.jetbrains.youtrack.db.internal.core.db;

public interface ODatabaseTask<X> {

  X call(YTDatabaseSessionInternal session);
}
