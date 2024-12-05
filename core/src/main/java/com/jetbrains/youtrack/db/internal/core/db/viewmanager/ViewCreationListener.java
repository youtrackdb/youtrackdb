package com.jetbrains.youtrack.db.internal.core.db.viewmanager;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;

public interface ViewCreationListener {

  void afterCreate(YTDatabaseSession database, String viewName);

  void onError(String viewName, Exception exception);
}
