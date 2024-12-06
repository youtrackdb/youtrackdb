package com.jetbrains.youtrack.db.internal.core.db.viewmanager;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;

public interface ViewCreationListener {

  void afterCreate(DatabaseSession database, String viewName);

  void onError(String viewName, Exception exception);
}
