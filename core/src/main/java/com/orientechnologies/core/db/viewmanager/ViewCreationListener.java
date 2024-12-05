package com.orientechnologies.core.db.viewmanager;

import com.orientechnologies.core.db.YTDatabaseSession;

public interface ViewCreationListener {

  void afterCreate(YTDatabaseSession database, String viewName);

  void onError(String viewName, Exception exception);
}
