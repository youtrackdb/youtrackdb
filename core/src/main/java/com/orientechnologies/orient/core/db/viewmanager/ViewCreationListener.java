package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.orient.core.db.YTDatabaseSession;

public interface ViewCreationListener {

  void afterCreate(YTDatabaseSession database, String viewName);

  void onError(String viewName, Exception exception);
}
