package com.orientechnologies.orient.test.database.security;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import java.io.File;
import org.junit.Test;

public class SystemUsersTest {

  @Test
  public void test() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    System.setProperty(
        "YOU_TRACK_DB_HOME",
        buildDirectory + File.separator + SystemUsersTest.class.getSimpleName());

    LogManager.instance()
        .info(this, "YOU_TRACK_DB_HOME: " + System.getProperty("YOU_TRACK_DB_HOME"));

    YouTrackDB orient =
        new YouTrackDB(
            "plocal:target/" + SystemUsersTest.class.getSimpleName(),
            YouTrackDBConfig.defaultConfig());

    try {
      orient.execute(
          "create database " + "test" + " memory users ( admin identified by 'admin' role admin)");

      orient.execute("create system user systemxx identified by systemxx role admin").close();
      YTDatabaseSession db = orient.open("test", "systemxx", "systemxx");

      db.close();
    } finally {
      orient.close();
    }
  }
}
