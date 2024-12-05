package com.orientechnologies.core;

import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.ODatabaseType;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.YouTrackDBConfig;

/**
 * Used as part of the security test refactoring of the ODB `core` module, cf.
 * https://gist.github.com/tglman/4a24fa59efd88415e765a78487d64366#file-test-migrations-md
 */
public class OCreateDatabaseUtil {

  public static final String NEW_ADMIN_PASSWORD = "adminpwd";

  public static final String TYPE_PLOCAL = ODatabaseType.PLOCAL.name().toLowerCase(); // "plocal";
  public static final String TYPE_MEMORY = ODatabaseType.MEMORY.name().toLowerCase(); // "memory";

  public static YouTrackDB createDatabase(
      final String database, final String url, final String type) {
    final YouTrackDB youTrackDB =
        new YouTrackDB(
            url,
            YouTrackDBConfig.builder()
                .addConfig(YTGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!youTrackDB.exists(database)) {
      youTrackDB.execute(
          "create database "
              + database
              + " "
              + type
              + " users ( admin identified by '"
              + NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    return youTrackDB;
  }

  public static void createDatabase(
      final String database, final YouTrackDB youTrackDB, final String type) {
    if (!youTrackDB.exists(database)) {
      youTrackDB.execute(
          "create database "
              + database
              + " "
              + type
              + " users ( admin identified by '"
              + NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
  }
}
