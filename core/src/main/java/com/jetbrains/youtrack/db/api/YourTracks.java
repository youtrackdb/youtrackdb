package com.jetbrains.youtrack.db.api;


import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;

public abstract class YourTracks {

  /**
   * Create a new YouTrackDB manager instance for an embedded deployment with default configuration.
   * For in memory database use any directory name, for example "mydb"
   *
   * @param directoryPath the directory where the database are stored
   */
  public static YouTrackDB embedded(String directoryPath) {
    return embedded(directoryPath, YouTrackDBConfig.defaultConfig());
  }

  /**
   * Create a new YouTrackDB manager instance for a embedded deployment with custom configuration.
   * For in memory database use any directory name, for example "mydb"
   *
   * @param directoryPath the directory where the database are stored
   * @param config        custom configuration for current environment
   */
  public static YouTrackDB embedded(String directoryPath, YouTrackDBConfig config) {
    return new YouTrackDBImpl(YouTrackDBInternal.embedded(directoryPath, config));
  }

  /**
   * Create a new YouTrackDB manager instance for a remote deployment with default configuration.
   *
   * @param url            the url for the database server for example "localhost" or
   *                       "localhost:2424"
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @return a new YouTrackDB instance
   */
  public static YouTrackDB remote(String url, String serverUser, String serverPassword) {
    return remote(url, serverUser, serverPassword, YouTrackDBConfig.defaultConfig());
  }

  /**
   * Create a new YouTrackDB manager instance for a remote deployment with custom configuration.
   *
   * @param url            the url for the database server for example "localhost" or
   *                       "localhost:2424"
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param config         custom configuration for current environment
   * @return a new YouTrackDB instance
   */
  public static YouTrackDB remote(
      String url, String serverUser, String serverPassword, YouTrackDBConfig config) {
    var youTrackDB =
        new YouTrackDBImpl(
            YouTrackDBInternal.remote(url.substring(url.indexOf(':') + 1).split("[,;]"),
                (YouTrackDBConfigImpl) config));

    youTrackDB.serverUser = serverUser;
    youTrackDB.serverPassword = serverPassword;

    return youTrackDB;
  }
}
