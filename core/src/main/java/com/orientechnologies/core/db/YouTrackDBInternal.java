/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

package com.orientechnologies.core.db;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.command.OCommandOutputListener;
import com.orientechnologies.core.command.script.OScriptManager;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.core.security.OSecuritySystem;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.storage.OStorage;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 *
 */
public interface YouTrackDBInternal extends AutoCloseable, OSchedulerInternal {

  /**
   * Create a new factory from a given url.
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * @param url           the url for the specific factory.
   * @param configuration configuration for the specific factory for the list of option
   *                      {@see YTGlobalConfiguration}.
   * @return the new Orient Factory.
   */
  static YouTrackDBInternal fromUrl(String url, YouTrackDBConfig configuration) {
    String what = url.substring(0, url.indexOf(':'));
    if ("embedded".equals(what)) {
      return embedded(url.substring(url.indexOf(':') + 1), configuration);
    } else if ("remote".equals(what)) {
      return remote(url.substring(url.indexOf(':') + 1).split(";"), configuration);
    }
    throw new YTDatabaseException("not supported database type");
  }

  default YouTrackDB newOrientDB() {
    return new YouTrackDB(this);
  }

  default YouTrackDB newOrientDBNoClose() {
    return new YouTrackDB(this) {
      @Override
      public void close() {
      }
    };
  }

  /**
   * Create a new remote factory
   *
   * @param hosts         array of hosts
   * @param configuration configuration for the specific factory for the list of option
   *                      {@see YTGlobalConfiguration}.
   * @return a new remote databases factory
   */
  static YouTrackDBInternal remote(String[] hosts, YouTrackDBConfig configuration) {
    YouTrackDBInternal factory;

    try {
      String className = "com.orientechnologies.orient.client.remote.YouTrackDBRemote";
      ClassLoader loader;
      if (configuration != null) {
        loader = configuration.getClassLoader();
      } else {
        loader = YouTrackDBInternal.class.getClassLoader();
      }
      Class<?> kass = loader.loadClass(className);
      Constructor<?> constructor =
          kass.getConstructor(String[].class, YouTrackDBConfig.class, YouTrackDBManager.class);
      factory = (YouTrackDBInternal) constructor.newInstance(hosts, configuration,
          YouTrackDBManager.instance());
    } catch (ClassNotFoundException
             | NoSuchMethodException
             | IllegalAccessException
             | InstantiationException e) {
      throw YTException.wrapException(new YTDatabaseException("YouTrackDB client API missing"), e);
    } catch (InvocationTargetException e) {
      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw YTException.wrapException(
          new YTDatabaseException("Error creating YouTrackDB remote factory"),
          e.getTargetException());
    }
    return factory;
  }

  /**
   * Create a new Embedded factory
   *
   * @param directoryPath base path where the database are hosted
   * @param config        configuration for the specific factory for the list of option
   *                      {@see YTGlobalConfiguration}
   * @return a new embedded databases factory
   */
  static YouTrackDBInternal embedded(String directoryPath, YouTrackDBConfig config) {
    return new YouTrackDBEmbedded(directoryPath, config, YouTrackDBManager.instance());
  }

  static YouTrackDBInternal distributed(String directoryPath, YouTrackDBConfig configuration) {
    YouTrackDBInternal factory;

    try {
      ClassLoader loader;
      if (configuration != null) {
        loader = configuration.getClassLoader();
      } else {
        loader = YouTrackDBInternal.class.getClassLoader();
      }
      Class<?> kass;
      try {
        String className = "com.orientechnologies.orient.distributed.db.OrientDBDistributed";
        kass = loader.loadClass(className);
      } catch (ClassNotFoundException e) {
        String className = "com.orientechnologies.orient.distributed.OrientDBDistributed";
        kass = loader.loadClass(className);
      }
      Constructor<?> constructor =
          kass.getConstructor(String.class, YouTrackDBConfig.class, YouTrackDBManager.class);
      factory =
          (YouTrackDBInternal)
              constructor.newInstance(directoryPath, configuration, YouTrackDBManager.instance());
    } catch (ClassNotFoundException
             | NoSuchMethodException
             | IllegalAccessException
             | InstantiationException e) {
      throw YTException.wrapException(new YTDatabaseException("YouTrackDB distributed API missing"),
          e);
    } catch (InvocationTargetException e) {
      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw YTException.wrapException(
          new YTDatabaseException("Error creating YouTrackDB remote factory"),
          e.getTargetException());
    }
    return factory;
  }

  /**
   * Open a database specified by name using the username and password if needed
   *
   * @param name     of the database to open
   * @param user     the username allowed to open the database
   * @param password related to the specified username
   * @return the opened database
   */
  YTDatabaseSessionInternal open(String name, String user, String password);

  /**
   * Open a database specified by name using the username and password if needed, with specific
   * configuration
   *
   * @param name     of the database to open
   * @param user     the username allowed to open the database
   * @param password related to the specified username
   * @param config   database specific configuration that override the factory global settings where
   *                 needed.
   * @return the opened database
   */
  YTDatabaseSessionInternal open(String name, String user, String password,
      YouTrackDBConfig config);

  /**
   * Open a database specified by name using the authentication info provided, with specific
   * configuration
   *
   * @param authenticationInfo authentication informations provided for the authentication.
   * @param config             database specific configuration that override the factory global
   *                           settings where needed.
   * @return the opened database
   */
  YTDatabaseSessionInternal open(OAuthenticationInfo authenticationInfo, YouTrackDBConfig config);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user     the username of a user allowed to create a database, in case of remote is a
   *                 server user for embedded it can be left empty
   * @param password the password relative to the user
   * @param type     can be plocal or memory
   */
  void create(String name, String user, String password, ODatabaseType type);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user     the username of a user allowed to create a database, in case of remote is a
   *                 server user for embedded it can be left empty
   * @param password the password relative to the user
   * @param config   database specific configuration that override the factory global settings where
   *                 needed.
   * @param type     can be plocal or memory
   */
  void create(String name, String user, String password, ODatabaseType type,
      YouTrackDBConfig config);

  /**
   * Check if a database exists
   *
   * @param name     database name to check
   * @param user     the username of a user allowed to check the database existence, in case of
   *                 remote is a server user for embedded it can be left empty.
   * @param password the password relative to the user
   * @return boolean true if exist false otherwise.
   */
  boolean exists(String name, String user, String password);

  /**
   * Drop a database
   *
   * @param name     database name
   * @param user     the username of a user allowed to drop a database, in case of remote is a
   *                 server user for embedded it can be left empty
   * @param password the password relative to the user
   */
  void drop(String name, String user, String password);

  /**
   * List of database exiting in the current environment
   *
   * @param user     the username of a user allowed to list databases, in case of remote is a server
   *                 user for embedded it can be left empty
   * @param password the password relative to the user
   * @return a set of databases names.
   */
  Set<String> listDatabases(String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name     database name
   * @param user     the username allowed to open the database
   * @param password the password relative to the user
   * @return a new pool of databases.
   */
  ODatabasePoolInternal openPool(String name, String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name     database name
   * @param user     the username allowed to open the database
   * @param password the password relative to the user
   * @param config   database specific configuration that override the factory global settings where
   *                 needed.
   * @return a new pool of databases.
   */
  ODatabasePoolInternal openPool(String name, String user, String password,
      YouTrackDBConfig config);

  ODatabasePoolInternal cachedPool(String database, String user, String password);

  ODatabasePoolInternal cachedPool(
      String database, String user, String password, YouTrackDBConfig config);

  /**
   * Internal api for request to open a database with a pool
   */
  YTDatabaseSessionInternal poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool);

  void restore(
      String name,
      String user,
      String password,
      ODatabaseType type,
      String path,
      YouTrackDBConfig config);

  void restore(
      String name,
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener);

  /**
   * Close the factory with all related databases and pools.
   */
  void close();

  /**
   * Should be called only by shutdown listeners
   */
  void internalClose();

  /**
   * Internal API for pool close
   */
  void removePool(ODatabasePoolInternal toRemove);

  /**
   * Check if the current instance is open
   */
  boolean isOpen();

  boolean isEmbedded();

  default boolean isMemoryOnly() {
    return false;
  }

  static YouTrackDBInternal extract(YouTrackDB youTrackDB) {
    return youTrackDB.internal;
  }

  static String extractUser(YouTrackDB youTrackDB) {
    return youTrackDB.serverUser;
  }

  YTDatabaseSessionInternal openNoAuthenticate(String iDbUrl, String user);

  YTDatabaseSessionInternal openNoAuthorization(String name);

  void initCustomStorage(String name, String baseUrl, String userName, String userPassword);

  void loadAllDatabases();

  void removeShutdownHook();

  Collection<OStorage> getStorages();

  void forceDatabaseClose(String databaseName);

  Future<?> execute(Runnable task);

  <X> Future<X> execute(Callable<X> task);

  <X> Future<X> execute(String database, String user, ODatabaseTask<X> task);

  <X> Future<X> executeNoAuthorizationAsync(String database, ODatabaseTask<X> task);

  <X> X executeNoAuthorizationSync(YTDatabaseSessionInternal database, ODatabaseTask<X> task);

  default OStorage fullSync(String dbName, InputStream backupStream, YouTrackDBConfig config) {
    throw new UnsupportedOperationException();
  }

  default void deltaSync(String dbName, InputStream backupStream, YouTrackDBConfig config) {
    throw new UnsupportedOperationException();
  }

  default OScriptManager getScriptManager() {
    throw new UnsupportedOperationException();
  }

  default void networkRestore(String databaseName, InputStream in, Callable<Object> callback) {
    throw new UnsupportedOperationException();
  }

  default YTResultSet executeServerStatementNamedParams(String script, String user, String pw,
      Map<String, Object> params) {
    throw new UnsupportedOperationException();
  }

  default YTResultSet executeServerStatementPositionalParams(String script, String user, String pw,
      Object... params) {
    throw new UnsupportedOperationException();
  }

  default OSystemDatabase getSystemDatabase() {
    throw new UnsupportedOperationException();
  }

  default String getBasePath() {
    throw new UnsupportedOperationException();
  }

  void internalDrop(String database);

  default void distributedSetOnline(String database) {
  }

  void create(
      String name,
      String user,
      String password,
      ODatabaseType type,
      YouTrackDBConfig config,
      ODatabaseTask<Void> createOps);

  YouTrackDBConfig getConfigurations();

  OSecuritySystem getSecuritySystem();

  default Set<String> listLodadedDatabases() {
    throw new UnsupportedOperationException();
  }

  String getConnectionUrl();

  default void startCommand(Optional<Long> timeout) {
  }

  default void endCommand() {
  }

  static YouTrackDBInternal getInternal(YouTrackDB youTrackDB) {
    return youTrackDB.internal;
  }
}
