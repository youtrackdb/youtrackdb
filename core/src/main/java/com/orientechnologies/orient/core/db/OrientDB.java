/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;

/**
 * OrientDB management environment, it allows to connect to an environment and manipulate databases
 * or open sessions.
 *
 * <p>Usage examples: Remote Example:
 *
 * <pre>
 * <code>
 * try(OrientDB orientDb = OrientDB.remote("localhost","root","root") {
 *  orientDb.createIfNotExists("test",ODatabaseType.PLOCAL, "superuser", "password", "admin",
 *  "writer" , "password2", "writer");
 *  try(ODatabaseDocument session = orientDb.open("test","superuser","password")) {
 *     session.createClass("MyClass");
 *   }
 *  try(ODatabaseDocument session = orientDb.open("test","writer","password2")) {
 *     //...
 *  }
 * }
 * </code>
 * </pre>
 * <p>
 * Embedded example:
 *
 * <pre>
 * <code>
 * try(OrientDB orientDb = OrientDB.embedded("./databases/")) {
 *  orientDb.createIfNotExists("test",ODatabaseType.PLOCAL, "superuser", "password", "admin",
 *  "writer" , "password2", "writer");
 *   try(ODatabaseDocument session = orientDb.open("test","superuser","password")) {
 *     session.createClass("MyClass");
 *   }
 *
 *   try(ODatabaseDocument session = orientDb.open("test","writer","password2")) {
 *     //...
 *   }
 * }
 * </code>
 * </pre>
 * <p>
 * Database Manipulation Example:
 *
 * <pre>
 * <code>
 * tru(OrientDB orientDb = ...) {
 *  if(!orientDb.exists("one")) {
 *     orientDb.create("one",ODatabaseType.PLOCAL, "superuser", "password", "admin", "writer,
 *     "password2", "writer");
 *  }
 *  if(orientDb.exists("two")) {
 *    orientDb.drop("two");
 *  }
 *  List<tString> databases = orientDb.list();
 *  assertEquals(databases.size(),1);
 *  assertEquals(databases.get("0"),"one");
 * }
 * </code>
 * </pre>
 *
 * <p>
 *
 * <p>Created by tglman on 08/02/17.
 */
public class OrientDB implements AutoCloseable {

  private final ConcurrentLinkedHashMap<ODatabasePoolInternal, ODatabasePool> cachedPools =
      new ConcurrentLinkedHashMap.Builder<ODatabasePoolInternal, ODatabasePool>()
          .maximumWeightedCapacity(100)
          .build(); // cache for links to database pools. Avoid create database pool wrapper each
  // time when it is requested

  protected OrientDBInternal internal;
  protected String serverUser;
  private String serverPassword;

  /**
   * Create a new OrientDb instance for an embedded deployment with default configuration. For in
   * memory database use any directory name, for example "mydb"
   *
   * @param directoryPath the directory where the database are stored
   */
  public static OrientDB embedded(String directoryPath) {
    return embedded(directoryPath, OrientDBConfig.defaultConfig());
  }

  /**
   * Create a new OrientDb instance for a embedded deployment with custom configuration. For in
   * memory database use any directory name, for example "mydb"
   *
   * @param directoryPath the directory where the database are stored
   * @param config        custom configuration for current environment
   */
  public static OrientDB embedded(String directoryPath, OrientDBConfig config) {
    return new OrientDB(OrientDBInternal.embedded(directoryPath, config));
  }

  /**
   * Create a new OrientDb instance for a remote deployment with default configuration.
   *
   * @param url            the url for the database server for example "localhost" or
   *                       "localhost:2424"
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @return a new OrientDB instance
   */
  public static OrientDB remote(String url, String serverUser, String serverPassword) {
    return remote(url, serverUser, serverPassword, OrientDBConfig.defaultConfig());
  }

  /**
   * Create a new OrientDb instance for a remote deployment with custom configuration.
   *
   * @param url            the url for the database server for example "localhost" or
   *                       "localhost:2424"
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param config         custom configuration for current environment
   * @return a new OrientDB instance
   */
  public static OrientDB remote(
      String url, String serverUser, String serverPassword, OrientDBConfig config) {
    var orientdb =
        new OrientDB(
            OrientDBInternal.remote(url.substring(url.indexOf(':') + 1).split("[,;]"), config));

    orientdb.serverUser = serverUser;
    orientdb.serverPassword = serverPassword;

    return orientdb;
  }

  /**
   * Create a new OrientDb instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("remote:localhost");
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("embedded:./databases/");
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * @param url           the url for the specific environment.
   * @param configuration configuration for the specific environment for the list of option
   *                      {@see OGlobalConfiguration}.
   * @see #embedded(String, OrientDBConfig)
   * @see #remote(String, String, String, OrientDBConfig)
   * @see #remote(String, String, String)
   * @see #embedded(String)
   */
  public OrientDB(String url, OrientDBConfig configuration) {
    this(url, null, null, configuration);
  }

  /**
   * Create a new OrientDb instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("remote:localhost","root","root");
   * orientDb.create("test",ODatabaseType.PLOCAL);
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("embedded:./databases/",null,null);
   * orientDb.create("test",ODatabaseType.MEMORY);
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * @param url            the url for the specific environment.
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param configuration  configuration for the specific environment for the list of option
   *                       {@see OGlobalConfiguration}.
   * @see #embedded(String, OrientDBConfig)
   * @see #remote(String, String, String, OrientDBConfig)
   * @see #remote(String, String, String)
   * @see #embedded(String)
   */
  public OrientDB(
      String url, String serverUser, String serverPassword, OrientDBConfig configuration) {
    int pos;
    String what;
    if ((pos = url.indexOf(':')) > 0) {
      what = url.substring(0, pos);
    } else {
      what = url;
    }
    if ("embedded".equals(what) || "memory".equals(what) || "plocal".equals(what)) {
      internal = OrientDBInternal.embedded(url.substring(url.indexOf(':') + 1), configuration);
    } else if ("remote".equals(what)) {
      internal =
          OrientDBInternal.remote(url.substring(url.indexOf(':') + 1).split("[,;]"), configuration);
    } else {
      throw new IllegalArgumentException("Wrong url:`" + url + "`");
    }

    this.serverUser = serverUser;
    this.serverPassword = serverPassword;
  }

  OrientDB(OrientDBInternal internal) {
    this.internal = internal;
    this.serverUser = null;
    this.serverPassword = null;
  }

  /**
   * Open a database
   *
   * @param database the database to open
   * @param user     username of a database user or a server user allowed to open the database
   * @param password related to the specified username
   * @return the opened database
   */
  public ODatabaseSession open(String database, String user, String password) {
    return open(database, user, password, OrientDBConfig.defaultConfig());
  }

  /**
   * Open a database
   *
   * @param database the database to open
   * @param user     username of a database user or a server user allowed to open the database
   * @param password related to the specified username
   * @param config   custom configuration for current database
   * @return the opened database
   */
  public ODatabaseSession open(
      String database, String user, String password, OrientDBConfig config) {
    return internal.open(database, user, password, config);
  }

  /**
   * Create a new database without users. In case if you want to create users during creation please
   * use {@link #create(String, ODatabaseType, String...)}
   *
   * @param database database name
   * @param type     can be plocal or memory
   * @see #create(String, ODatabaseType, String...)
   */
  public void create(String database, ODatabaseType type) {
    create(database, type, OrientDBConfig.defaultConfig());
  }

  /**
   * Creates a new database alongside with users, passwords and roles.
   *
   * <p>If you want to create users during creation of database you should provide array that
   * consist of triple strings. Each triple string should contain user name, password and role.
   *
   * <p>For example:
   *
   * <p>{@code orientDB.create("test", ODatabaseType.PLOCAL, "user1", "password1", "admin",
   * "user2", "password2", "reader"); }
   *
   * <p>The predefined roles are:
   *
   * <ul>
   *   <li>admin: has all privileges on the database
   *   <li>reader: can read the data but cannot modify it
   *   <li>writer: can read and modify the data but cannot create or delete classes
   * </ul>
   *
   * @param database        database name
   * @param type            can be plocal or memory
   * @param userCredentials user names, passwords and roles provided as a sequence of triple
   *                        strings
   */
  public void create(String database, ODatabaseType type, String... userCredentials) {
    StringBuilder queryString = new StringBuilder("create database ? " + type.name());
    var params = addUsersToCreationScript(userCredentials, queryString);
    execute(queryString.toString(), ArrayUtils.add(params, 0, database)).close();
  }

  /**
   * Creates a new database without users. In case if you want to create users during creation
   * please use {@link #create(String, ODatabaseType, String...)}
   *
   * @param database database name
   * @param type     can be plocal or memory
   * @param config   custom configuration for current database
   */
  public void create(String database, ODatabaseType type, OrientDBConfig config) {
    this.internal.create(database, serverUser, serverPassword, type, config);
  }

  /**
   * Create a new database without users if it does not exist. In case if you want to create users
   * during creation please use {@link #createIfNotExists(String, ODatabaseType, String...)}
   *
   * @param database database name
   * @param type     can be plocal or memory
   * @return true if the database has been created, false if already exists
   */
  public boolean createIfNotExists(String database, ODatabaseType type) {
    return createIfNotExists(database, type, OrientDBConfig.defaultConfig());
  }

  /**
   * Creates a new database alongside with users, passwords and roles if such one does not exist
   * yet.
   *
   * <p>If you want to create users during creation of database you should provide array that
   * consist of triple strings. Each triple string should contain user name, password and role.
   *
   * <p>The predefined roles are:
   *
   * <ul>
   *   <li>admin: has all privileges on the database
   *   <li>reader: can read the data but cannot modify it
   *   <li>writer: can read and modify the data but cannot create or delete classes
   * </ul>
   *
   * <p>For example:
   *
   * <p>{@code orientDB.createIfNotExists("test", ODatabaseType.PLOCAL, "user1", "password1",
   * "admin", "user2", "password2", "reader"); }
   *
   * @param database        database name
   * @param type            can be plocal or memory
   * @param userCredentials user names, passwords and roles provided as a sequence of triple
   *                        strings
   */
  public void createIfNotExists(String database, ODatabaseType type, String... userCredentials) {
    StringBuilder queryString =
        new StringBuilder("create database ? " + type.name() + " if not exists");
    var params = addUsersToCreationScript(userCredentials, queryString);
    execute(queryString.toString(), ArrayUtils.add(params, 0, database)).close();
  }

  private static String[] addUsersToCreationScript(
      String[] userCredentials, StringBuilder queryString) {
    if (userCredentials != null && userCredentials.length > 0) {
      if (userCredentials.length % 3 != 0) {
        throw new IllegalArgumentException(
            "User credentials should be provided as a sequence of triple strings");
      }

      queryString.append(" users (");

      var result = new String[2 * userCredentials.length / 3];
      for (int i = 0; i < userCredentials.length / 3; i++) {
        if (i > 0) {
          queryString.append(", ");
        }
        queryString.append("? identified by ? role ").append(userCredentials[i * 3 + 2]);

        result[i * 2] = userCredentials[i * 3];
        result[i * 2 + 1] = userCredentials[i * 3 + 1];
      }

      queryString.append(")");

      return result;
    }

    return new String[0];
  }

  /**
   * Create a new database without users if not exists. In case if you want to create users during
   * creation please use {@link #createIfNotExists(String, ODatabaseType, String...)}
   *
   * @param database database name
   * @param type     can be plocal or memory
   * @param config   custom configuration for current database
   * @return true if the database has been created, false if already exists
   */
  public boolean createIfNotExists(String database, ODatabaseType type, OrientDBConfig config) {
    if (!this.internal.exists(database, serverUser, serverPassword)) {
      this.internal.create(database, serverUser, serverPassword, type, config);
      return true;
    }
    return false;
  }

  /**
   * Drop a database
   *
   * @param database database name
   */
  public void drop(String database) {
    this.internal.drop(database, serverUser, serverPassword);
  }

  /**
   * Check if a database exists
   *
   * @param database database name to check
   * @return boolean true if exist false otherwise.
   */
  public boolean exists(String database) {
    return this.internal.exists(database, serverUser, serverPassword);
  }

  /**
   * List exiting databases in the current environment
   *
   * @return a list of existing databases.
   */
  public List<String> list() {
    return new ArrayList<>(this.internal.listDatabases(serverUser, serverPassword));
  }

  /**
   * Close the current OrientDB context with all related databases and pools.
   */
  @Override
  public void close() {
    this.cachedPools.clear();
    this.internal.close();
  }

  /**
   * Check if the current OrientDB context is open
   *
   * @return boolean true if is open false otherwise.
   */
  public boolean isOpen() {
    return this.internal.isOpen();
  }

  ODatabasePoolInternal openPool(
      String database, String user, String password, OrientDBConfig config) {
    return this.internal.openPool(database, user, password, config);
  }

  public ODatabasePool cachedPool(String database, String user, String password) {
    return cachedPool(database, user, password, null);
  }

  /**
   * Retrieve cached database pool with given username and password
   *
   * @param database database name
   * @param user     user name
   * @param password user password
   * @param config   OrientDB config for pool if need create it (in case if there is no cached
   *                 pool)
   * @return cached {@link ODatabasePool}
   */
  public ODatabasePool cachedPool(
      String database, String user, String password, OrientDBConfig config) {
    ODatabasePoolInternal internalPool = internal.cachedPool(database, user, password, config);

    ODatabasePool pool = cachedPools.get(internalPool);

    if (pool != null) {
      return pool;
    }

    return cachedPools.computeIfAbsent(internalPool, key -> new ODatabasePool(this, internalPool));
  }

  public void invalidateCachedPools() {
    synchronized (this) {
      cachedPools.forEach((internalPool, pool) -> pool.close());
      cachedPools.clear();
    }
  }

  public OResultSet execute(String script, Map<String, Object> params) {
    return internal.executeServerStatement(script, serverUser, serverPassword, params);
  }

  public OResultSet execute(String script, Object... params) {
    return internal.executeServerStatement(script, serverUser, serverPassword, params);
  }

  OrientDBInternal getInternal() {
    return internal;
  }
}
