/*
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
 */
package com.orientechnologies.orient.core.db;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;

/**
 * OxygenDB management environment, it allows to connect to an environment and manipulate databases
 * or open sessions.
 *
 * <p>Usage examples: Remote Example:
 *
 * <pre>
 * <code>
 * try(OxygenDB oxygenDB = OxygenDB.remote("localhost","root","root") {
 *  oxygenDB.createIfNotExists("test",ODatabaseType.PLOCAL, "superuser", "password", "admin",
 *  "writer" , "password2", "writer");
 *  try(ODatabaseDocument session = oxygenDB.open("test","superuser","password")) {
 *     session.createClass("MyClass");
 *   }
 *  try(ODatabaseDocument session = oxygenDB.open("test","writer","password2")) {
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
 * try(OxygenDB oxygenDB = OxygenDB.embedded("./databases/")) {
 *  oxygenDB.createIfNotExists("test",ODatabaseType.PLOCAL, "superuser", "password", "admin",
 *  "writer" , "password2", "writer");
 *   try(ODatabaseDocument session = oxygenDB.open("test","superuser","password")) {
 *     session.createClass("MyClass");
 *   }
 *
 *   try(ODatabaseDocument session = oxygenDB.open("test","writer","password2")) {
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
 * tru(OxygenDB oxygenDB = ...) {
 *  if(!oxygenDB.exists("one")) {
 *     oxygenDB.create("one",ODatabaseType.PLOCAL, "superuser", "password", "admin", "writer,
 *     "password2", "writer");
 *  }
 *  if(oxygenDB.exists("two")) {
 *    oxygenDB.drop("two");
 *  }
 *  List<tString> databases = oxygenDB.list();
 *  assertEquals(databases.size(),1);
 *  assertEquals(databases.get("0"),"one");
 * }
 * </code>
 * </pre>
 *
 * <p>
 *
 * <p>
 */
public class OxygenDB implements AutoCloseable {

  private final ConcurrentLinkedHashMap<ODatabasePoolInternal, ODatabasePool> cachedPools =
      new ConcurrentLinkedHashMap.Builder<ODatabasePoolInternal, ODatabasePool>()
          .maximumWeightedCapacity(100)
          .build(); // cache for links to database pools. Avoid create database pool wrapper each
  // time when it is requested

  protected OxygenDBInternal internal;
  protected String serverUser;
  private String serverPassword;

  /**
   * Create a new OxygenDB instance for an embedded deployment with default configuration. For in
   * memory database use any directory name, for example "mydb"
   *
   * @param directoryPath the directory where the database are stored
   */
  public static OxygenDB embedded(String directoryPath) {
    return embedded(directoryPath, OxygenDBConfig.defaultConfig());
  }

  /**
   * Create a new OxygenDB instance for a embedded deployment with custom configuration. For in
   * memory database use any directory name, for example "mydb"
   *
   * @param directoryPath the directory where the database are stored
   * @param config        custom configuration for current environment
   */
  public static OxygenDB embedded(String directoryPath, OxygenDBConfig config) {
    return new OxygenDB(OxygenDBInternal.embedded(directoryPath, config));
  }

  /**
   * Create a new OxygenDB instance for a remote deployment with default configuration.
   *
   * @param url            the url for the database server for example "localhost" or
   *                       "localhost:2424"
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @return a new OxygenDB instance
   */
  public static OxygenDB remote(String url, String serverUser, String serverPassword) {
    return remote(url, serverUser, serverPassword, OxygenDBConfig.defaultConfig());
  }

  /**
   * Create a new OxygenDB instance for a remote deployment with custom configuration.
   *
   * @param url            the url for the database server for example "localhost" or
   *                       "localhost:2424"
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param config         custom configuration for current environment
   * @return a new OxygenDB instance
   */
  public static OxygenDB remote(
      String url, String serverUser, String serverPassword, OxygenDBConfig config) {
    var oxygenDB =
        new OxygenDB(
            OxygenDBInternal.remote(url.substring(url.indexOf(':') + 1).split("[,;]"), config));

    oxygenDB.serverUser = serverUser;
    oxygenDB.serverPassword = serverPassword;

    return oxygenDB;
  }

  /**
   * Create a new OxygenDB instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OxygenDB oxygenDB = new OxygenDB("remote:localhost");
   * ODatabaseDocument session = oxygenDB.open("test","admin","admin");
   * //...
   * session.close();
   * oxygenDB.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OxygenDB oxygenDB = new OxygenDB("embedded:./databases/");
   * ODatabaseDocument session = oxygenDB.open("test","admin","admin");
   * //...
   * session.close();
   * oxygenDB.close();
   * </code>
   * </pre>
   *
   * @param url           the url for the specific environment.
   * @param configuration configuration for the specific environment for the list of option
   *                      {@see OGlobalConfiguration}.
   * @see #embedded(String, OxygenDBConfig)
   * @see #remote(String, String, String, OxygenDBConfig)
   * @see #remote(String, String, String)
   * @see #embedded(String)
   */
  public OxygenDB(String url, OxygenDBConfig configuration) {
    this(url, null, null, configuration);
  }

  /**
   * Create a new OxygenDB instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OxygenDB oxygenDB = new OxygenDB("remote:localhost","root","root");
   * oxygenDB.create("test",ODatabaseType.PLOCAL);
   * ODatabaseDocument session = oxygenDB.open("test","admin","admin");
   * //...
   * session.close();
   * oxygenDB.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OxygenDB oxygenDB = new OxygenDB("embedded:./databases/",null,null);
   * oxygenDB.create("test",ODatabaseType.MEMORY);
   * ODatabaseDocument session = oxygenDB.open("test","admin","admin");
   * //...
   * session.close();
   * oxygenDB.close();
   * </code>
   * </pre>
   *
   * @param url            the url for the specific environment.
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param configuration  configuration for the specific environment for the list of option
   *                       {@see OGlobalConfiguration}.
   * @see #embedded(String, OxygenDBConfig)
   * @see #remote(String, String, String, OxygenDBConfig)
   * @see #remote(String, String, String)
   * @see #embedded(String)
   */
  public OxygenDB(
      String url, String serverUser, String serverPassword, OxygenDBConfig configuration) {
    int pos;
    String what;
    if ((pos = url.indexOf(':')) > 0) {
      what = url.substring(0, pos);
    } else {
      what = url;
    }
    if ("embedded".equals(what) || "memory".equals(what) || "plocal".equals(what)) {
      internal = OxygenDBInternal.embedded(url.substring(url.indexOf(':') + 1), configuration);
    } else if ("remote".equals(what)) {
      internal =
          OxygenDBInternal.remote(url.substring(url.indexOf(':') + 1).split("[,;]"), configuration);
    } else {
      throw new IllegalArgumentException("Wrong url:`" + url + "`");
    }

    this.serverUser = serverUser;
    this.serverPassword = serverPassword;
  }

  OxygenDB(OxygenDBInternal internal) {
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
    return open(database, user, password, OxygenDBConfig.defaultConfig());
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
      String database, String user, String password, OxygenDBConfig config) {
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
    create(database, type, OxygenDBConfig.defaultConfig());
  }

  /**
   * Creates a new database alongside with users, passwords and roles.
   *
   * <p>If you want to create users during creation of database you should provide array that
   * consist of triple strings. Each triple string should contain user name, password and role.
   *
   * <p>For example:
   *
   * <p>{@code oxygenDB.create("test", ODatabaseType.PLOCAL, "user1", "password1", "admin",
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
  public void create(String database, ODatabaseType type, OxygenDBConfig config) {
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
    return createIfNotExists(database, type, OxygenDBConfig.defaultConfig());
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
   * <p>{@code oxygenDB.createIfNotExists("test", ODatabaseType.PLOCAL, "user1", "password1",
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
  public boolean createIfNotExists(String database, ODatabaseType type, OxygenDBConfig config) {
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
   * Close the current OxygenDB context with all related databases and pools.
   */
  @Override
  public void close() {
    this.cachedPools.clear();
    this.internal.close();
  }

  /**
   * Check if the current OxygenDB context is open
   *
   * @return boolean true if is open false otherwise.
   */
  public boolean isOpen() {
    return this.internal.isOpen();
  }

  ODatabasePoolInternal openPool(
      String database, String user, String password, OxygenDBConfig config) {
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
   * @param config   OxygenDB config for pool if need create it (in case if there is no cached
   *                 pool)
   * @return cached {@link ODatabasePool}
   */
  public ODatabasePool cachedPool(
      String database, String user, String password, OxygenDBConfig config) {
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
    return internal.executeServerStatement(script, null, serverUser, serverPassword, params);
  }

  public OResultSet execute(String script, Object... params) {
    return internal.executeServerStatement(script, null, serverUser, serverPassword, params);
  }

  OxygenDBInternal getInternal() {
    return internal;
  }
}
