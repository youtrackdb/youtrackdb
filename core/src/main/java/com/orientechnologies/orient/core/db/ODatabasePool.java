package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.exception.OAcquireTimeoutException;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;

/**
 * A Pool of databases.
 *
 * <p>Example of usage with an OxygenDB context:
 *
 * <p>
 *
 * <pre>
 * <code>
 * OxygenDB oxygenDb= new OxygenDB("remote:localhost","root","password");
 * //...
 * ODatabasePool pool = new ODatabasePool(oxygenDb,"myDb","admin","adminpwd");
 * ODatabaseDocument session = pool.acquire();
 * //....
 * session.close();
 * pool.close();
 * oxygenDb.close();
 *
 * </code>
 * </pre>
 *
 * <p>
 *
 * <p>
 *
 * <p>Example of usage as simple access to a specific database without a context:
 *
 * <p>
 *
 * <pre><code>
 * ODatabasePool pool = new ODatabasePool("remote:localhost/myDb","admin","adminpwd");
 * ODatabaseDocument session = pool.acquire();
 * //....
 * session.close();
 * pool.close();
 *
 * </code></pre>
 *
 * <p>
 *
 * <p>
 */
public class ODatabasePool implements AutoCloseable {

  private final OxygenDB oxygenDb;
  private final ODatabasePoolInternal internal;
  private final boolean autoclose;

  /**
   * Open a new database pool on a specific environment.
   *
   * @param environment the starting environment.
   * @param database    the database name
   * @param user        the database user for the current pool of databases.
   * @param password    the password relative to the user name
   */
  public ODatabasePool(OxygenDB environment, String database, String user, String password) {
    this(environment, database, user, password, OxygenDBConfig.defaultConfig());
  }

  /**
   * Open a new database pool on a specific environment, with a specific configuration for this
   * pool.
   *
   * @param environment   the starting environment.
   * @param database      the database name
   * @param user          the database user for the current pool of databases.
   * @param password      the password relative to the user name
   * @param configuration the configuration relative for the current pool.
   */
  public ODatabasePool(
      OxygenDB environment,
      String database,
      String user,
      String password,
      OxygenDBConfig configuration) {
    oxygenDb = environment;
    autoclose = false;
    internal = oxygenDb.openPool(database, user, password, configuration);
  }

  /**
   * Open a new database pool from a url, useful in case the application access to only a database
   * or do not manipulate databases.
   *
   * @param url      the full url for a database, like "embedded:/full/path/to/database" or
   *                 "remote:localhost/database"
   * @param user     the database user for the current pool of databases.
   * @param password the password relative to the user
   */
  public ODatabasePool(String url, String user, String password) {
    this(url, user, password, OxygenDBConfig.defaultConfig());
  }

  /**
   * Open a new database pool from a url and additional configuration, useful in case the
   * application access to only a database or do not manipulate databases.
   *
   * @param url           the full url for a database, like "embedded:/full/path/to/database" or
   *                      "remote:localhost/database"
   * @param user          the database user for the current pool of databases.
   * @param password      the password relative to the user
   * @param configuration the configuration relative to the current pool.
   */
  public ODatabasePool(String url, String user, String password, OxygenDBConfig configuration) {
    OURLConnection val = OURLHelper.parseNew(url);
    oxygenDb = new OxygenDB(val.getType() + ":" + val.getPath(), configuration);
    autoclose = true;
    internal = oxygenDb.openPool(val.getDbName(), user, password, configuration);
  }

  /**
   * Open a new database pool from a environment and a database name, useful in case the application
   * access to only a database or do not manipulate databases.
   *
   * @param environment the url for an environemnt, like "embedded:/the/environment/path/" or
   *                    "remote:localhost"
   * @param database    the database for the current url.
   * @param user        the database user for the current pool of databases.
   * @param password    the password relative to the user
   */
  public ODatabasePool(String environment, String database, String user, String password) {
    this(environment, database, user, password, OxygenDBConfig.defaultConfig());
  }

  /**
   * Open a new database pool from a environment and a database name with a custom configuration,
   * useful in case the application access to only a database or do not manipulate databases.
   *
   * @param environment   the url for an environemnt, like "embedded:/the/environment/path/" or
   *                      "remote:localhost"
   * @param database      the database for the current url.
   * @param user          the database user for the current pool of databases.
   * @param password      the password relative to the user
   * @param configuration the configuration relative to the current pool.
   */
  public ODatabasePool(
      String environment,
      String database,
      String user,
      String password,
      OxygenDBConfig configuration) {
    oxygenDb = new OxygenDB(environment, configuration);
    autoclose = true;
    internal = oxygenDb.openPool(database, user, password, configuration);
  }

  ODatabasePool(OxygenDB environment, ODatabasePoolInternal internal) {
    this.oxygenDb = environment;
    this.internal = internal;
    autoclose = false;
  }

  /**
   * Acquire a session from the pool, if no session are available will wait until a session is
   * available or a timeout is reached
   *
   * @return a session from the pool.
   * @throws OAcquireTimeoutException in case the timeout for waiting for a session is reached.
   */
  public ODatabaseSession acquire() throws OAcquireTimeoutException {
    return internal.acquire();
  }

  @Override
  public void close() {
    internal.close();
    if (autoclose) {
      oxygenDb.close();
    }
  }

  /**
   * Check if database pool is closed
   *
   * @return true if database pool is closed
   */
  public boolean isClosed() {
    return internal.isClosed();
  }
}
