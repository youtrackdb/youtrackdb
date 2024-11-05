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

import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.util.OBackupable;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.Closeable;
import java.util.Map;

/**
 * Generic Database interface. Represents the lower level of the Database providing raw API to
 * access to the raw records.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabase<T> extends OBackupable, Closeable {
  enum STATUS {
    OPEN,
    CLOSED,
    IMPORTING
  }

  enum ATTRIBUTES {
    TYPE,
    STATUS,
    DEFAULTCLUSTERID,
    DATEFORMAT,
    DATETIMEFORMAT,
    TIMEZONE,
    LOCALECOUNTRY,
    LOCALELANGUAGE,
    CHARSET,
    CUSTOM,
    CLUSTERSELECTION,
    MINIMUMCLUSTERS,
    CONFLICTSTRATEGY,
    VALIDATION
  }

  /**
   * Activate current database instance on current thread. Call this method before using the
   * database if you switch between multiple databases instances on the same thread or if you pass
   * them across threads.
   */
  ODatabase activateOnCurrentThread();

  /**
   * Returns true if the current database instance is active on current thread, otherwise false.
   */
  boolean isActiveOnCurrentThread();

  /**
   * Returns the database configuration settings. If defined, any database configuration overwrites
   * the global one.
   *
   * @return OContextConfiguration
   */
  OContextConfiguration getConfiguration();

  /**
   * Closes an opened database, if the database is already closed does nothing, if a transaction is
   * active will be rollback.
   */
  void close();

  /**
   * Returns the current status of database.
   */
  STATUS getStatus();

  /**
   * Returns the database name.
   *
   * @return Name of the database
   */
  String getName();

  /**
   * Returns the database URL.
   *
   * @return URL of the database
   */
  String getURL();

  /**
   * Returns the level1 cache. Cannot be null.
   *
   * @return Current cache.
   */
  OLocalRecordCache getLocalCache();

  /**
   * Checks if the database is closed.
   *
   * @return true if is closed, otherwise false.
   */
  boolean isClosed();

  /**
   * Adds a new cluster for store blobs.
   *
   * @param iClusterName Cluster name
   * @param iParameters  Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addBlobCluster(String iClusterName, Object... iParameters);

  /**
   * Retrieve the set of defined blob cluster.
   *
   * @return the set of defined blob cluster ids.
   */
  IntSet getBlobClusterIds();

  /**
   * Drops a cluster by its name. Physical clusters will be completely deleted
   *
   * @param iClusterName the name of the cluster
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(String iClusterName);

  /**
   * Drops a cluster by its id. Physical clusters will be completely deleted.
   *
   * @param iClusterId id of cluster to delete
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(int iClusterId);

  /**
   * Returns a database attribute value
   *
   * @param iAttribute Attributes between #ATTRIBUTES enum
   * @return The attribute value
   */
  Object get(ATTRIBUTES iAttribute);

  /**
   * Sets a database attribute value
   *
   * @param iAttribute Attributes between #ATTRIBUTES enum
   * @param iValue     Value to set
   * @return underlying
   */
  <DB extends ODatabase> DB set(ATTRIBUTES iAttribute, Object iValue);

  /**
   * Flush cached storage content to the disk.
   *
   * <p>After this call users can perform only idempotent calls like read records and
   * select/traverse queries. All write-related operations will queued till {@link #release()}
   * command will be called.
   *
   * <p>Given command waits till all on going modifications in indexes or DB will be finished.
   *
   * <p>IMPORTANT: This command is not reentrant.
   *
   * @see #release()
   */
  void freeze();

  /**
   * Allows to execute write-related commands on DB. Called after {@link #freeze()} command.
   *
   * @see #freeze()
   */
  void release();

  /**
   * Flush cached storage content to the disk.
   *
   * <p>After this call users can perform only select queries. All write-related commands will
   * queued till {@link #release()} command will be called or exception will be thrown on attempt to
   * modify DB data. Concrete behaviour depends on <code>throwException</code> parameter.
   *
   * <p>IMPORTANT: This command is not reentrant.
   *
   * @param throwException If <code>true</code>
   *                       {@link
   *                       com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *                       exception will be thrown in case of write command will be performed.
   */
  void freeze(boolean throwException);

  /**
   * Returns the current user logged into the database.
   *
   * @see com.orientechnologies.orient.core.metadata.security.OSecurity
   */
  OSecurityUser getUser();

  /**
   * retrieves a class from the schema
   *
   * @param className The class name
   * @return The object representing the class in the schema. Null if the class does not exist.
   */
  default OClass getClass(String className) {
    OSchema schema = getMetadata().getSchema();
    return schema.getClass(className);
  }

  /**
   * Creates a new class in the schema
   *
   * @param className    the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws OSchemaException if a class with this name already exists or if one of the superclasses
   *                          does not exist.
   */
  default OClass createClass(String className, String... superclasses) throws OSchemaException {
    OSchema schema = getMetadata().getSchema();
    schema.reload();
    OClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new OClass[superclasses.length];
      for (int i = 0; i < superclasses.length; i++) {
        String superclass = superclasses[i];
        OClass superclazz = schema.getClass(superclass);
        if (superclazz == null) {
          throw new OSchemaException("Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superclazz;
      }
    }
    OClass result = schema.getClass(className);
    if (result != null) {
      throw new OSchemaException("Class " + className + " already exists");
    }
    if (superclassInstances == null) {
      return schema.createClass(className);
    } else {
      return schema.createClass(className, superclassInstances);
    }
  }

  /**
   * Creates a new abstract class in the schema
   *
   * @param className    the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws OSchemaException if a class with this name already exists or if one of the superclasses
   *                          does not exist.
   */
  default OClass createAbstractClass(String className, String... superclasses)
      throws OSchemaException {
    OSchema schema = getMetadata().getSchema();
    schema.reload();
    OClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new OClass[superclasses.length];
      for (int i = 0; i < superclasses.length; i++) {
        String superclass = superclasses[i];
        OClass superclazz = schema.getClass(superclass);
        if (superclazz == null) {
          throw new OSchemaException("Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superclazz;
      }
    }
    OClass result = schema.getClass(className);
    if (result != null) {
      throw new OSchemaException("Class " + className + " already exists");
    }
    if (superclassInstances == null) {
      return schema.createAbstractClass(className);
    } else {
      return schema.createAbstractClass(className, superclassInstances);
    }
  }

  /**
   * Loads the entity by the Record ID.
   *
   * @param recordId The unique record id of the entity to load.
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID recordId);

  /**
   * Checks if record exists in database. That happens in two cases:
   * <ol>
   *   <li>Record is already stored in database.</li>
   *   <li>Record is only added in current transaction.</li>
   * </ol>
   * <p/>
   *
   * @param rid Record id to check.
   * @return True if record exists, otherwise false.
   */
  boolean exists(ORID rid);

  /**
   * Saves an entity in synchronous mode. If the entity is not dirty, then the operation will be
   * ignored. For custom entity implementations assure to set the entity as dirty.
   *
   * @param iObject The entity to save
   * @return The saved entity.
   */
  <RET extends T> RET save(T iObject);

  /**
   * Deletes an entity from the database in synchronous mode.
   *
   * @param iObject The entity to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  ODatabase<T> delete(T iObject);

  /**
   * Deletes the entity with the received RID from the database.
   *
   * @param iRID The RecordID to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  ODatabase<T> delete(ORID iRID);

  /**
   * Return active transaction. Cannot be null. If no transaction is active, then a OTransactionNoTx
   * instance is returned.
   *
   * @return OTransaction implementation
   */
  OTransaction getTransaction();

  /**
   * Begins a new transaction. By default the type is OPTIMISTIC. If a previous transaction is
   * running a nested call counter is incremented. A transaction once begun has to be closed by
   * calling the {@link #commit()} or {@link #rollback()}.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  ODatabase<T> begin();

  /**
   * Commits the current transaction. The approach is all or nothing. All changes will be permanent
   * following the storage type. If the operation succeed all the entities changed inside the
   * transaction context will be effective. If the operation fails, all the changed entities will be
   * restored in the data store.
   */
  ODatabase<T> commit() throws OTransactionException;

  /**
   * Aborts the current running transaction. All the pending changed entities will be restored in
   * the data store.
   */
  ODatabase<T> rollback() throws OTransactionException;

  /**
   * Executes an SQL query. The result set has to be closed after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   * OResultSet rs = db.query("SELECT FROM V where name = ?", "John"); while(rs.hasNext()){ OResult
   * item = rs.next(); ... } rs.close(); </code>
   *
   * @param query the query string
   * @param args  query parameters (positional)
   * @return the query result set
   */
  default OResultSet query(String query, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes an SQL query (idempotent). The result set has to be closed after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   * Map&lt;String, Object&gt params = new HashMapMap&lt;&gt(); params.put("name", "John");
   * OResultSet rs = db.query("SELECT FROM V where name = :name", params); while(rs.hasNext()){
   * OResult item = rs.next(); ... } rs.close();
   * </code>
   *
   * @param query the query string
   * @param args  query parameters (named)
   * @return
   */
  default OResultSet query(String query, Map args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes a generic (idempotent or non idempotent) command. The result set has to be closed
   * after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   * OResultSet rs = db.command("INSERT INTO Person SET name = ?", "John"); ... rs.close(); </code>
   *
   * @param query
   * @param args  query arguments
   * @return
   */
  default OResultSet command(String query, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes a generic (idempotent or non idempotent) command. The result set has to be closed
   * after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   * Map&lt;String, Object&gt params = new HashMapMap&lt;&gt(); params.put("name", "John");
   * OResultSet rs = db.query("INSERT INTO Person SET name = :name", params); ... rs.close();
   * </code>
   *
   * @param query
   * @param args
   * @return
   */
  default OResultSet command(String query, Map args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Execute a script in a specified query language. The result set has to be closed after usage
   * <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   * String script = "INSERT INTO Person SET name = 'foo', surname = ?;"+ "INSERT INTO Person SET
   * name = 'bar', surname = ?;"+ "INSERT INTO Person SET name = 'baz', surname = ?;";
   * <p>
   * OResultSet rs = db.execute("sql", script, "Surname1", "Surname2", "Surname3"); ... rs.close();
   * </code>
   *
   * @param language
   * @param script
   * @param args
   * @return
   */
  default OResultSet execute(String language, String script, Object... args)
      throws OCommandExecutionException, OCommandScriptException {
    throw new UnsupportedOperationException();
  }

  /**
   * Execute a script of a specified query language The result set has to be closed after usage
   * <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   * Map&lt;String, Object&gt params = new HashMapMap&lt;&gt(); params.put("surname1", "Jones");
   * params.put("surname2", "May"); params.put("surname3", "Ali");
   * <p>
   * String script = "INSERT INTO Person SET name = 'foo', surname = :surname1;"+ "INSERT INTO
   * Person SET name = 'bar', surname = :surname2;"+ "INSERT INTO Person SET name = 'baz', surname =
   * :surname3;";
   * <p>
   * OResultSet rs = db.execute("sql", script, params); ... rs.close(); </code>
   *
   * @param language
   * @param script
   * @param args
   * @return
   */
  default OResultSet execute(String language, String script, Map<String, ?> args)
      throws OCommandExecutionException, OCommandScriptException {
    throw new UnsupportedOperationException();
  }

  /**
   * Registers a hook to listen all events for Records.
   *
   * @param iHookImpl ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  <DB extends ODatabase<?>> DB registerHook(ORecordHook iHookImpl);

  <DB extends ODatabase<?>> DB registerHook(
      final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition);

  /**
   * Retrieves all the registered hooks.
   *
   * @return A not-null unmodifiable map of ORecordHook and position instances. If there are no
   * hooks registered, the Map is empty.
   */
  Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks();

  /**
   * Unregisters a previously registered hook.
   *
   * @param iHookImpl ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain. deprecated since 2.2
   */
  <DB extends ODatabase<?>> DB unregisterHook(ORecordHook iHookImpl);

  /**
   * Retrieves all the registered listeners.
   *
   * @return An iterable of ODatabaseListener instances.
   */
  Iterable<ODatabaseListener> getListeners();

  /**
   * Performs incremental backup of database content to the selected folder. This is thread safe
   * operation and can be done in normal operational mode.
   *
   * <p>If it will be first backup of data full content of database will be copied into folder
   * otherwise only changes after last backup in the same folder will be copied.
   *
   * @param path Path to backup folder.
   * @return File name of the backup
   * @since 2.2
   */
  String incrementalBackup(String path) throws UnsupportedOperationException;

  /**
   * Subscribe a query as a live query for future create/update event with the referred conditions
   *
   * @param query    live query
   * @param listener the listener that receive the query results
   * @param args     the live query args
   */
  OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Map<String, ?> args);

  /**
   * Subscribe a query as a live query for future create/update event with the referred conditions
   *
   * @param query    live query
   * @param listener the listener that receive the query results
   * @param args     the live query args
   */
  OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args);

  OMetadata getMetadata();
}
