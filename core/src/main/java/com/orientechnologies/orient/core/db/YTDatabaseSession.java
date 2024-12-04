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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.lock.YTModificationOperationProhibitedException;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.script.YTCommandScriptException;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.exception.YTTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTEdgeInternal;
import com.orientechnologies.orient.core.sql.YTCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Session for database operations with a specific user.
 */
public interface YTDatabaseSession extends AutoCloseable {

  enum STATUS {
    OPEN,
    CLOSED,
    IMPORTING
  }

  /**
   * Executes the passed in code in a transaction. Starts a transaction if not already started, in
   * this case the transaction is committed after the code is executed or rolled back if an
   * exception is thrown.
   *
   * @param runnable Code to execute in transaction
   */
  void executeInTx(Runnable runnable);

  /**
   * Splits data provided by iterator in batches and execute every batch in separate transaction.
   * Currently, YouTrackDB accumulates all changes in single batch in memory and then commits them to
   * the storage, that causes OutOfMemoryError in case of large data sets. This method allows to
   * avoid described problems.
   *
   * @param <T>       Type of data
   * @param iterator  Data to process
   * @param batchSize Size of batch
   * @param consumer  Consumer to process data
   */
  <T> void executeInTxBatches(
      Iterator<T> iterator, int batchSize, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Splits data by batches, size of each batch is specified by parameter
   * {@link YTGlobalConfiguration#TX_BATCH_SIZE}.
   *
   * @param iterator Data to process
   * @param consumer Consumer to process data
   * @param <T>      Type of data
   * @see #executeInTxBatches(Iterator, int, BiConsumer)
   */
  <T> void executeInTxBatches(Iterator<T> iterator, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Splits data provided by iterator in batches and execute every batch in separate transaction.
   * Currently, YouTrackDB accumulates all changes in single batch in memory and then commits them to
   * the storage, that causes OutOfMemoryError in case of large data sets. This method allows to
   * avoid described problems.
   *
   * @param <T>       Type of data
   * @param iterable  Data to process
   * @param batchSize Size of batch
   * @param consumer  Consumer to process data
   */
  <T> void executeInTxBatches(
      Iterable<T> iterable, int batchSize, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Splits data by batches, size of each batch is specified by parameter
   * {@link YTGlobalConfiguration#TX_BATCH_SIZE}.
   *
   * @param iterable Data to process
   * @param consumer Consumer to process data
   * @param <T>      Type of data
   * @see #executeInTxBatches(Iterable, BiConsumer)
   */
  <T> void executeInTxBatches(Iterable<T> iterable, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Splits data provided by stream in batches and execute every batch in separate transaction.
   * Currently, YouTrackDB accumulates all changes in single batch in memory and then commits them to
   * the storage, that causes OutOfMemoryError in case of large data sets. This method allows to
   * avoid described problems.
   * <p>
   * Stream is closed after processing.
   *
   * @param <T>       Type of data
   * @param stream    Data to process
   * @param batchSize Size of batch
   * @param consumer  Consumer to process data
   */
  <T> void executeInTxBatches(
      Stream<T> stream, int batchSize, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Splits processing of  data by batches, size of each batch is specified by parameter
   * {@link YTGlobalConfiguration#TX_BATCH_SIZE}.
   * <p>
   * Stream is closed after processing.
   *
   * @param stream   Data to process
   * @param consumer Consumer to process data
   * @param <T>      Type of data
   * @see #executeInTxBatches(Stream, int, BiConsumer)
   */
  <T> void executeInTxBatches(Stream<T> stream, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Executes the given code in a transaction. Starts a transaction if not already started, in this
   * case the transaction is committed after the code is executed or rolled back if an exception is
   * thrown.
   *
   * @param supplier Code to execute in transaction
   * @param <T>      the type of the returned result
   * @return the result of the code execution
   */
  <T> T computeInTx(Supplier<T> supplier);

  /**
   * Executes the given code for each element in the iterator in a transaction. Starts a transaction
   * if not already started, in this case the transaction is committed after the code is executed or
   * rolled back if an exception is thrown.
   *
   * @param iterator the iterator to iterate over
   * @param <T>      the type of the elements in the iterator
   */
  <T> void forEachInTx(Iterator<T> iterator, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Executes the given code for each element in the iterable in a transaction. Starts a transaction
   * if not already started, in this case the transaction is committed after the code is executed or
   * rolled back if an exception is thrown.
   * <p>
   * The consumer function should return true if it wants to continue the iteration, false
   * otherwise.
   *
   * @param iterable the iterable to iterate over
   * @param consumer the code to execute for each element
   * @param <T>      the type of the elements in the iterable
   */
  <T> void forEachInTx(Iterable<T> iterable, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Executes the given code for each element in the stream in a transaction. Starts a transaction
   * if not already started, in this case the transaction is committed after the code is executed or
   * rolled back if an exception is thrown.
   * <p>
   * The consumer function should return true if it wants to continue the iteration, false
   * otherwise.
   * <p>
   * The stream is closed after processing.
   *
   * @param stream   the stream to iterate over
   * @param consumer the code to execute for each element
   * @param <T>      the type of the elements in the stream
   */
  <T> void forEachInTx(Stream<T> stream, BiConsumer<YTDatabaseSession, T> consumer);

  /**
   * Executes the given code for each element in the iterator in a transaction. Starts a transaction
   * if not already started, in this case the transaction is committed after the code is executed or
   * rolled back if an exception is thrown.
   * <p>
   * The consumer function should return true if it wants to continue the iteration, false
   * otherwise.
   *
   * @param iterator the iterator to iterate over
   * @param <T>      the type of the elements in the iterator
   */
  <T> void forEachInTx(Iterator<T> iterator, BiFunction<YTDatabaseSession, T, Boolean> consumer);


  /**
   * Executes the given code for each element in the iterable in a transaction. Starts a transaction
   * if not already started, in this case the transaction is committed after the code is executed or
   * rolled back if an exception is thrown.
   * <p>
   * The consumer function should return true if it wants to continue the iteration, false
   * otherwise.
   *
   * @param iterable the iterable to iterate over
   * @param consumer the code to execute for each element
   * @param <T>      the type of the elements in the iterable
   */
  <T> void forEachInTx(Iterable<T> iterable, BiFunction<YTDatabaseSession, T, Boolean> consumer);

  /**
   * Executes the given code for each element in the stream in a transaction. Starts a transaction
   * if not already started, in this case the transaction is committed after the code is executed or
   * rolled back if an exception is thrown.
   * <p>
   * The consumer function should return true if it wants to continue the iteration, false
   * otherwise.
   * <p>
   * The stream is closed after processing.
   *
   * @param stream   the stream to iterate over
   * @param consumer the code to execute for each element
   * @param <T>      the type of the elements in the stream
   */
  <T> void forEachInTx(Stream<T> stream, BiFunction<YTDatabaseSession, T, Boolean> consumer);

  /**
   * Binds current record to the session. It is mandatory to call this method in case you use
   * records that are not created or loaded by the session. Method returns bounded instance of given
   * record, usage of passed in instance is prohibited.
   * <p>
   * Method throws {@link YTRecordNotFoundException} if record does not exist in database or if
   * record rid is temporary.
   * <p/>
   * You can verify if record already bound to the session by calling
   * {@link YTRecord#isNotBound(YTDatabaseSession)} method.
   * <p/>
   * <p>
   * Records with temporary RIDs are not allowed to be bound to the session and can not be accepted
   * from the outside of the transaction boundaries.
   *
   * @param identifiable Record or rid to bind to the session, passed in instance is
   *                     <b>prohibited</b> for further usage.
   * @param <T>          Type of record.
   * @return Bounded instance of given record.
   * @throws YTRecordNotFoundException if record does not exist in database
   * @throws YTDatabaseException       if the record rid is temporary
   * @see YTRecord#isNotBound(YTDatabaseSession)
   * @see YTIdentifiable#getIdentity()
   * @see YTRID#isPersistent()
   */
  <T extends YTIdentifiable> T bindToSession(T identifiable);

  /**
   * Returns the schema of the database.
   *
   * @return the schema of the database
   */
  YTSchema getSchema();

  /**
   * Returns the number of active nested transactions.
   *
   * @return the number of active transactions, 0 means no active transactions are present.
   * @see #begin()
   * @see #commit()
   * @see #rollback()
   */
  int activeTxCount();

  /**
   * Loads an element by its id, throws an exception if record is not an element or does not exist.
   *
   * @param id the id of the element to load
   * @return the loaded element
   * @throws YTDatabaseException                                                   if the record is
   *                                                                              not an element
   * @throws YTRecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default YTEntity loadElement(YTRID id) throws YTDatabaseException, YTRecordNotFoundException {
    var record = load(id);
    if (record instanceof YTEntity element) {
      return element;
    }

    throw new YTDatabaseException(
        "Record with id " + id + " is not an element, but a " + record.getClass().getSimpleName());
  }

  /**
   * Loads a vertex by its id, throws an exception if record is not a vertex or does not exist.
   *
   * @param id the id of the vertex to load
   * @return the loaded vertex
   * @throws YTDatabaseException                                                   if the record is
   *                                                                              not a vertex
   * @throws YTRecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default YTVertex loadVertex(YTRID id) throws YTDatabaseException, YTRecordNotFoundException {
    var record = load(id);
    if (record instanceof YTVertex vertex) {
      return vertex;
    }

    throw new YTDatabaseException(
        "Record with id " + id + " is not a vertex, but a " + record.getClass().getSimpleName());
  }

  /**
   * Loads an edge by its id, throws an exception if record is not an edge or does not exist.
   *
   * @param id the id of the edge to load
   * @return the loaded edge
   * @throws YTDatabaseException                                                   if the record is
   *                                                                              not an edge
   * @throws YTRecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default YTEdge loadEdge(YTRID id) throws YTDatabaseException, YTRecordNotFoundException {
    var record = load(id);
    if (record instanceof YTEdge edge) {
      return edge;
    }

    throw new YTDatabaseException(
        "Record with id " + id + " is not an edge, but a " + record.getClass().getSimpleName());
  }

  /**
   * Loads a blob by its id, throws an exception if record is not a blob or does not exist.
   *
   * @param id the id of the blob to load
   * @return the loaded blob
   * @throws YTDatabaseException                                                   if the record is
   *                                                                              not a blob
   * @throws YTRecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default YTBlob loadBlob(YTRID id) throws YTDatabaseException, YTRecordNotFoundException {
    var record = load(id);
    if (record instanceof YTBlob blob) {
      return blob;
    }

    throw new YTDatabaseException(
        "Record with id " + id + " is not a blob, but a " + record.getClass().getSimpleName());
  }

  /**
   * Create a new instance of a blob containing the given bytes.
   *
   * @param bytes content of the YTBlob
   * @return the YTBlob instance.
   */
  YTBlob newBlob(byte[] bytes);

  /**
   * Create a new empty instance of a blob.
   *
   * @return the YTBlob instance.
   */
  YTBlob newBlob();

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   * otherwise.
   */
  boolean isPooled();

  YTEntity newElement();

  YTEntity newElement(final String className);

  /**
   * Creates a new Edge of type E
   *
   * @param from the starting point vertex
   * @param to   the endpoint vertex
   * @return the edge
   */
  default YTEdge newEdge(YTVertex from, YTVertex to) {
    return newEdge(from, to, "E");
  }

  /**
   * Creates a new Edge
   *
   * @param from the starting point vertex
   * @param to   the endpoint vertex
   * @param type the edge type
   * @return the edge
   */
  YTEdge newEdge(YTVertex from, YTVertex to, YTClass type);

  /**
   * Creates a new Edge
   *
   * @param from the starting point vertex
   * @param to   the endpoint vertex
   * @param type the edge type
   * @return the edge
   */
  YTEdgeInternal newEdge(YTVertex from, YTVertex to, String type);

  /**
   * Creates a new Vertex of type V
   */
  default YTVertex newVertex() {
    return newVertex("V");
  }

  /**
   * Creates a new Vertex
   *
   * @param type the vertex type
   */
  YTVertex newVertex(YTClass type);

  /**
   * Creates a new Vertex
   *
   * @param type the vertex type (class name)
   */
  YTVertex newVertex(String type);

  /**
   * creates a new vertex class (a class that extends V)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws YTSchemaException if the class already exists or if V class is not defined (Eg. if it
   *                          was deleted from the schema)
   */
  default YTClass createVertexClass(String className) throws YTSchemaException {
    return createClass(className, "V");
  }

  /**
   * creates a new edge class (a class that extends E)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws YTSchemaException if the class already exists or if E class is not defined (Eg. if it
   *                          was deleted from the schema)
   */
  default YTClass createEdgeClass(String className) {
    var edgeClass = createClass(className, "E");

    edgeClass.createProperty(this, YTEdge.DIRECTION_IN, YTType.LINK);
    edgeClass.createProperty(this, YTEdge.DIRECTION_OUT, YTType.LINK);

    return edgeClass;
  }

  /**
   * Creates a new edge class for lightweight edge (an abstract class that extends E)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws YTSchemaException if the class already exists or if E class is not defined (Eg. if it
   *                          was deleted from the schema)
   */
  default YTClass createLightweightEdgeClass(String className) {
    return createAbstractClass(className, "E");
  }

  /**
   * If a class with given name already exists, it's just returned, otherwise the method creates a
   * new class and returns it.
   *
   * @param className    the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws YTSchemaException if one of the superclasses does not exist in the schema
   */
  default YTClass createClassIfNotExist(String className, String... superclasses)
      throws YTSchemaException {
    YTSchema schema = getSchema();
    schema.reload();

    YTClass result = schema.getClass(className);
    if (result == null) {
      result = createClass(className, superclasses);
    }
    schema.reload();
    return result;
  }

  /**
   * Activate current database instance on current thread. Call this method before using the
   * database if you switch between multiple databases instances on the same thread or if you pass
   * them across threads.
   */
  void activateOnCurrentThread();

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
   *                       YTModificationOperationProhibitedException}
   *                       exception will be thrown in case of write command will be performed.
   */
  void freeze(boolean throwException);

  /**
   * Returns the current user logged into the database.
   */
  OSecurityUser getUser();

  /**
   * retrieves a class from the schema
   *
   * @param className The class name
   * @return The object representing the class in the schema. Null if the class does not exist.
   */
  default YTClass getClass(String className) {
    YTSchema schema = getSchema();
    return schema.getClass(className);
  }

  /**
   * Creates a new class in the schema
   *
   * @param className    the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws YTSchemaException if a class with this name already exists or if one of the superclasses
   *                          does not exist.
   */
  default YTClass createClass(String className, String... superclasses) throws YTSchemaException {
    YTSchema schema = getSchema();
    schema.reload();
    YTClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new YTClass[superclasses.length];
      for (int i = 0; i < superclasses.length; i++) {
        String superclass = superclasses[i];
        YTClass superclazz = schema.getClass(superclass);
        if (superclazz == null) {
          throw new YTSchemaException("Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superclazz;
      }
    }
    YTClass result = schema.getClass(className);
    if (result != null) {
      throw new YTSchemaException("Class " + className + " already exists");
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
   * @throws YTSchemaException if a class with this name already exists or if one of the superclasses
   *                          does not exist.
   */
  default YTClass createAbstractClass(String className, String... superclasses)
      throws YTSchemaException {
    YTSchema schema = getSchema();
    schema.reload();
    YTClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new YTClass[superclasses.length];
      for (int i = 0; i < superclasses.length; i++) {
        String superclass = superclasses[i];
        YTClass superclazz = schema.getClass(superclass);
        if (superclazz == null) {
          throw new YTSchemaException("Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superclazz;
      }
    }
    YTClass result = schema.getClass(className);
    if (result != null) {
      throw new YTSchemaException("Class " + className + " already exists");
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
   * @throws YTRecordNotFoundException if record does not
   *                                                                              exist in database
   */
  @Nonnull
  <RET extends YTRecord> RET load(YTRID recordId);

  /**
   * Loads the entity by the Record ID, unlike {@link  #load(YTRID)} method does not throw exception
   * if record not found but returns <code>null</code> instead.
   *
   * @param recordId The unique record id of the entity to load.
   * @return The loaded entity or <code>null</code> if entity does not exist.
   */
  @Nullable
  default <RET extends YTRecord> RET loadSilently(YTRID recordId) {
    try {
      return load(recordId);
    } catch (YTRecordNotFoundException e) {
      return null;
    }
  }

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
  boolean exists(YTRID rid);

  /**
   * Saves an entity in synchronous mode. If the entity is not dirty, then the operation will be
   * ignored. For custom entity implementations assure to set the entity as dirty.
   *
   * @param record The entity to save
   * @return The saved entity.
   */
  <RET extends YTRecord> RET save(YTRecord record);

  /**
   * Deletes an entity from the database in synchronous mode.
   *
   * @param record The entity to delete.
   */
  void delete(YTRecord record);

  /**
   * Deletes the entity with the received RID from the database.
   *
   * @param iRID The RecordID to delete.
   */
  void delete(YTRID iRID);

  /**
   * Begins a new transaction.If a previous transaction is running a nested call counter is
   * incremented. A transaction once begun has to be closed by calling the {@link #commit()} or
   * {@link #rollback()}.
   *
   * @return Amount of nested transaction calls. First call is 1.
   */
  int begin();

  /**
   * Commits the current transaction. The approach is all or nothing. All changes will be permanent
   * following the storage type. If the operation succeed all the entities changed inside the
   * transaction context will be effective. If the operation fails, all the changed entities will be
   * restored in the data store.
   *
   * @return true if the transaction is the last nested transaction and thus cmd can be committed,
   * otherwise false. If false is returned, then there are still nested transaction that have to be
   * committed.
   */
  boolean commit() throws YTTransactionException;

  /**
   * Aborts the current running transaction. All the pending changed entities will be restored in
   * the data store.
   */
  void rollback() throws YTTransactionException;

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
      throws YTCommandSQLParsingException, YTCommandExecutionException {
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
   * @return the query result set
   */
  default OResultSet query(String query, Map args)
      throws YTCommandSQLParsingException, YTCommandExecutionException {
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
   * @return the query result set
   */
  default OResultSet command(String query, Object... args)
      throws YTCommandSQLParsingException, YTCommandExecutionException {
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
      throws YTCommandSQLParsingException, YTCommandExecutionException {
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
      throws YTCommandExecutionException, YTCommandScriptException {
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
      throws YTCommandExecutionException, YTCommandScriptException {
    throw new UnsupportedOperationException();
  }

  /**
   * Registers a hook to listen all events for Records.
   *
   * @param iHookImpl ORecordHook implementation
   */
  void registerHook(ORecordHook iHookImpl);

  void registerHook(final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition);

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
   */
  void unregisterHook(ORecordHook iHookImpl);

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
  String incrementalBackup(String path);

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
}
