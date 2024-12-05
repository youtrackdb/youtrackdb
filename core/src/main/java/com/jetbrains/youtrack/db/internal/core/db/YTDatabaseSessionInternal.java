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

package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.command.OCommandRequest;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.ORecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.db.document.OQueryDatabaseState;
import com.jetbrains.youtrack.db.internal.core.db.record.OCurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.dictionary.ODictionary;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.enterprise.OEnterpriseEndpoint;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.YTTransactionException;
import com.jetbrains.youtrack.db.internal.core.hook.YTRecordHook;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.ORecordIteratorClass;
import com.jetbrains.youtrack.db.internal.core.iterator.ORecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTView;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.OSequenceAction;
import com.jetbrains.youtrack.db.internal.core.query.OQuery;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.ORecordMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import com.jetbrains.youtrack.db.internal.core.storage.OStorageInfo;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.tx.OTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionData;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionNoTx;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionOptimistic;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

public interface YTDatabaseSessionInternal extends YTDatabaseSession {

  String TYPE = "document";

  /**
   * Internal. Returns the factory that defines a set of components that current database should use
   * to be compatible to current version of storage. So if you open a database create with old
   * version of YouTrackDB it defines a components that should be used to provide backward
   * compatibility with that version of database.
   */
  OCurrentStorageComponentsFactory getStorageVersions();

  /**
   * Creates a new element instance.
   *
   * @return The new instance.
   */
  <RET extends Entity> RET newInstance(String iClassName);

  <RET extends Entity> RET newInstance();

  /**
   * Internal. Gets an instance of sb-tree collection manager for current database.
   */
  OSBTreeCollectionManager getSbTreeCollectionManager();

  /**
   * @return the factory of binary serializers.
   */
  OBinarySerializerFactory getSerializerFactory();

  /**
   * Returns the default record type for this kind of database.
   */
  byte getRecordType();

  /**
   * @return serializer which is used for document serialization.
   */
  ORecordSerializer getSerializer();

  int begin(OTransactionOptimistic tx);

  void setSerializer(ORecordSerializer serializer);

  int assignAndCheckCluster(Record record, String iClusterName);

  void reloadUser();

  void afterReadOperations(final YTIdentifiable identifiable);

  /**
   * @param identifiable
   * @return true if the record should be skipped
   */
  boolean beforeReadOperations(final YTIdentifiable identifiable);

  /**
   * @param id
   * @param iClusterName
   * @return null if nothing changed the instance if has been modified or replaced
   */
  YTIdentifiable beforeCreateOperations(final YTIdentifiable id, String iClusterName);

  /**
   * @param id
   * @param iClusterName
   * @return null if nothing changed the instance if has been modified or replaced
   */
  YTIdentifiable beforeUpdateOperations(final YTIdentifiable id, String iClusterName);

  void beforeDeleteOperations(final YTIdentifiable id, String iClusterName);

  void afterUpdateOperations(final YTIdentifiable id);

  void afterCreateOperations(final YTIdentifiable id);

  void afterDeleteOperations(final YTIdentifiable id);

  YTRecordHook.RESULT callbackHooks(final YTRecordHook.TYPE type, final YTIdentifiable id);

  @Nonnull
  <RET extends RecordAbstract> RET executeReadRecord(final YTRecordId rid);

  boolean executeExists(YTRID rid);

  void setDefaultTransactionMode();

  YTDatabaseSessionInternal copy();

  void recycle(Record record);

  void checkIfActive();


  boolean assertIfNotActive();

  void callOnOpenListeners();

  void callOnCloseListeners();

  void callOnDropListeners();

  YTDatabaseSession setCustom(final String name, final Object iValue);

  void setPrefetchRecords(boolean prefetchRecords);

  boolean isPrefetchRecords();

  void checkForClusterPermissions(String name);

  default YTResultSet getActiveQuery(String id) {
    throw new UnsupportedOperationException();
  }

  default Map<String, OQueryDatabaseState> getActiveQueries() {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a new lightweight edge with the specified class name, starting vertex, and ending
   * vertex.
   *
   * @param iClassName the class name of the edge
   * @param from       the starting vertex of the edge
   * @param to         the ending vertex of the edge
   * @return the new lightweight edge
   */
  EdgeInternal newLightweightEdge(String iClassName, Vertex from, Vertex to);

  EdgeInternal addLightweightEdge(Vertex from, Vertex to, String className);

  Edge newRegularEdge(String iClassName, Vertex from, Vertex to);

  void setUseLightweightEdges(boolean b);

  YTDatabaseSessionInternal cleanOutRecord(YTRID rid, int version);

  default void realClose() {
    // Only implemented by pooled instances
    throw new UnsupportedOperationException();
  }

  default void reuse() {
    // Only implemented by pooled instances
    throw new UnsupportedOperationException();
  }

  /**
   * synchronizes current database instance with the rest of the cluster (if in distributed mode).
   *
   * @return true if the database was synchronized, false otherwise
   */
  default boolean sync(boolean forceDeployment, boolean tryWithDelta) {
    return false;
  }

  default Map<String, Object> getHaStatus(
      boolean servers, boolean db, boolean latency, boolean messages) {
    return null;
  }

  default boolean removeHaServer(String serverName) {
    return false;
  }

  /**
   * sends an execution plan to a remote node for a remote query execution
   *
   * @param nodeName        the node name
   * @param executionPlan   the execution plan
   * @param inputParameters the input parameters for execution
   * @return an YTResultSet to fetch the results of the query execution
   */
  default YTResultSet queryOnNode(
      String nodeName, OExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    throw new UnsupportedOperationException();
  }

  /**
   * Executed the commit on the storage hiding away storage concepts from the transaction
   *
   * @param transaction
   */
  void internalCommit(OTransactionOptimistic transaction);

  boolean isClusterVertex(int cluster);

  boolean isClusterEdge(int cluster);

  boolean isClusterView(int cluster);

  void internalClose(boolean recycle);

  String getClusterName(final Record record);

  default YTResultSet indexQuery(String indexName, String query, Object... args) {
    return command(query, args);
  }

  YTView getViewFromCluster(int cluster);

  <T> T sendSequenceAction(OSequenceAction action) throws ExecutionException, InterruptedException;

  default boolean isDistributed() {
    return false;
  }

  default boolean isRemote() {
    return false;
  }

  Map<UUID, OBonsaiCollectionPointer> getCollectionsChanges();

  default void syncCommit(OTransactionData data) {
    throw new UnsupportedOperationException();
  }

  default boolean isLocalEnv() {
    return true;
  }

  boolean dropClusterInternal(int clusterId);

  default String getStorageId() {
    return getName();
  }

  long[] getClusterDataRange(int currentClusterId);

  void setDefaultClusterId(int addCluster);

  long getLastClusterPosition(int clusterId);

  String getClusterRecordConflictStrategy(int clusterId);

  int[] getClustersIds(Set<String> filterClusters);

  default void startExclusiveMetadataChange() {
  }

  default void endExclusiveMetadataChange() {
  }

  default void queryStartUsingViewCluster(int cluster) {
  }

  default void queryStartUsingViewIndex(String index) {
  }

  void truncateClass(String name);

  long truncateClass(String name, boolean polimorfic);

  long truncateClusterInternal(String name);

  OTransactionNoTx.NonTxReadMode getNonTxReadMode();

  /**
   * Browses all the records of the specified cluster.
   *
   * @param iClusterName Cluster name to iterate
   * @return Iterator of EntityImpl instances
   */
  <REC extends Record> ORecordIteratorCluster<REC> browseCluster(String iClusterName);

  <REC extends Record> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones);

  /**
   * Browses all the records of the specified cluster of the passed record type.
   *
   * @param iClusterName Cluster name to iterate
   * @param iRecordClass The record class expected
   * @return Iterator of EntityImpl instances
   */
  @Deprecated
  <REC extends Record> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName, Class<REC> iRecordClass);

  @Deprecated
  <REC extends Record> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition);

  @Deprecated
  <REC extends Record> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones);

  /**
   * Browses all the records of the specified class and also all the subclasses. If you've a class
   * Vehicle and Car that extends Vehicle then a db.browseClass("Vehicle", true) will return all the
   * instances of Vehicle and Car. The order of the returned instance starts from record id with
   * position 0 until the end. Base classes are worked at first.
   *
   * @param iClassName Class name to iterate
   * @return Iterator of EntityImpl instances
   */
  ORecordIteratorClass<EntityImpl> browseClass(String iClassName);

  /**
   * Browses all the records of the specified class and if iPolymorphic is true also all the
   * subclasses. If you've a class Vehicle and Car that extends Vehicle then a
   * db.browseClass("Vehicle", true) will return all the instances of Vehicle and Car. The order of
   * the returned instance starts from record id with position 0 until the end. Base classes are
   * worked at first.
   *
   * @param iClassName   Class name to iterate
   * @param iPolymorphic Consider also the instances of the subclasses or not
   * @return Iterator of EntityImpl instances
   */
  ORecordIteratorClass<EntityImpl> browseClass(String iClassName, boolean iPolymorphic);

  /**
   * Counts the entities contained in the specified class and sub classes (polymorphic).
   *
   * @param iClassName Class name
   * @return Total entities
   */
  long countClass(String iClassName);

  /**
   * Counts the entities contained in the specified class.
   *
   * @param iClassName   Class name
   * @param iPolymorphic True if consider also the sub classes, otherwise false
   * @return Total entities
   */
  long countClass(String iClassName, final boolean iPolymorphic);

  long countView(String iClassName);

  /**
   * Checks if the operation on a resource is allowed for the current user.
   *
   * @param iResource  Resource where to execute the operation
   * @param iOperation Operation to execute against the resource
   */
  @Deprecated
  void checkSecurity(String iResource, int iOperation);

  /**
   * Tells if validation of record is active. Default is true.
   *
   * @return true if it's active, otherwise false.
   */
  boolean isValidationEnabled();

  /**
   * Enables or disables the record validation.
   *
   * <p>Since 2.2 this setting is persistent.
   *
   * @param iEnabled True to enable, false to disable
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  YTDatabaseSession setValidationEnabled(boolean iEnabled);

  /**
   * Returns true if current configuration retains objects, otherwise false
   *
   * @see #setRetainRecords(boolean)
   */
  boolean isRetainRecords();

  /**
   * Specifies if retain handled objects in memory or not. Setting it to false can improve
   * performance on large inserts. Default is enabled.
   *
   * @param iValue True to enable, false to disable it.
   * @see #isRetainRecords()
   */
  YTDatabaseSession setRetainRecords(boolean iValue);

  /**
   * Checks if the operation on a resource is allowed for the current user. The check is made in two
   * steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resource
   * </ol>
   *
   * @param iResourceGeneric  Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation        Operation to execute against the resource
   * @param iResourceSpecific Target resource, i.e.: "employee" to specify the cluster name.
   */
  @Deprecated
  void checkSecurity(String iResourceGeneric, int iOperation, Object iResourceSpecific);

  /**
   * Checks if the operation against multiple resources is allowed for the current user. The check
   * is made in two steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resources
   * </ol>
   *
   * @param iResourceGeneric   Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation         Operation to execute against the resource
   * @param iResourcesSpecific Target resources as an array of Objects, i.e.: ["employee", 2] to
   *                           specify cluster name and id.
   */
  @Deprecated
  void checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific);

  /**
   * Checks if the operation on a resource is allowed for the current user.
   *
   * @param resourceGeneric Generic Resource where to execute the operation
   * @param iOperation      Operation to execute against the resource
   */
  void checkSecurity(
      ORule.ResourceGeneric resourceGeneric, String resourceSpecific, int iOperation);

  /**
   * Checks if the operation on a resource is allowed for the current user. The check is made in two
   * steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resource
   * </ol>
   *
   * @param iResourceGeneric  Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation        Operation to execute against the resource
   * @param iResourceSpecific Target resource, i.e.: "employee" to specify the cluster name.
   */
  void checkSecurity(
      ORule.ResourceGeneric iResourceGeneric, int iOperation, Object iResourceSpecific);

  /**
   * Checks if the operation against multiple resources is allowed for the current user. The check
   * is made in two steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resources
   * </ol>
   *
   * @param iResourceGeneric   Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation         Operation to execute against the resource
   * @param iResourcesSpecific Target resources as an array of Objects, i.e.: ["employee", 2] to
   *                           specify cluster name and id.
   */
  void checkSecurity(
      ORule.ResourceGeneric iResourceGeneric, int iOperation, Object... iResourcesSpecific);

  /**
   * Return active transaction. Cannot be null. If no transaction is active, then a OTransactionNoTx
   * instance is returned.
   *
   * @return OTransaction implementation
   */
  OTransaction getTransaction();

  /**
   * Reloads the database information like the cluster list.
   */
  void reload();

  /**
   * Returns the underlying storage implementation.
   *
   * @return The underlying storage implementation
   * @see OStorage
   */
  OStorage getStorage();

  OStorageInfo getStorageInfo();

  /**
   * Set user for current database instance.
   */
  void setUser(YTSecurityUser user);

  /**
   * Internal only: replace the storage with a new one.
   *
   * @param iNewStorage The new storage to use. Usually it's a wrapped instance of the current
   *                    cluster.
   */
  void replaceStorage(OStorage iNewStorage);

  void resetInitialization();

  /**
   * Returns the database owner. Used in wrapped instances to know the up level ODatabase instance.
   *
   * @return Returns the database owner.
   */
  YTDatabaseSessionInternal getDatabaseOwner();

  /**
   * Internal. Sets the database owner.
   */
  YTDatabaseSessionInternal setDatabaseOwner(YTDatabaseSessionInternal iOwner);

  /**
   * Return the underlying database. Used in wrapper instances to know the down level ODatabase
   * instance.
   *
   * @return The underlying ODatabase implementation.
   */
  YTDatabaseSession getUnderlying();

  /**
   * Internal method. Don't call it directly unless you're building an internal component.
   */
  void setInternal(ATTRIBUTES attribute, Object iValue);

  /**
   * Opens a database using an authentication token received as an argument.
   *
   * @param iToken Authentication token
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession open(final OToken iToken);

  OSharedContext getSharedContext();

  /**
   * returns the cluster map for current deploy. The keys of the map are node names, the values
   * contain names of clusters (data files) available on the single node.
   *
   * @return the cluster map for current deploy
   */
  default String getLocalNodeName() {
    return "local";
  }

  /**
   * returns the cluster map for current deploy. The keys of the map are node names, the values
   * contain names of clusters (data files) available on the single node.
   *
   * @return the cluster map for current deploy
   */
  default Map<String, Set<String>> getActiveClusterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    result.put(getLocalNodeName(), getStorage().getClusterNames());
    return result;
  }

  /**
   * returns the data center map for current deploy. The keys are data center names, the values are
   * node names per data center
   *
   * @return data center map for current deploy
   */
  default Map<String, Set<String>> getActiveDataCenterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    Set<String> val = new HashSet<>();
    val.add(getLocalNodeName());
    result.put("local", val);
    return result;
  }

  /**
   * checks the cluster map and tells whether this is a sharded database (ie. a distributed DB where
   * at least two nodes contain distinct subsets of data) or not
   *
   * @return true if the database is sharded, false otherwise
   */
  default boolean isSharded() {
    return false;
  }

  /**
   * @return an endpoint for Enterprise features. Null in Community Edition
   */
  default OEnterpriseEndpoint getEnterpriseEndpoint() {
    return null;
  }

  default ODatabaseStats getStats() {
    return new ODatabaseStats();
  }

  default void resetRecordLoadStats() {
  }

  default void addRidbagPrefetchStats(long execTimeMs) {
  }

  String getType();

  /**
   * creates an interrupt timer task for this db instance (without scheduling it!)
   *
   * @return the timer task. Null if this operation is not supported for current db impl.
   */
  default TimerTask createInterruptTimerTask() {
    return null;
  }

  /**
   * Opens a database using the user and password received as arguments.
   *
   * @param iUserName     Username to login
   * @param iUserPassword Password associated to the user
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession open(final String iUserName, final String iUserPassword);

  /**
   * Creates a new database.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession create();

  /**
   * Creates new database from database backup. Only incremental backups are supported.
   *
   * @param incrementalBackupPath Path to incremental backup
   * @return he Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession create(String incrementalBackupPath);

  /**
   * Creates a new database passing initial settings.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession create(Map<GlobalConfiguration, Object> iInitialSettings);

  /**
   * Drops a database.
   *
   * @throws YTDatabaseException if database is closed. @Deprecated use instead
   *                            {@link YouTrackDB#drop}
   */
  @Deprecated
  void drop();

  /**
   * Checks if the database exists.
   *
   * @return True if already exists, otherwise false.
   */
  @Deprecated
  boolean exists();

  /**
   * Set the current status of database. deprecated since 2.2
   */
  @Deprecated
  YTDatabaseSession setStatus(STATUS iStatus);

  /**
   * Returns the total size of records contained in the cluster defined by its name.
   *
   * @param iClusterName Cluster name
   * @return Total size of records contained.
   */
  @Deprecated
  long getClusterRecordSizeByName(String iClusterName);

  /**
   * Returns the total size of records contained in the cluster defined by its id.
   *
   * @param iClusterId Cluster id
   * @return The name of searched cluster.
   */
  @Deprecated
  long getClusterRecordSizeById(int iClusterId);

  /**
   * Removes all data in the cluster with given name. As result indexes for this class will be
   * rebuilt.
   *
   * @param clusterName Name of cluster to be truncated.
   */
  void truncateCluster(String clusterName);

  /**
   * Counts all the entities in the specified cluster id.
   *
   * @param iCurrentClusterId Cluster id
   * @return Total number of entities contained in the specified cluster
   */
  long countClusterElements(int iCurrentClusterId);

  @Deprecated
  long countClusterElements(int iCurrentClusterId, boolean countTombstones);

  /**
   * Counts all the entities in the specified cluster ids.
   *
   * @param iClusterIds Array of cluster ids Cluster id
   * @return Total number of entities contained in the specified clusters
   */
  long countClusterElements(int[] iClusterIds);

  @Deprecated
  long countClusterElements(int[] iClusterIds, boolean countTombstones);

  /**
   * Sets a property value
   *
   * @param iName  Property name
   * @param iValue new value to set
   * @return The previous value if any, otherwise null
   * @deprecated use <code>YouTrackDBConfig.builder().setConfig(propertyName, propertyValue).build();
   * </code> instead if you use >=3.0 API.
   */
  @Deprecated
  Object setProperty(String iName, Object iValue);

  /**
   * Gets the property value.
   *
   * @param iName Property name
   * @return The previous value if any, otherwise null
   * @deprecated use {@link YTDatabaseSession#getConfiguration()} instead if you use >=3.0 API.
   */
  @Deprecated
  Object getProperty(String iName);

  /**
   * Returns an iterator of the property entries
   *
   * @deprecated use {@link YTDatabaseSession#getConfiguration()} instead if you use >=3.0 API.
   */
  @Deprecated
  Iterator<Entry<String, Object>> getProperties();

  /**
   * Registers a listener to the database events.
   *
   * @param iListener the listener to register
   */
  @Deprecated
  void registerListener(YTDatabaseListener iListener);

  /**
   * Unregisters a listener to the database events.
   *
   * @param iListener the listener to unregister
   */
  @Deprecated
  void unregisterListener(YTDatabaseListener iListener);

  @Deprecated
  ORecordMetadata getRecordMetadata(final YTRID rid);

  /**
   * Returns the Dictionary manual index.
   *
   * @return ODictionary instance
   * @deprecated Manual indexes are prohibited and will be removed
   */
  @Deprecated
  ODictionary<Record> getDictionary();

  void rollback(boolean force) throws YTTransactionException;

  /**
   * Execute a query against the database. If the OStorage used is remote (OStorageRemote) then the
   * command will be executed remotely and the result returned back to the calling client.
   *
   * @param iCommand Query command
   * @param iArgs    Optional parameters to bind to the query
   * @return List of POJOs
   * @deprecated use {@link #query(String, Map)} or {@link #query(String, Object...)} instead
   */
  @Deprecated
  <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs);

  /**
   * Creates a command request to run a command against the database (you have to invoke
   * .execute(parameters) to actually execute it). A command can be a SQL statement or a Procedure.
   * If the OStorage used is remote (OStorageRemote) then the command will be executed remotely and
   * the result returned back to the calling client.
   *
   * @param iCommand Command request to execute.
   * @return The same Command request received as parameter.
   * @deprecated use {@link #command(String, Map)}, {@link #command(String, Object...)},
   * {@link #execute(String, String, Map)}, {@link #execute(String, String, Object...)} instead
   */
  @Deprecated
  <RET extends OCommandRequest> RET command(OCommandRequest iCommand);

  /**
   * Returns if the Multi Version Concurrency Control is enabled or not. If enabled the version of
   * the record is checked before each update and delete against the records.
   *
   * @return true if enabled, otherwise false deprecated since 2.2
   */
  @Deprecated
  boolean isMVCC();

  /**
   * Enables or disables the Multi-Version Concurrency Control. If enabled the version of the record
   * is checked before each update and delete against the records.
   *
   * @param iValue
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain. deprecated since 2.2
   */
  @Deprecated
  YTDatabaseSession setMVCC(boolean iValue);

  /**
   * Returns the current record conflict strategy.
   */
  @Deprecated
  ORecordConflictStrategy getConflictStrategy();

  /**
   * Overrides record conflict strategy selecting the strategy by name.
   *
   * @param iStrategyName ORecordConflictStrategy strategy name
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession setConflictStrategy(String iStrategyName);

  /**
   * Overrides record conflict strategy.
   *
   * @param iResolver ORecordConflictStrategy implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  YTDatabaseSession setConflictStrategy(ORecordConflictStrategy iResolver);

  /**
   * Returns the total size of the records in the database.
   */
  long getSize();

  /**
   * Returns the default cluster id. If not specified all the new entities will be stored in the
   * default cluster.
   *
   * @return The default cluster id
   */
  int getDefaultClusterId();

  /**
   * Returns the number of clusters.
   *
   * @return Number of the clusters
   */
  int getClusters();

  /**
   * Returns true if the cluster exists, otherwise false.
   *
   * @param iClusterName Cluster name
   * @return true if the cluster exists, otherwise false
   */
  boolean existsCluster(String iClusterName);

  /**
   * Returns all the names of the clusters.
   *
   * @return Collection of cluster names.
   */
  Collection<String> getClusterNames();

  /**
   * Returns the cluster id by name.
   *
   * @param iClusterName Cluster name
   * @return The id of searched cluster.
   */
  int getClusterIdByName(String iClusterName);

  /**
   * Returns the cluster name by id.
   *
   * @param iClusterId Cluster id
   * @return The name of searched cluster.
   */
  String getClusterNameById(int iClusterId);

  /**
   * Counts all the entities in the specified cluster name.
   *
   * @param iClusterName Cluster name
   * @return Total number of entities contained in the specified cluster
   */
  long countClusterElements(String iClusterName);

  /**
   * Adds a new cluster.
   *
   * @param iClusterName Cluster name
   * @param iParameters  Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addCluster(String iClusterName, Object... iParameters);

  /**
   * Adds a new cluster.
   *
   * @param iClusterName Cluster name
   * @param iRequestedId requested id of the cluster
   * @return Cluster id
   */
  int addCluster(String iClusterName, int iRequestedId);

  /**
   * Saves an entity in the specified cluster in synchronous mode. If the entity is not dirty, then
   * the operation will be ignored. For custom entity implementations assure to set the entity as
   * dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject      The entity to save
   * @param iClusterName Name of the cluster where to save
   * @return The saved entity.
   */
  <RET extends Record> RET save(Record iObject, String iClusterName);

  OMetadataInternal getMetadata();

  void afterCommitOperations();

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
   */
  void set(ATTRIBUTES iAttribute, Object iValue);

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
}
