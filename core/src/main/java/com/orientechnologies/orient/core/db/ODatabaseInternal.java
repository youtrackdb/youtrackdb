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

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.enterprise.OEnterpriseEndpoint;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import java.util.*;

public interface ODatabaseInternal<T> extends ODatabase<T> {

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
  void setUser(OSecurityUser user);

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
  ODatabaseInternal<?> getDatabaseOwner();

  /**
   * Internal. Sets the database owner.
   */
  ODatabaseInternal<?> setDatabaseOwner(ODatabaseInternal<?> iOwner);

  /**
   * Return the underlying database. Used in wrapper instances to know the down level ODatabase
   * instance.
   *
   * @return The underlying ODatabase implementation.
   */
  <DB extends ODatabase> DB getUnderlying();

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
  <DB extends ODatabase> DB open(final OToken iToken);

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

  default void resetRecordLoadStats() {}

  default void addRidbagPrefetchStats(long execTimeMs) {}

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
  <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword);

  /**
   * Creates a new database.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB create();

  /**
   * Creates new database from database backup. Only incremental backups are supported.
   *
   * @param incrementalBackupPath Path to incremental backup
   * @param <DB>                  Concrete database instance type.
   * @return he Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB create(String incrementalBackupPath);

  /**
   * Creates a new database passing initial settings.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB create(Map<OGlobalConfiguration, Object> iInitialSettings);

  /**
   * Drops a database.
   *
   * @throws ODatabaseException if database is closed. @Deprecated use instead
   *                            {@link OrientDB#drop}
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
  <DB extends ODatabase> DB setStatus(STATUS iStatus);

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
   * Loads the entity and return it.
   *
   * @param iObject The entity to load. If the entity was already loaded it will be reloaded and all
   *                the changes will be lost.
   */
  <RET extends T> RET load(T iObject);

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
   * @deprecated use <code>OrientDBConfig.builder().setConfig(propertyName, propertyValue).build();
   * </code> instead if you use >=3.0 API.
   */
  @Deprecated
  Object setProperty(String iName, Object iValue);

  /**
   * Gets the property value.
   *
   * @param iName Property name
   * @return The previous value if any, otherwise null
   * @deprecated use {@link ODatabase#getConfiguration()} instead if you use >=3.0 API.
   */
  @Deprecated
  Object getProperty(String iName);

  /**
   * Returns an iterator of the property entries
   *
   * @deprecated use {@link ODatabase#getConfiguration()} instead if you use >=3.0 API.
   */
  @Deprecated
  Iterator<Map.Entry<String, Object>> getProperties();

  /**
   * Registers a listener to the database events.
   *
   * @param iListener the listener to register
   */
  @Deprecated
  void registerListener(ODatabaseListener iListener);

  /**
   * Unregisters a listener to the database events.
   *
   * @param iListener the listener to unregister
   */
  @Deprecated
  void unregisterListener(ODatabaseListener iListener);

  @Deprecated
  ORecordMetadata getRecordMetadata(final ORID rid);

  /**
   * Returns the Dictionary manual index.
   *
   * @return ODictionary instance
   * @deprecated Manual indexes are prohibited and will be removed
   */
  @Deprecated
  ODictionary<T> getDictionary();

  void rollback(boolean force) throws OTransactionException;

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
  <DB extends ODatabase<?>> DB setMVCC(boolean iValue);

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
  <DB extends ODatabase<?>> DB setConflictStrategy(String iStrategyName);

  /**
   * Overrides record conflict strategy.
   *
   * @param iResolver ORecordConflictStrategy implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabase<?>> DB setConflictStrategy(ORecordConflictStrategy iResolver);

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
   * Loads a record using a fetch plan.
   *
   * @param iObject    Record to load
   * @param iFetchPlan Fetch plan used
   * @return The record received
   */
  <RET extends T> RET load(T iObject, String iFetchPlan);

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject      Record to load
   * @param iFetchPlan   Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The record received
   */
  <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Loads the entity by the Record ID using a fetch plan.
   *
   * @param iRecordId  The unique record id of the entity to load.
   * @param iFetchPlan Fetch plan used
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan);

  /**
   * Loads the entity by the Record ID using a fetch plan and specifying if the cache must be
   * ignored.
   *
   * @param iRecordId    The unique record id of the entity to load.
   * @param iFetchPlan   Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Saves an entity in the specified cluster in synchronous mode. If the entity is not dirty, then
   * the operation will be ignored. For custom entity implementations assure to set the entity as
   * dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject      The entity to save
   * @param iClusterName Name of the cluster where to save
   * @return The saved entity.
   */
  <RET extends T> RET save(T iObject, String iClusterName);

  @Override
  OMetadataInternal getMetadata();
}
