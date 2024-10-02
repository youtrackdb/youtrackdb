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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.OQueryDatabaseState;
import com.orientechnologies.orient.core.db.document.RecordReader;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.OEdgeInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public interface ODatabaseDocumentInternal extends ODatabaseSession, ODatabaseInternal<ORecord> {

  String TYPE = "document";

  /**
   * Internal. Returns the factory that defines a set of components that current database should use
   * to be compatible to current version of storage. So if you open a database create with old
   * version of OrientDB it defines a components that should be used to provide backward
   * compatibility with that version of database.
   */
  OCurrentStorageComponentsFactory getStorageVersions();

  /**
   * Creates a new element instance.
   *
   * @return The new instance.
   */
  <RET extends OElement> RET newInstance(String iClassName);

  <RET extends OElement> RET newInstance();

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

  void begin(OTransactionOptimistic tx);

  void setSerializer(ORecordSerializer serializer);

  int assignAndCheckCluster(ORecord record, String iClusterName);

  void reloadUser();

  void afterReadOperations(final OIdentifiable identifiable);

  /**
   * @param identifiable
   * @return true if the record should be skipped
   */
  boolean beforeReadOperations(final OIdentifiable identifiable);

  /**
   * @param id
   * @param iClusterName
   * @return null if nothing changed the instance if has been modified or replaced
   */
  OIdentifiable beforeCreateOperations(final OIdentifiable id, String iClusterName);

  /**
   * @param id
   * @param iClusterName
   * @return null if nothing changed the instance if has been modified or replaced
   */
  OIdentifiable beforeUpdateOperations(final OIdentifiable id, String iClusterName);

  void beforeDeleteOperations(final OIdentifiable id, String iClusterName);

  void afterUpdateOperations(final OIdentifiable id);

  void afterCreateOperations(final OIdentifiable id);

  void afterDeleteOperations(final OIdentifiable id);

  ORecordHook.RESULT callbackHooks(final ORecordHook.TYPE type, final OIdentifiable id);

  <RET extends ORecord> RET executeReadRecord(
      final ORecordId rid,
      ORecordAbstract iRecord,
      final int recordVersion,
      final String fetchPlan,
      final boolean ignoreCache,
      final boolean loadTombstones,
      RecordReader recordReader);

  boolean executeExists(ORID rid);

  void setDefaultTransactionMode();

  ODatabaseDocumentInternal copy();

  void recycle(ORecord record);

  void checkIfActive();

  void callOnOpenListeners();

  void callOnCloseListeners();

  void callOnDropListeners();

  <DB extends ODatabase> DB setCustom(final String name, final Object iValue);

  void setPrefetchRecords(boolean prefetchRecords);

  boolean isPrefetchRecords();

  void checkForClusterPermissions(String name);

  default OResultSet getActiveQuery(String id) {
    throw new UnsupportedOperationException();
  }

  default Map<String, OQueryDatabaseState> getActiveQueries() {
    throw new UnsupportedOperationException();
  }

  boolean isUseLightweightEdges();

  /**
   * Creates a new lightweight edge with the specified class name, starting vertex, and ending
   * vertex.
   *
   * @param iClassName the class name of the edge
   * @param from       the starting vertex of the edge
   * @param to         the ending vertex of the edge
   * @return the new lightweight edge
   */
  OEdgeInternal newLightweightEdge(String iClassName, OVertex from, OVertex to);

  OEdgeInternal addLightweightEdge(OVertex from, OVertex to, String className);

  OEdge newRegularEdge(String iClassName, OVertex from, OVertex to);

  void setUseLightweightEdges(boolean b);

  ODatabaseDocumentInternal cleanOutRecord(ORID rid, int version);

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
   * @return an OResultSet to fetch the results of the query execution
   */
  default OResultSet queryOnNode(
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

  String getClusterName(final ORecord record);

  default OResultSet indexQuery(String indexName, String query, Object... args) {
    return command(query, args);
  }

  OView getViewFromCluster(int cluster);

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

  default void startExclusiveMetadataChange() {}

  default void endExclusiveMetadataChange() {}

  default void queryStartUsingViewCluster(int cluster) {}

  default void queryStartUsingViewIndex(String index) {}

  public void truncateClass(String name);

  public long truncateClass(String name, boolean polimorfic);

  public long truncateClusterInternal(String name);

  /**
   * Browses all the records of the specified cluster.
   *
   * @param iClusterName Cluster name to iterate
   * @return Iterator of ODocument instances
   */
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName);

  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones);

  /**
   * Browses all the records of the specified cluster of the passed record type.
   *
   * @param iClusterName Cluster name to iterate
   * @param iRecordClass The record class expected
   * @return Iterator of ODocument instances
   */
  @Deprecated
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName, Class<REC> iRecordClass);

  @Deprecated
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition);

  @Deprecated
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
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
   * @return Iterator of ODocument instances
   */
  ORecordIteratorClass<ODocument> browseClass(String iClassName);

  /**
   * Browses all the records of the specified class and if iPolymorphic is true also all the
   * subclasses. If you've a class Vehicle and Car that extends Vehicle then a
   * db.browseClass("Vehicle", true) will return all the instances of Vehicle and Car. The order of
   * the returned instance starts from record id with position 0 until the end. Base classes are
   * worked at first.
   *
   * @param iClassName   Class name to iterate
   * @param iPolymorphic Consider also the instances of the subclasses or not
   * @return Iterator of ODocument instances
   */
  ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic);

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
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabaseDocument> DB checkSecurity(String iResource, int iOperation);

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
  <DB extends ODatabaseDocument> DB setValidationEnabled(boolean iEnabled);

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
  ODatabaseDocument setRetainRecords(boolean iValue);

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
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabaseDocument> DB checkSecurity(
      String iResourceGeneric, int iOperation, Object iResourceSpecific);

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
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   */
  @Deprecated
  <DB extends ODatabaseDocument> DB checkSecurity(
      String iResourceGeneric, int iOperation, Object... iResourcesSpecific);
}
