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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDBInternal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.util.OBackupable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations
 * are: Local, Remote and Memory.
 *
 * @see com.orientechnologies.orient.core.storage.memory.ODirectMemoryStorage
 */
public interface OStorage extends OBackupable, OStorageInfo {

  String CLUSTER_DEFAULT_NAME = "default";

  enum STATUS {
    CLOSED,
    OPEN,
    MIGRATION,
    CLOSING,
    @Deprecated
    OPENING,
  }

  void open(
      ODatabaseSessionInternal remote, String iUserName, String iUserPassword,
      final OContextConfiguration contextConfiguration);

  void create(OContextConfiguration contextConfiguration) throws IOException;

  boolean exists();

  void reload(ODatabaseSessionInternal database);

  void delete();

  void close(@Nullable ODatabaseSessionInternal session);

  void close(@Nullable ODatabaseSessionInternal database, boolean iForce);

  boolean isClosed(ODatabaseSessionInternal database);

  // CRUD OPERATIONS
  @Nonnull
  ORawBuffer readRecord(
      ODatabaseSessionInternal session, ORecordId iRid,
      boolean iIgnoreCache,
      boolean prefetchRecords,
      ORecordCallback<ORawBuffer> iCallback);

  boolean recordExists(ODatabaseSessionInternal session, ORID rid);

  ORecordMetadata getRecordMetadata(ODatabaseSessionInternal session, final ORID rid);

  boolean cleanOutRecord(
      ODatabaseSessionInternal session, ORecordId recordId, int recordVersion, int iMode,
      ORecordCallback<Boolean> callback);

  // TX OPERATIONS
  List<ORecordOperation> commit(OTransactionOptimistic iTx);

  Set<String> getClusterNames();

  Collection<? extends OCluster> getClusterInstances();

  /**
   * Add a new cluster into the storage.
   *
   * @param database
   * @param iClusterName name of the cluster
   */
  int addCluster(ODatabaseSessionInternal database, String iClusterName, Object... iParameters);

  /**
   * Add a new cluster into the storage.
   *
   * @param database
   * @param iClusterName name of the cluster
   * @param iRequestedId requested id of the cluster
   */
  int addCluster(ODatabaseSessionInternal database, String iClusterName, int iRequestedId);

  boolean dropCluster(ODatabaseSessionInternal session, String iClusterName);

  String getClusterName(ODatabaseSessionInternal database, final int clusterId);

  boolean setClusterAttribute(final int id, OCluster.ATTRIBUTES attribute, Object value);

  /**
   * Drops a cluster.
   *
   * @param database
   * @param iId      id of the cluster to delete
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(ODatabaseSessionInternal database, int iId);

  String getClusterNameById(final int clusterId);

  long getClusterRecordsSizeById(final int clusterId);

  long getClusterRecordsSizeByName(final String clusterName);

  String getClusterRecordConflictStrategy(final int clusterId);

  String getClusterEncryption(final int clusterId);

  boolean isSystemCluster(final int clusterId);

  long getLastClusterPosition(final int clusterId);

  long getClusterNextPosition(final int clusterId);

  OPaginatedCluster.RECORD_STATUS getRecordStatus(final ORID rid);

  long count(ODatabaseSessionInternal session, int iClusterId);

  long count(ODatabaseSessionInternal session, int iClusterId, boolean countTombstones);

  long count(ODatabaseSessionInternal session, int[] iClusterIds);

  long count(ODatabaseSessionInternal session, int[] iClusterIds, boolean countTombstones);

  /**
   * Returns the size of the database.
   */
  long getSize(ODatabaseSessionInternal session);

  /**
   * Returns the total number of records.
   */
  long countRecords(ODatabaseSessionInternal session);

  void setDefaultClusterId(final int defaultClusterId);

  int getClusterIdByName(String iClusterName);

  String getPhysicalClusterNameById(int iClusterId);

  boolean checkForRecordValidity(OPhysicalPosition ppos);

  String getName();

  long getVersion();

  /**
   * @return Version of product release under which storage was created.
   */
  String getCreatedAtVersion();

  void synch();

  /**
   * Execute the command request and return the result back.
   */
  Object command(ODatabaseSessionInternal database, OCommandRequestText iCommand);

  /**
   * Returns a pair of long values telling the begin and end positions of data in the requested
   * cluster. Useful to know the range of the records.
   *
   * @param session
   * @param currentClusterId Cluster id
   */
  long[] getClusterDataRange(ODatabaseSessionInternal session, int currentClusterId);

  OPhysicalPosition[] higherPhysicalPositions(ODatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  OPhysicalPosition[] lowerPhysicalPositions(ODatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  OPhysicalPosition[] ceilingPhysicalPositions(ODatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  OPhysicalPosition[] floorPhysicalPositions(ODatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  /**
   * Returns the current storage's status
   */
  STATUS getStatus();

  /**
   * Returns the storage's type.
   */
  String getType();

  OStorage getUnderlying();

  boolean isRemote();

  @Deprecated
  boolean isDistributed();

  boolean isAssigningClusterIds();

  OSBTreeCollectionManager getSBtreeCollectionManager();

  OCurrentStorageComponentsFactory getComponentsFactory();

  ORecordConflictStrategy getRecordConflictStrategy();

  void setConflictStrategy(ORecordConflictStrategy iResolver);

  /**
   * @return Backup file name
   */
  String incrementalBackup(ODatabaseSessionInternal session, String backupDirectory,
      OCallable<Void, Void> started)
      throws UnsupportedOperationException;

  boolean supportIncremental();

  void fullIncrementalBackup(OutputStream stream) throws UnsupportedOperationException;

  void restoreFromIncrementalBackup(ODatabaseSessionInternal session, String filePath);

  void restoreFullIncrementalBackup(ODatabaseSessionInternal session, InputStream stream)
      throws UnsupportedOperationException;

  /**
   * This method is called in {@link Oxygen#shutdown()} method. For most of the storages it means
   * that storage will be merely closed, but sometimes additional operations are need to be taken in
   * account.
   */
  void shutdown();

  void setSchemaRecordId(String schemaRecordId);

  void setDateFormat(String dateFormat);

  void setTimeZone(TimeZone timeZoneValue);

  void setLocaleLanguage(String locale);

  void setCharset(String charset);

  void setIndexMgrRecordId(String indexMgrRecordId);

  void setDateTimeFormat(String dateTimeFormat);

  void setLocaleCountry(String localeCountry);

  void setClusterSelection(String clusterSelection);

  void setMinimumClusters(int minimumClusters);

  void setValidation(boolean validation);

  void removeProperty(String property);

  void setProperty(String property, String value);

  void setRecordSerializer(String recordSerializer, int version);

  void clearProperties();

  int[] getClustersIds(Set<String> filterClusters);

  default boolean isIcrementalBackupRunning() {
    return false;
  }

  OxygenDBInternal getContext();
}
