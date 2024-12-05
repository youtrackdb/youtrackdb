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
package com.jetbrains.youtrack.db.internal.core.storage;

import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.PaginatedCluster;
import com.jetbrains.youtrack.db.internal.core.storage.memory.DirectMemoryStorage;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.config.YTContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.ORecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.OCurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionOptimistic;
import com.jetbrains.youtrack.db.internal.core.util.OBackupable;
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
 * @see DirectMemoryStorage
 */
public interface Storage extends OBackupable, OStorageInfo {

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
      YTDatabaseSessionInternal remote, String iUserName, String iUserPassword,
      final YTContextConfiguration contextConfiguration);

  void create(YTContextConfiguration contextConfiguration) throws IOException;

  boolean exists();

  void reload(YTDatabaseSessionInternal database);

  void delete();

  void close(@Nullable YTDatabaseSessionInternal session);

  void close(@Nullable YTDatabaseSessionInternal database, boolean iForce);

  boolean isClosed(YTDatabaseSessionInternal database);

  // CRUD OPERATIONS
  @Nonnull
  ORawBuffer readRecord(
      YTDatabaseSessionInternal session, YTRecordId iRid,
      boolean iIgnoreCache,
      boolean prefetchRecords,
      ORecordCallback<ORawBuffer> iCallback);

  boolean recordExists(YTDatabaseSessionInternal session, YTRID rid);

  ORecordMetadata getRecordMetadata(YTDatabaseSessionInternal session, final YTRID rid);

  boolean cleanOutRecord(
      YTDatabaseSessionInternal session, YTRecordId recordId, int recordVersion, int iMode,
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
  int addCluster(YTDatabaseSessionInternal database, String iClusterName, Object... iParameters);

  /**
   * Add a new cluster into the storage.
   *
   * @param database
   * @param iClusterName name of the cluster
   * @param iRequestedId requested id of the cluster
   */
  int addCluster(YTDatabaseSessionInternal database, String iClusterName, int iRequestedId);

  boolean dropCluster(YTDatabaseSessionInternal session, String iClusterName);

  String getClusterName(YTDatabaseSessionInternal database, final int clusterId);

  boolean setClusterAttribute(final int id, OCluster.ATTRIBUTES attribute, Object value);

  /**
   * Drops a cluster.
   *
   * @param database
   * @param iId      id of the cluster to delete
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(YTDatabaseSessionInternal database, int iId);

  String getClusterNameById(final int clusterId);

  long getClusterRecordsSizeById(final int clusterId);

  long getClusterRecordsSizeByName(final String clusterName);

  String getClusterRecordConflictStrategy(final int clusterId);

  String getClusterEncryption(final int clusterId);

  boolean isSystemCluster(final int clusterId);

  long getLastClusterPosition(final int clusterId);

  long getClusterNextPosition(final int clusterId);

  PaginatedCluster.RECORD_STATUS getRecordStatus(final YTRID rid);

  long count(YTDatabaseSessionInternal session, int iClusterId);

  long count(YTDatabaseSessionInternal session, int iClusterId, boolean countTombstones);

  long count(YTDatabaseSessionInternal session, int[] iClusterIds);

  long count(YTDatabaseSessionInternal session, int[] iClusterIds, boolean countTombstones);

  /**
   * Returns the size of the database.
   */
  long getSize(YTDatabaseSessionInternal session);

  /**
   * Returns the total number of records.
   */
  long countRecords(YTDatabaseSessionInternal session);

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
  Object command(YTDatabaseSessionInternal database, CommandRequestText iCommand);

  /**
   * Returns a pair of long values telling the begin and end positions of data in the requested
   * cluster. Useful to know the range of the records.
   *
   * @param session
   * @param currentClusterId Cluster id
   */
  long[] getClusterDataRange(YTDatabaseSessionInternal session, int currentClusterId);

  OPhysicalPosition[] higherPhysicalPositions(YTDatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  OPhysicalPosition[] lowerPhysicalPositions(YTDatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  OPhysicalPosition[] ceilingPhysicalPositions(YTDatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  OPhysicalPosition[] floorPhysicalPositions(YTDatabaseSessionInternal session, int clusterId,
      OPhysicalPosition physicalPosition);

  /**
   * Returns the current storage's status
   */
  STATUS getStatus();

  /**
   * Returns the storage's type.
   */
  String getType();

  Storage getUnderlying();

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
  String incrementalBackup(YTDatabaseSessionInternal session, String backupDirectory,
      OCallable<Void, Void> started)
      throws UnsupportedOperationException;

  boolean supportIncremental();

  void fullIncrementalBackup(OutputStream stream) throws UnsupportedOperationException;

  void restoreFromIncrementalBackup(YTDatabaseSessionInternal session, String filePath);

  void restoreFullIncrementalBackup(YTDatabaseSessionInternal session, InputStream stream)
      throws UnsupportedOperationException;

  /**
   * This method is called in {@link YouTrackDBManager#shutdown()} method. For most of the storages it means
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

  YouTrackDBInternal getContext();
}
