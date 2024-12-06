/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.storage.cluster;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.config.StorageClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.RecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.RawBuffer;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.ClusterBrowsePage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * Represents an offline cluster, created with the "alter cluster X status offline" command. To
 * restore the original cluster assure to have the cluster files in the right path and execute:
 * "alter cluster X status online".
 *
 * @since 2.0
 */
public class OfflineCluster implements StorageCluster {

  private final String name;
  private final int id;
  private final AbstractPaginatedStorage storageLocal;
  private volatile int binaryVersion;

  public OfflineCluster(
      final AbstractPaginatedStorage iStorage, final int iId, final String iName) {
    storageLocal = iStorage;
    id = iId;
    name = iName;
  }

  @Override
  public void configure(int iId, String iClusterName) throws IOException {
  }

  @Override
  public void configure(Storage iStorage, StorageClusterConfiguration iConfig)
      throws IOException {
    binaryVersion = iConfig.getBinaryVersion();
  }

  @Override
  public void create(AtomicOperation atomicOperation) throws IOException {
  }

  @Override
  public void open(AtomicOperation atomicOperation) throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void close(boolean flush) throws IOException {
  }

  @Override
  public void delete(AtomicOperation atomicOperation) throws IOException {
  }

  @Override
  public void setClusterName(final String name) {
    throw new OfflineClusterException(
        "Cannot set cluster name on offline cluster '" + name + "'");
  }

  @Override
  public void setRecordConflictStrategy(final String conflictStrategy) {
    throw new OfflineClusterException(
        "Cannot set record conflict strategy on offline cluster '" + name + "'");
  }

  @Override
  public void setEncryption(final String encryptionName, final String encryptionKey) {
    throw new OfflineClusterException("Cannot set encryption on offline cluster '" + name + "'");
  }

  @Override
  public String encryption() {
    return null;
  }

  @Override
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public PhysicalPosition allocatePosition(byte recordType, AtomicOperation atomicOperation)
      throws IOException {
    throw new OfflineClusterException(
        "Cannot allocate a new position on offline cluster '" + name + "'");
  }

  @Override
  public PhysicalPosition createRecord(
      byte[] content,
      int recordVersion,
      byte recordType,
      PhysicalPosition allocatedPosition,
      AtomicOperation atomicOperation) {
    throw new OfflineClusterException(
        "Cannot create a new record on offline cluster '" + name + "'");
  }

  @Override
  public boolean deleteRecord(AtomicOperation atomicOperation, long clusterPosition) {
    throw new OfflineClusterException("Cannot delete a record on offline cluster '" + name + "'");
  }

  @Override
  public void updateRecord(
      long clusterPosition,
      byte[] content,
      int recordVersion,
      byte recordType,
      AtomicOperation atomicOperation) {
    throw new OfflineClusterException("Cannot update a record on offline cluster '" + name + "'");
  }

  @Override
  public @Nonnull RawBuffer readRecord(long clusterPosition, boolean prefetchRecords) {
    throw BaseException.wrapException(
        new RecordNotFoundException(
            new RecordId(id, clusterPosition),
            "Record with rid #" + id + ":" + clusterPosition + " was not found in database"),
        new OfflineClusterException(
            "Cannot read a record from the offline cluster '" + name + "'"));
  }

  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public PhysicalPosition getPhysicalPosition(PhysicalPosition iPPosition) throws IOException {
    throw new OfflineClusterException("Cannot read a record on offline cluster '" + name + "'");
  }

  @Override
  public long getEntries() {
    return 0;
  }

  @Override
  public long getFirstPosition() throws IOException {
    return RID.CLUSTER_POS_INVALID;
  }

  @Override
  public long getLastPosition() throws IOException {
    return RID.CLUSTER_POS_INVALID;
  }

  @Override
  public long getNextPosition() throws IOException {
    return RID.CLUSTER_POS_INVALID;
  }

  @Override
  public String getFileName() {
    throw new OfflineClusterException("Cannot return filename of offline cluster '" + name + "'");
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void synch() throws IOException {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getRecordsSize() throws IOException {
    return 0;
  }

  @Override
  public String compression() {
    return null;
  }

  @Override
  public boolean isSystemCluster() {
    return false;
  }

  @Override
  public boolean exists(long clusterPosition) throws IOException {
    return false;
  }

  @Override
  public PhysicalPosition[] higherPositions(PhysicalPosition position) throws IOException {
    return CommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
  }

  @Override
  public PhysicalPosition[] ceilingPositions(PhysicalPosition position) throws IOException {
    return CommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
  }

  @Override
  public PhysicalPosition[] lowerPositions(PhysicalPosition position) throws IOException {
    return CommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
  }

  @Override
  public PhysicalPosition[] floorPositions(PhysicalPosition position) throws IOException {
    return CommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
  }

  @Override
  public RecordConflictStrategy getRecordConflictStrategy() {
    return null;
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    // do nothing, anyway there is no real data behind to lock it
  }

  @Override
  public ClusterBrowsePage nextPage(long lastPosition) {
    return null;
  }

  @Override
  public int getBinaryVersion() {
    return binaryVersion;
  }
}
