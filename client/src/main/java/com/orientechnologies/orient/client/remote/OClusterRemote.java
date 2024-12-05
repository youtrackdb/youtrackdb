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
package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.config.OStorageClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.ORecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.storage.OCluster;
import com.jetbrains.youtrack.db.internal.core.storage.OPhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.ORawBuffer;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OClusterBrowsePage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * Remote cluster implementation
 */
public class OClusterRemote implements OCluster {

  private String name;
  private int id;

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.storage.OCluster#configure(com.orientechnologies.core.storage.Storage, int,
   * java.lang.String, java.lang.String, int, java.lang.Object[])
   */
  public void configure(int iId, String iClusterName) {
    id = iId;
    name = iClusterName;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.storage.OCluster#configure(com.orientechnologies.core.storage.Storage,
   * com.orientechnologies.core.config.OStorageClusterConfiguration)
   */
  public void configure(Storage iStorage, OStorageClusterConfiguration iConfig) {
    id = iConfig.getId();
    name = iConfig.getName();
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.storage.OCluster#create(int)
   */
  public void create(OAtomicOperation atomicOperation) {
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.core.storage.OCluster#open()
   */
  public void open(OAtomicOperation atomicOperation) {
  }

  public void close() {
  }

  @Override
  public void close(boolean flush) {
  }

  @Override
  public OPhysicalPosition allocatePosition(byte recordType, OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("allocatePosition");
  }

  @Override
  public OPhysicalPosition createRecord(
      byte[] content,
      int recordVersion,
      byte recordType,
      OPhysicalPosition allocatedPosition,
      OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("createRecord");
  }

  @Override
  public boolean deleteRecord(OAtomicOperation atomicOperation, long clusterPosition) {
    throw new UnsupportedOperationException("deleteRecord");
  }

  @Override
  public void updateRecord(
      long clusterPosition,
      byte[] content,
      int recordVersion,
      byte recordType,
      OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("updateRecord");
  }

  @Override
  public @Nonnull ORawBuffer readRecord(long clusterPosition, boolean prefetchRecords) {
    throw new UnsupportedOperationException("readRecord");
  }

  @Override
  public void setClusterName(final String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRecordConflictStrategy(final String conflictStrategy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEncryption(final String encryptionName, final String encryptionKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("exists");
  }

  public void delete(OAtomicOperation atomicOperation) {
  }

  public Object set(ATTRIBUTES iAttribute, Object iValue) {
    return null;
  }

  @Override
  public String encryption() {
    throw new UnsupportedOperationException("encryption");
  }

  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) {
    return null;
  }

  public long getEntries() {
    return 0;
  }

  @Override
  public long getTombstonesCount() {
    throw new UnsupportedOperationException("getTombstonesCount()");
  }

  @Override
  public long getFirstPosition() {
    return 0;
  }

  @Override
  public long getLastPosition() {
    return 0;
  }

  @Override
  public long getNextPosition() {
    return 0;
  }

  @Override
  public String getFileName() {
    throw new UnsupportedOperationException("getFileName()");
  }

  public int getId() {
    return id;
  }

  public void synch() {
  }

  public String getName() {
    return name;
  }

  public long getRecordsSize() {
    throw new UnsupportedOperationException("getRecordsSize()");
  }

  @Override
  public boolean isSystemCluster() {
    return false;
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("higherPositions()");
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("lowerPositions()");
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("ceilingPositions()");
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("floorPositions()");
  }

  @Override
  public boolean exists(long clusterPosition) throws IOException {
    throw new UnsupportedOperationException("exists()");
  }

  @Override
  public String compression() {
    throw new UnsupportedOperationException("compression()");
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return null;
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    throw new UnsupportedOperationException("remote cluster doesn't support atomic locking");
  }

  @Override
  public OClusterBrowsePage nextPage(long lastPosition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBinaryVersion() {
    throw new UnsupportedOperationException(
        "Operation is not supported for given cluster implementation");
  }
}
