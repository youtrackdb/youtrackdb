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
package com.jetbrains.youtrack.db.internal.core.iterator;

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import java.util.Iterator;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a
 * direction, the iterator cannot change it.
 */
public class ORecordIteratorCluster<REC extends Record> extends OIdentifiableIterator<REC> {

  private Record currentRecord;
  private boolean initialized;

  public ORecordIteratorCluster(final YTDatabaseSessionInternal iDatabase, final int iClusterId) {
    this(iDatabase, iClusterId, YTRID.CLUSTER_POS_INVALID, YTRID.CLUSTER_POS_INVALID);
  }

  protected ORecordIteratorCluster(final YTDatabaseSessionInternal database) {
    //noinspection deprecation
    super(database);
  }

  @Deprecated
  public ORecordIteratorCluster(
      final YTDatabaseSessionInternal iDatabase,
      final int iClusterId,
      final long firstClusterEntry,
      final long lastClusterEntry) {
    super(iDatabase);

    if (iClusterId == YTRID.CLUSTER_ID_INVALID) {
      throw new IllegalArgumentException("The clusterId is invalid");
    }

    checkForSystemClusters(iDatabase, new int[]{iClusterId});

    current.setClusterId(iClusterId);
    final long[] range = database.getClusterDataRange(current.getClusterId());

    if (firstClusterEntry == YTRID.CLUSTER_POS_INVALID) {
      this.firstClusterEntry = range[0];
    } else {
      this.firstClusterEntry = Math.max(firstClusterEntry, range[0]);
    }

    if (lastClusterEntry == YTRID.CLUSTER_POS_INVALID) {
      this.lastClusterEntry = range[1];
    } else {
      this.lastClusterEntry = Math.min(lastClusterEntry, range[1]);
    }

    totalAvailableRecords = database.countClusterElements(current.getClusterId());

    txEntries = iDatabase.getTransaction().getNewRecordEntriesByClusterIds(new int[]{iClusterId});

    if (txEntries != null)
    // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
    {
      for (ORecordOperation entry : txEntries) {
        switch (entry.type) {
          case ORecordOperation.CREATED:
            totalAvailableRecords++;
            break;

          case ORecordOperation.DELETED:
            totalAvailableRecords--;
            break;
        }
      }
    }
  }

  @Override
  public boolean hasPrevious() {
    initialize();
    checkDirection(false);

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
    // LIMIT REACHED
    {
      return false;
    }

    boolean thereAreRecordsToBrowse = getCurrentEntry() > firstClusterEntry;

    if (thereAreRecordsToBrowse) {
      currentRecord = readCurrentRecord(-1);
    }

    return currentRecord != null;
  }

  public boolean hasNext() {
    initialize();
    checkDirection(true);

    if (Thread.interrupted())
    // INTERRUPTED
    {
      return false;
    }

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
    // LIMIT REACHED
    {
      return false;
    }

    if (browsedRecords >= totalAvailableRecords) {
      return false;
    }

    if (!(current.getClusterPosition() < YTRID.CLUSTER_POS_INVALID)
        && getCurrentEntry() < lastClusterEntry) {
      try {
        currentRecord = readCurrentRecord(+1);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during read of record", e);
        currentRecord = null;
      }

      if (currentRecord != null) {
        return true;
      }
    }

    // CHECK IN TX IF ANY
    if (txEntries != null) {
      return txEntries.size() - (currentTxEntryPosition + 1) > 0;
    }

    return false;
  }

  /**
   * Return the element at the current position and move backward the stream to the previous
   * position available.
   *
   * @return the previous record found, otherwise the NoSuchElementException exception is thrown
   * when no more records are found.
   */
  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    initialize();
    checkDirection(false);

    if (currentRecord != null) {
      try {
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }
    }

    if (hasPrevious()) {
      try {
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }
    }

    return null;
  }

  /**
   * Return the element at the current position and move forward the stream to the next position
   * available.
   *
   * @return the next record found, otherwise the NoSuchElementException exception is thrown when no
   * more records are found.
   */
  @SuppressWarnings("unchecked")
  public REC next() {
    initialize();
    checkDirection(true);

    Record record;

    // ITERATE UNTIL THE NEXT GOOD RECORD
    while (hasNext()) {
      // FOUND
      if (currentRecord != null) {
        try {
          return (REC) currentRecord;
        } finally {
          currentRecord = null;
        }
      }

      record = getTransactionEntry();
      if (record != null) {
        return (REC) record;
      }
    }

    return null;
  }

  /**
   * Move the iterator to the begin of the range. If no range was specified move to the first record
   * of the cluster.
   *
   * @return The object itself
   */
  @Override
  public ORecordIteratorCluster<REC> begin() {
    return doBegin();
  }

  private ORecordIteratorCluster<REC> doBegin() {
    browsedRecords = 0;

    updateRangesOnLiveUpdate();
    resetCurrentPosition();

    currentRecord = readCurrentRecord(+1);

    return this;
  }

  /**
   * Move the iterator to the end of the range. If no range was specified move to the last record of
   * the cluster.
   *
   * @return The object itself
   */
  @Override
  public ORecordIteratorCluster<REC> last() {
    initialize();
    browsedRecords = 0;

    updateRangesOnLiveUpdate();
    resetCurrentPosition();

    currentRecord = readCurrentRecord(-1);

    return this;
  }

  public Iterator<REC> reversed() {
    initialize();
    this.last();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return ORecordIteratorCluster.this.hasPrevious();
      }

      @Override
      public REC next() {
        return ORecordIteratorCluster.this.previous();
      }
    };
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when
   * concurrent deletes or additions change the size of the cluster while you're browsing it.
   * Default is false.
   *
   * @param iLiveUpdated True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  @Override
  public ORecordIteratorCluster<REC> setLiveUpdated(boolean iLiveUpdated) {
    initialize();
    super.setLiveUpdated(iLiveUpdated);

    // SET THE RANGE LIMITS
    if (iLiveUpdated) {
      firstClusterEntry = 0L;
      lastClusterEntry = Long.MAX_VALUE;
    } else {
      final long[] range = database.getClusterDataRange(current.getClusterId());
      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }

    totalAvailableRecords = database.countClusterElements(current.getClusterId());

    return this;
  }

  private void updateRangesOnLiveUpdate() {
    if (liveUpdated) {
      final long[] range = database.getClusterDataRange(current.getClusterId());

      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }
  }

  private void initialize() {
    if (!initialized) {
      initialized = true;
      doBegin();
    }
  }
}
