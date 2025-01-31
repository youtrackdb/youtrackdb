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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.NoTxRecordReadException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a
 * direction, the iterator cannot change it.
 */
public abstract class IdentifiableIterator<REC extends Identifiable>
    implements Iterator<REC>, Iterable<REC> {

  protected final DatabaseSessionInternal database;
  protected final RecordId current = new ChangeableRecordId();
  private final Storage dbStorage;
  protected boolean liveUpdated = false;
  protected long limit = -1;
  protected long browsedRecords = 0;
  protected long totalAvailableRecords;
  protected List<RecordOperation> txEntries;
  protected int currentTxEntryPosition = -1;
  protected long firstClusterEntry = 0;
  protected long lastClusterEntry = Long.MAX_VALUE;
  // REUSE IT
  private Boolean directionForward;
  private long currentEntry = RID.CLUSTER_POS_INVALID;
  private int currentEntryPosition = -1;
  private PhysicalPosition[] positionsToProcess = null;

  /**
   * Set of RIDs of records which were indicated as broken during cluster iteration. Mainly used
   * during JSON export/import procedure to fix links on broken records.
   */
  protected final Set<RID> brokenRIDs = new HashSet<>();

  /**
   * @deprecated usage of this constructor may lead to deadlocks.
   */
  @Deprecated
  public IdentifiableIterator(final DatabaseSessionInternal iDatabase) {
    database = iDatabase;

    dbStorage = database.getStorage();
    current.setClusterPosition(RID.CLUSTER_POS_INVALID); // DEFAULT = START FROM THE BEGIN
  }

  public abstract boolean hasPrevious();

  public abstract Identifiable previous();

  public abstract IdentifiableIterator<REC> begin();

  public abstract IdentifiableIterator<REC> last();

  public DBRecord current() {
    return readCurrentRecord(0);
  }

  public Set<RID> getBrokenRIDs() {
    return brokenRIDs;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  public long getCurrentEntry() {
    return currentEntry;
  }

  /**
   * Return the iterator to be used in Java5+ constructs<br>
   * <br>
   * <code>
   * for( ORecordDocument rec : database.browseCluster( "Animal" ) ){<br> ...<br> }<br>
   * </code>
   */
  public Iterator<REC> iterator() {
    return this;
  }

  /**
   * Return the current limit on browsing record. -1 means no limits (default).
   *
   * @return The limit if setted, otherwise -1
   * @see #setLimit(long)
   */
  public long getLimit() {
    return limit;
  }

  /**
   * Set the limit on browsing record. -1 means no limits. You can set the limit even while you're
   * browsing.
   *
   * @param limit The current limit on browsing record. -1 means no limits (default).
   * @see #getLimit()
   */
  public IdentifiableIterator<REC> setLimit(final long limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Return current configuration of live updates.
   *
   * @return True to activate it, otherwise false (default)
   * @see #setLiveUpdated(boolean)
   */
  public boolean isLiveUpdated() {
    return liveUpdated;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when
   * concurrent deletes or additions change the size of the cluster while you're browsing it.
   * Default is false.
   *
   * @param liveUpdated True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  public IdentifiableIterator<REC> setLiveUpdated(final boolean liveUpdated) {
    this.liveUpdated = liveUpdated;
    return this;
  }

  protected DBRecord getTransactionEntry() {
    boolean noPhysicalRecordToBrowse;

    if (current.getClusterPosition() < RID.CLUSTER_POS_INVALID) {
      noPhysicalRecordToBrowse = true;
    } else if (directionForward) {
      noPhysicalRecordToBrowse = lastClusterEntry <= currentEntry;
    } else {
      noPhysicalRecordToBrowse = currentEntry <= firstClusterEntry;
    }

    if (!noPhysicalRecordToBrowse && positionsToProcess.length == 0) {
      noPhysicalRecordToBrowse = true;
    }

    if (noPhysicalRecordToBrowse && txEntries != null) {
      // IN TX
      currentTxEntryPosition++;
      if (currentTxEntryPosition >= txEntries.size()) {
        throw new NoSuchElementException();
      } else {
        return txEntries.get(currentTxEntryPosition).record;
      }
    }
    return null;
  }

  protected void checkDirection(final boolean iForward) {
    if (directionForward == null)
    // SET THE DIRECTION
    {
      directionForward = iForward;
    } else if (directionForward != iForward) {
      throw new IterationException("Iterator cannot change direction while browsing");
    }
  }

  /**
   * Read the current record and increment the counter if the record was found.
   *
   * @return record which was read from db.
   */
  protected DBRecord readCurrentRecord(final int movement) {
    // LIMIT REACHED
    if (limit > -1 && browsedRecords >= limit) {
      return null;
    }

    final var moveResult =
        switch (movement) {
          case 1 -> nextPosition();
          case -1 -> prevPosition();
          case 0 -> checkCurrentPosition();
          default -> throw new IllegalStateException("Invalid movement value : " + movement);
        };

    if (!moveResult) {
      return null;
    }

    DBRecord record;
    try {
      record = database.load(current);
    } catch (RecordNotFoundException rne) {
      record = null;
    } catch (DatabaseException e) {
      if (Thread.interrupted() || database.isClosed())
      // THREAD INTERRUPTED: RETURN
      {
        throw e;
      }

      if (e.getCause() instanceof SecurityException) {
        throw e;
      }

      if (e instanceof NoTxRecordReadException) {
        throw e;
      }

      if (GlobalConfiguration.DB_SKIP_BROKEN_RECORDS.getValueAsBoolean()) {
        record = null;
        brokenRIDs.add(current.copy());

        LogManager.instance()
            .error(
                this, "Error on fetching record during browsing. The record has been skipped", e);
      } else {
        throw e;
      }
    }

    if (record != null) {
      browsedRecords++;
      return record;
    }

    return null;
  }

  protected boolean nextPosition() {
    if (positionsToProcess == null) {
      positionsToProcess =
          dbStorage.ceilingPhysicalPositions(database,
              current.getClusterId(), new PhysicalPosition(firstClusterEntry));
      if (positionsToProcess == null) {
        return false;
      }
    } else {
      if (currentEntry >= lastClusterEntry) {
        return false;
      }
    }

    incrementEntreePosition();
    while (positionsToProcess.length > 0 && currentEntryPosition >= positionsToProcess.length) {
      positionsToProcess =
          dbStorage.higherPhysicalPositions(database,
              current.getClusterId(), positionsToProcess[positionsToProcess.length - 1]);

      currentEntryPosition = -1;
      incrementEntreePosition();
    }

    if (positionsToProcess.length == 0) {
      return false;
    }

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry > lastClusterEntry || currentEntry == RID.CLUSTER_POS_INVALID) {
      return false;
    }

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected boolean checkCurrentPosition() {
    if (currentEntry == RID.CLUSTER_POS_INVALID
        || firstClusterEntry > currentEntry
        || lastClusterEntry < currentEntry) {
      return false;
    }

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected boolean prevPosition() {
    if (positionsToProcess == null) {
      positionsToProcess =
          dbStorage.floorPhysicalPositions(database,
              current.getClusterId(), new PhysicalPosition(lastClusterEntry));
      if (positionsToProcess == null) {
        return false;
      }

      if (positionsToProcess.length == 0) {
        return false;
      }

      currentEntryPosition = positionsToProcess.length;
    } else {
      if (currentEntry < firstClusterEntry) {
        return false;
      }
    }

    decrementEntreePosition();

    while (positionsToProcess.length > 0 && currentEntryPosition < 0) {
      positionsToProcess =
          dbStorage.lowerPhysicalPositions(database, current.getClusterId(), positionsToProcess[0]);
      currentEntryPosition = positionsToProcess.length;

      decrementEntreePosition();
    }

    if (positionsToProcess.length == 0) {
      return false;
    }

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry < firstClusterEntry) {
      return false;
    }

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected void resetCurrentPosition() {
    currentEntry = RID.CLUSTER_POS_INVALID;
    positionsToProcess = null;
    currentEntryPosition = -1;
  }

  protected long currentPosition() {
    return currentEntry;
  }

  protected static void checkForSystemClusters(
      final DatabaseSessionInternal iDatabase, final int[] iClusterIds) {
    if (iDatabase.isRemote()) {
      return;
    }

    for (var clId : iClusterIds) {
      if (iDatabase.getStorage().isSystemCluster(clId)) {
        final var dbUser = iDatabase.geCurrentUser();
        if (dbUser == null
            || dbUser.allow(iDatabase, Rule.ResourceGeneric.SYSTEM_CLUSTERS, null,
            Role.PERMISSION_READ)
            != null)
        // AUTHORIZED
        {
          break;
        }
      }
    }
  }

  private void decrementEntreePosition() {
    if (positionsToProcess.length > 0) {
      currentEntryPosition--;
    }
  }

  private void incrementEntreePosition() {
    if (positionsToProcess.length > 0) {
      currentEntryPosition++;
    }
  }
}
