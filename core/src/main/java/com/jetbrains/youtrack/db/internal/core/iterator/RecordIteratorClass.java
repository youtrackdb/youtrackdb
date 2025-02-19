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

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a
 * direction, the iterator cannot change it. This iterator with "live updates" set is able to catch
 * updates to the cluster sizes while browsing. This is the case when concurrent clients/threads
 * insert and remove item in any cluster the iterator is browsing. If the cluster are hot removed by
 * from the database the iterator could be invalid and throw exception of cluster not found.
 */
public class RecordIteratorClass<REC extends DBRecord> extends RecordIteratorClusters<REC> {

  protected final SchemaClass targetClass;
  protected boolean polymorphic;

  public RecordIteratorClass(
      final DatabaseSessionInternal iDatabase,
      final String iClassName,
      final boolean iPolymorphic,
      boolean begin) {
    this(iDatabase, iClassName, iPolymorphic);
    if (begin) {
      begin();
    }
  }

  @Deprecated
  public RecordIteratorClass(
      final DatabaseSessionInternal iDatabase,
      final String iClassName,
      final boolean iPolymorphic) {
    super(iDatabase);

    targetClass = session.getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);
    if (targetClass == null) {
      throw new IllegalArgumentException(
          "Class '" + iClassName + "' was not found in database schema");
    }

    polymorphic = iPolymorphic;
    clusterIds = polymorphic ? targetClass.getPolymorphicClusterIds(iDatabase)
        : targetClass.getClusterIds(iDatabase);
    clusterIds = SchemaClassImpl.readableClusters(iDatabase, clusterIds,
        targetClass.getName(iDatabase));

    checkForSystemClusters(iDatabase, clusterIds);

    Arrays.sort(clusterIds);
    config();
  }

  @Override
  public REC next() {
    final Identifiable rec = super.next();
    if (rec == null) {
      return null;
    }
    return rec.getRecord(session);
  }

  @Override
  public REC previous() {
    final Identifiable rec = super.previous();
    if (rec == null) {
      return null;
    }

    return rec.getRecord(session);
  }

  public boolean isPolymorphic() {
    return polymorphic;
  }

  @Override
  public String toString() {
    return String.format(
        "RecordIteratorClass.targetClass(%s).polymorphic(%s)", targetClass, polymorphic);
  }

  @Override
  protected boolean include(final DBRecord record) {
    if (!(record instanceof EntityImpl entity)) {
      return false;
    }
    SchemaImmutableClass result;
    result = entity.getImmutableSchemaClass(session);
    return targetClass.isSuperClassOf(session,
        result);
  }

  public SchemaClass getTargetClass() {
    return targetClass;
  }

  @Override
  protected void config() {
    currentClusterIdx = 0; // START FROM THE FIRST CLUSTER

    updateClusterRange();

    totalAvailableRecords = session.countClusterElements(clusterIds);

    txEntries = session.getTransaction().getNewRecordEntriesByClass(targetClass, polymorphic);

    if (txEntries != null)
    // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
    {
      for (var entry : txEntries) {
        if (!entry.record.getIdentity().isPersistent() && entry.type != RecordOperation.DELETED) {
          totalAvailableRecords++;
        } else if (entry.type == RecordOperation.DELETED) {
          totalAvailableRecords--;
        }
      }
    }
  }
}
