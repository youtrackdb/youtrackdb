/*
 *
 *  *  Copyright YouTrackDB
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
package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 *
 */
public class DirtyManager {

  private DirtyManager overrider;
  private Set<RecordAbstract> newRecords;
  private Set<RecordAbstract> updateRecords;

  public void setDirty(RecordAbstract record) {
    DirtyManager real = getReal();
    if (record.getIdentity().isNew() && !record.getIdentity().isTemporary()) {
      if (real.newRecords == null) {
        real.newRecords = Collections.newSetFromMap(new IdentityHashMap<>());
      }
      real.newRecords.add(record);
    } else {
      if (real.updateRecords == null) {
        real.updateRecords = Collections.newSetFromMap(new IdentityHashMap<>());
      }
      real.updateRecords.add(record);
    }
  }

  public DirtyManager getReal() {
    DirtyManager real = this;
    while (real.overrider != null) {
      real = real.overrider;
    }
    if (this.overrider != null && this.overrider != real) {
      this.overrider = real;
    }
    return real;
  }

  public Set<RecordAbstract> getNewRecords() {
    return getReal().newRecords;
  }

  public Set<RecordAbstract> getUpdateRecords() {
    return getReal().updateRecords;
  }

  public boolean isSame(DirtyManager other) {
    return this.getReal() == other.getReal();
  }

  public void merge(DirtyManager toMerge) {
    if (isSame(toMerge)) {
      return;
    }
    this.newRecords = mergeSet(this.newRecords, toMerge.getNewRecords());
    this.updateRecords = mergeSet(this.updateRecords, toMerge.getUpdateRecords());
    toMerge.override(this);
  }

  /**
   * Merge the two set try to use the optimum case
   */
  private static Set<RecordAbstract> mergeSet(
      Set<RecordAbstract> target, Set<RecordAbstract> source) {
    if (source != null) {
      if (target == null) {
        return source;
      } else {
        if (target.size() > source.size()) {
          target.addAll(source);
          return target;
        } else {
          source.addAll(target);
          return source;
        }
      }
    } else {
      return target;
    }
  }

  public void track(DBRecord pointing, Identifiable pointed) {
    getReal().internalTrack(pointing, pointed);
  }

  public void unTrack(DBRecord pointing, Identifiable pointed) {
    getReal().internalUnTrack(pointing, pointed);
  }

  private void internalUnTrack(DBRecord pointing, Identifiable pointed) {
  }

  private void internalTrack(DBRecord pointing, Identifiable pointed) {
    if (pointed instanceof DBRecord) {
      RecordInternal.setDirtyManager((DBRecord) pointed, this);
    }
  }

  private void override(DirtyManager dirtyManager) {
    DirtyManager real = getReal();
    dirtyManager = dirtyManager.getReal();
    if (real == dirtyManager) {
      return;
    }
    real.overrider = dirtyManager;
    real.newRecords = null;
    real.updateRecords = null;
  }

  public void clearForSave() {
    DirtyManager real = getReal();
    real.newRecords = null;
    real.updateRecords = null;
  }

  public void removeNew(DBRecord record) {
    DirtyManager real = getReal();
    if (real.newRecords != null) {
      real.newRecords.remove(record);
    }
  }

  public void clear() {
    clearForSave();
  }
}
