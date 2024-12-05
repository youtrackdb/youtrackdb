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

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 *
 */
public class ODirtyManager {

  private ODirtyManager overrider;
  private Set<RecordAbstract> newRecords;
  private Set<RecordAbstract> updateRecords;

  public void setDirty(RecordAbstract record) {
    ODirtyManager real = getReal();
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

  public ODirtyManager getReal() {
    ODirtyManager real = this;
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

  public boolean isSame(ODirtyManager other) {
    return this.getReal() == other.getReal();
  }

  public void merge(ODirtyManager toMerge) {
    if (isSame(toMerge)) {
      return;
    }
    this.newRecords = mergeSet(this.newRecords, toMerge.getNewRecords());
    this.updateRecords = mergeSet(this.updateRecords, toMerge.getUpdateRecords());
    toMerge.override(this);
  }

  /**
   * Merge the two set try to use the optimum case
   *
   * @param target
   * @param source
   * @return
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

  public void track(Record pointing, YTIdentifiable pointed) {
    getReal().internalTrack(pointing, pointed);
  }

  public void unTrack(Record pointing, YTIdentifiable pointed) {
    getReal().internalUnTrack(pointing, pointed);
  }

  private void internalUnTrack(Record pointing, YTIdentifiable pointed) {
  }

  private void internalTrack(Record pointing, YTIdentifiable pointed) {
    if (pointed instanceof Record) {
      ORecordInternal.setDirtyManager((Record) pointed, this);
    }
  }

  private void override(ODirtyManager oDirtyManager) {
    ODirtyManager real = getReal();
    oDirtyManager = oDirtyManager.getReal();
    if (real == oDirtyManager) {
      return;
    }
    real.overrider = oDirtyManager;
    real.newRecords = null;
    real.updateRecords = null;
  }

  public void clearForSave() {
    ODirtyManager real = getReal();
    real.newRecords = null;
    real.updateRecords = null;
  }

  public void removeNew(Record record) {
    ODirtyManager real = getReal();
    if (real.newRecords != null) {
      real.newRecords.remove(record);
    }
  }

  public void clear() {
    clearForSave();
  }
}
