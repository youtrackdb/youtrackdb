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
package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.IdentityChangeListener;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexInternal;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import java.util.IdentityHashMap;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Collects the changes to an index for a certain key
 */
public class FrontendTransactionIndexChanges implements IdentityChangeListener {

  public enum OPERATION {
    PUT,
    REMOVE,
    CLEAR
  }

  public NavigableMap<Object, FrontendTransactionIndexChangesPerKey> changesPerKey =
      new TreeMap<>(DefaultComparator.INSTANCE);

  private final IdentityHashMap<Object, FrontendTransactionIndexChangesPerKey> identityChangeQueue =
      new IdentityHashMap<>();

  public FrontendTransactionIndexChangesPerKey nullKeyChanges = new FrontendTransactionIndexChangesPerKey(
      null);

  public boolean cleared = false;

  private IndexInternal resolvedIndex = null;

  public FrontendTransactionIndexChangesPerKey getChangesPerKey(final Object key) {
    if (key == null) {
      return nullKeyChanges;
    }

    return changesPerKey.computeIfAbsent(key, FrontendTransactionIndexChangesPerKey::new);
  }

  public void setCleared() {
    changesPerKey.clear();
    nullKeyChanges.clear();

    cleared = true;
  }

  public Object getFirstKey() {
    return changesPerKey.firstKey();
  }

  public Object getLastKey() {
    return changesPerKey.lastKey();
  }

  public Object getLowerKey(Object key) {
    return changesPerKey.lowerKey(key);
  }

  public Object getHigherKey(Object key) {
    return changesPerKey.higherKey(key);
  }

  public Object[] firstAndLastKeys(
      Object from, boolean fromInclusive, Object to, boolean toInclusive) {
    final NavigableMap<Object, FrontendTransactionIndexChangesPerKey> interval;
    if (from != null && to != null) {
      interval = changesPerKey.subMap(from, fromInclusive, to, toInclusive);
    } else if (from != null) {
      interval = changesPerKey.headMap(from, fromInclusive);
    } else if (to != null) {
      interval = changesPerKey.tailMap(to, toInclusive);
    } else {
      interval = changesPerKey;
    }

    if (interval.isEmpty()) {
      return new Object[0];
    } else {
      return new Object[]{interval.firstKey(), interval.lastKey()};
    }
  }

  public IndexInternal resolveAssociatedIndex(
      String indexName, IndexManagerAbstract indexManager, DatabaseSessionInternal db) {
    if (resolvedIndex == null) {
      final Index index = indexManager.getIndex(db, indexName);
      if (index != null) {
        resolvedIndex = index.getInternal();
      }
    }

    return resolvedIndex;
  }

  public IndexInternal getAssociatedIndex() {
    return resolvedIndex;
  }

  @Override
  public void onBeforeIdentityChange(Object source) {
    var changes = changesPerKey.remove(source);
    if (changes != null) {
      assert changes.key == source;
      identityChangeQueue.put(source, changes);
    }
  }

  @Override
  public void onAfterIdentityChange(Object source) {
    var changes = identityChangeQueue.remove(source);

    if (changes != null) {
      assert changes.key == source;
      changesPerKey.put(source, changes);
    }
  }
}
