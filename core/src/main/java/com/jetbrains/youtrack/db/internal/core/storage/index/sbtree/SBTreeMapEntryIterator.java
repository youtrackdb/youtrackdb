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

package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 *
 */
public class SBTreeMapEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {

  private LinkedList<Map.Entry<K, V>> preFetchedValues;
  private final TreeInternal<K, V> sbTree;
  private K firstKey;

  private final int prefetchSize;

  public SBTreeMapEntryIterator(TreeInternal<K, V> sbTree) {
    this(sbTree, 8000);
  }

  private SBTreeMapEntryIterator(TreeInternal<K, V> sbTree, int prefetchSize) {
    this.sbTree = sbTree;
    this.prefetchSize = prefetchSize;

    if (sbTree.isEmpty()) {
      this.preFetchedValues = null;
      return;
    }

    this.preFetchedValues = new LinkedList<>();
    firstKey = sbTree.firstKey();

    prefetchData(true);
  }

  private void prefetchData(boolean firstTime) {
    var db = DatabaseRecordThreadLocal.instance().getIfDefined();
    var begin = System.currentTimeMillis();
    try {
      sbTree.loadEntriesMajor(
          firstKey,
          firstTime,
          true,
          entry -> {
            final var value = entry.getValue();
            final V resultValue;
            resultValue = value;

            preFetchedValues.add(
                new Map.Entry<K, V>() {
                  @Override
                  public K getKey() {
                    return entry.getKey();
                  }

                  @Override
                  public V getValue() {
                    return resultValue;
                  }

                  @Override
                  public V setValue(V v) {
                    throw new UnsupportedOperationException("setValue");
                  }
                });

            return preFetchedValues.size() <= prefetchSize;
          });

      if (preFetchedValues.isEmpty()) {
        preFetchedValues = null;
      } else {
        firstKey = preFetchedValues.getLast().getKey();
      }
    } finally {
      if (db != null) {
        db.addRidbagPrefetchStats(System.currentTimeMillis() - begin);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return preFetchedValues != null;
  }

  @Override
  public Map.Entry<K, V> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final var entry = preFetchedValues.removeFirst();
    if (preFetchedValues.isEmpty()) {
      prefetchData(false);
    }

    return entry;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
