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

package com.orientechnologies.core.index;

import com.orientechnologies.core.db.record.YTIdentifiable;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @since 4/24/14
 */
public abstract class OIndexAbstractCursor implements OIndexCursor {

  protected int prefetchSize = -1;
  private Map.Entry<Object, YTIdentifiable> nextEntry;
  private boolean firstTime = true;

  @Override
  public Set<YTIdentifiable> toValues() {
    final HashSet<YTIdentifiable> result = new HashSet<YTIdentifiable>();
    Map.Entry<Object, YTIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry.getValue());
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public Set<Map.Entry<Object, YTIdentifiable>> toEntries() {
    final HashSet<Map.Entry<Object, YTIdentifiable>> result =
        new HashSet<Map.Entry<Object, YTIdentifiable>>();

    Map.Entry<Object, YTIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry);
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public Set<Object> toKeys() {
    final HashSet<Object> result = new HashSet<Object>();

    Map.Entry<Object, YTIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry.getKey());
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public boolean hasNext() {
    if (firstTime) {
      nextEntry = nextEntry();
      firstTime = false;
    }

    return nextEntry != null;
  }

  @Override
  public YTIdentifiable next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final Map.Entry<Object, YTIdentifiable> result = nextEntry;
    nextEntry = nextEntry();

    return result.getValue();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  public int getPrefetchSize() {
    return prefetchSize;
  }

  public void setPrefetchSize(final int prefetchSize) {
    this.prefetchSize = prefetchSize;
  }
}
