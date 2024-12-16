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
package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import java.util.Iterator;
import java.util.List;

/**
 * Interface that indicates that collection will send notifications about operations that are
 * performed on it to the listeners.
 *
 * @param <K> Value that indicates position of item inside collection.
 * @param <V> Value that is hold by collection.
 */
public interface TrackedMultiValue<K, V> {

  /**
   * Reverts all operations that were performed on collection and return original collection state.
   *
   * @param session
   * @param changeEvents List of operations that were performed on collection.
   * @return Original collection state.
   */
  Object returnOriginalState(DatabaseSessionInternal session,
      List<MultiValueChangeEvent<K, V>> changeEvents);

  Class<?> getGenericClass();

  void replace(MultiValueChangeEvent<Object, Object> event, Object newValue);

  void enableTracking(RecordElement parent);

  void disableTracking(RecordElement entity);

  boolean isModified();

  boolean isTransactionModified();

  MultiValueChangeTimeLine<Object, Object> getTimeLine();

  static <X> void nestedEnabled(Iterator<X> iterator, RecordElement parent) {
    while (iterator.hasNext()) {
      X x = iterator.next();
      if (x instanceof TrackedMultiValue) {
        ((TrackedMultiValue) x).enableTracking(parent);
      }
    }
  }

  static <X> void nestedDisable(Iterator<X> iterator, RecordElement parent) {
    while (iterator.hasNext()) {
      X x = iterator.next();
      if (x instanceof TrackedMultiValue) {
        ((TrackedMultiValue) x).disableTracking(parent);
      } else if (x instanceof EntityImpl) {
        if (((EntityImpl) x).isEmbedded()) {
          EntityInternalUtils.clearTrackData((EntityImpl) x);
          RecordInternal.unsetDirty((EntityImpl) x);
        }
      }
    }
  }

  static <X> void nestedTransactionClear(Iterator<X> iterator) {
    while (iterator.hasNext()) {
      X x = iterator.next();
      if (x instanceof TrackedMultiValue) {
        ((TrackedMultiValue) x).transactionClear();
      } else if (x instanceof EntityImpl) {
        if (((EntityImpl) x).isEmbedded()) {
          EntityInternalUtils.clearTransactionTrackData((EntityImpl) x);
        }
      }
    }
  }

  void transactionClear();

  boolean addInternal(final V e);

  MultiValueChangeTimeLine<K, V> getTransactionTimeLine();
}
