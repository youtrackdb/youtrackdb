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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Container that contains information about all operations that were performed on collection
 * starting from the time when it was loaded from DB.
 *
 * @param <K> Value that uniquely identifies position of element inside collection.
 * @param <V> Value that is hold by collection.
 */
public class MultiValueChangeTimeLine<K, V> {

  private final List<MultiValueChangeEvent<K, V>> multiValueChangeEvents =
      new ArrayList<MultiValueChangeEvent<K, V>>();

  /**
   * @return <code>List</code> of all operations that were performed on collection starting from the
   * time when it was loaded from DB.
   */
  public List<MultiValueChangeEvent<K, V>> getMultiValueChangeEvents() {
    return Collections.unmodifiableList(multiValueChangeEvents);
  }

  /**
   * Add new operation that was performed on collection to collection history.
   *
   * @param changeEvent Description of operation that was performed on collection.
   */
  public void addCollectionChangeEvent(MultiValueChangeEvent<K, V> changeEvent) {
    multiValueChangeEvents.add(changeEvent);
  }
}
