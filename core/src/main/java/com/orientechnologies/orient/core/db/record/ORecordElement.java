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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.record.impl.YTDocument;

/**
 * Base interface that represents a record element.
 */
public interface ORecordElement {

  /**
   * Available record statuses.
   */
  enum STATUS {
    NOT_LOADED,
    LOADED,
    MARSHALLING,
    UNMARSHALLING
  }

  /**
   * Marks the instance as dirty. The dirty status could be propagated up if the implementation
   * supports ownership concept.
   *
   * @return The object it self. Useful to call methods in chain.
   */
  <RET> RET setDirty();

  void setDirtyNoChanged();

  /**
   * @return Returns record element which contains given one.
   */
  ORecordElement getOwner();

  default YTDocument getOwnerRecord() {
    var owner = getOwner();

    while (true) {
      if (owner instanceof YTDocument document) {
        return document;
      }
      if (owner == null) {
        return null;
      }
      owner = owner.getOwner();
    }
  }
}
