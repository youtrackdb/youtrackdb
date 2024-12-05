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

package com.orientechnologies.core.storage.index.sbtree.multivalue.v2;

import com.orientechnologies.core.exception.YTDurableComponentException;

/**
 * @since 8/30/13
 */
public final class CellBTreeMultiValueException extends YTDurableComponentException {

  @SuppressWarnings("unused")
  public CellBTreeMultiValueException(final CellBTreeMultiValueException exception) {
    super(exception);
  }

  CellBTreeMultiValueException(final String message, final CellBTreeMultiValueV2 component) {
    super(message, component);
  }
}
