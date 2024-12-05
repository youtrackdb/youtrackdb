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

package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.serialization.OSerializableStream;

/**
 * Extension of RecordBytes that handle lazy serialization and converts temporary links (record id
 * in transactions) to finals.
 *
 *
 * <p>Depecraded since v2.2
 */
@SuppressWarnings({"unchecked", "serial"})
@Deprecated
public class RecordBytesLazy extends RecordBytes {

  private OSerializableStream serializableContent;

  public RecordBytesLazy() {
  }

  public RecordBytesLazy(final OSerializableStream iSerializable) {
    this.serializableContent = iSerializable;
  }

  @Override
  public byte[] toStream() {
    if (source == null) {
      source = serializableContent.toStream();
    }
    return source;
  }

  @Override
  public RecordBytesLazy copy() {
    final RecordBytesLazy c = (RecordBytesLazy) copyTo(
        new RecordBytesLazy(serializableContent));
    return c;
  }

  public OSerializableStream getSerializableContent() {
    return serializableContent;
  }

  public void recycle(final OSerializableStream iSerializableContent) {
    this.serializableContent = iSerializableContent;
  }
}
