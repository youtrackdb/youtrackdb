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

package com.orientechnologies.core.record.impl;

import com.orientechnologies.core.serialization.OSerializableStream;

/**
 * Extension of YTRecordBytes that handle lazy serialization and converts temporary links (record id
 * in transactions) to finals.
 *
 *
 * <p>Depecraded since v2.2
 */
@SuppressWarnings({"unchecked", "serial"})
@Deprecated
public class YTRecordBytesLazy extends YTRecordBytes {

  private OSerializableStream serializableContent;

  public YTRecordBytesLazy() {
  }

  public YTRecordBytesLazy(final OSerializableStream iSerializable) {
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
  public YTRecordBytesLazy copy() {
    final YTRecordBytesLazy c = (YTRecordBytesLazy) copyTo(
        new YTRecordBytesLazy(serializableContent));
    return c;
  }

  public OSerializableStream getSerializableContent() {
    return serializableContent;
  }

  public void recycle(final OSerializableStream iSerializableContent) {
    this.serializableContent = iSerializableContent;
  }
}
