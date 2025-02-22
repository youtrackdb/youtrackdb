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

package com.jetbrains.youtrack.db.internal.common.serialization.types;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

/**
 * Serializes and deserializes null values.
 */
public class NullSerializer implements BinarySerializer<Object> {

  public static final byte ID = 11;
  public static final NullSerializer INSTANCE = new NullSerializer();

  public int getObjectSize(final Object object, Object... hints) {
    return 0;
  }

  public void serialize(
      final Object object, final byte[] stream, final int startPosition, Object... hints) {
    // nothing to serialize
  }

  public Object deserialize(final byte[] stream, final int startPosition) {
    // nothing to deserialize
    return null;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return 0;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return 0;
  }

  public void serializeNativeObject(
      Object object, byte[] stream, int startPosition, Object... hints) {
  }

  public Object deserializeNativeObject(byte[] stream, int startPosition) {
    return null;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public Object preprocess(Object value, Object... hints) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(Object object, ByteBuffer buffer, Object... hints) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object deserializeFromByteBufferObject(ByteBuffer buffer) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return 0;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return 0;
  }
}
