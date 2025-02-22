/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.jetbrains.youtrack.db.internal.spatial;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

/**
 *
 */
public class LuceneMockSpatialSerializer implements BinarySerializer<EntityImpl> {

  protected static LuceneMockSpatialSerializer INSTANCE = new LuceneMockSpatialSerializer();

  protected LuceneMockSpatialSerializer() {
  }

  @Override
  public int getObjectSize(EntityImpl object, Object... hints) {
    return 0;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return 0;
  }

  @Override
  public void serialize(EntityImpl object, byte[] stream, int startPosition, Object... hints) {
  }

  @Override
  public EntityImpl deserialize(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public byte getId() {
    return -10;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return 0;
  }

  @Override
  public void serializeNativeObject(
      EntityImpl object, byte[] stream, int startPosition, Object... hints) {
  }

  @Override
  public EntityImpl deserializeNativeObject(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return 0;
  }

  @Override
  public EntityImpl preprocess(EntityImpl value, Object... hints) {
    return null;
  }

  @Override
  public void serializeInByteBufferObject(EntityImpl object, ByteBuffer buffer, Object... hints) {
  }

  @Override
  public EntityImpl deserializeFromByteBufferObject(ByteBuffer buffer) {
    return null;
  }

  @Override
  public EntityImpl deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    return null;
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return 0;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return 0;
  }

  @Override
  public EntityImpl deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    return null;
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return 0;
  }
}
