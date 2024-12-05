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
package com.orientechnologies.core.serialization.serializer.stream;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

public class OStreamSerializerRID implements OBinarySerializer<YTIdentifiable> {

  public static final OStreamSerializerRID INSTANCE = new OStreamSerializerRID();
  public static final byte ID = 16;

  public int getObjectSize(YTIdentifiable object, Object... hints) {
    return OLinkSerializer.INSTANCE.getObjectSize(object.getIdentity());
  }

  public void serialize(YTIdentifiable object, byte[] stream, int startPosition, Object... hints) {
    OLinkSerializer.INSTANCE.serialize(object.getIdentity(), stream, startPosition);
  }

  public YTRID deserialize(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.deserialize(stream, startPosition);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.getObjectSize(stream, startPosition);
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
  }

  public void serializeNativeObject(
      YTIdentifiable object, byte[] stream, int startPosition, Object... hints) {
    OLinkSerializer.INSTANCE.serializeNativeObject(object.getIdentity(), stream, startPosition);
  }

  public YTIdentifiable deserializeNativeObject(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.deserializeNativeObject(stream, startPosition);
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return OLinkSerializer.RID_SIZE;
  }

  @Override
  public YTIdentifiable preprocess(YTIdentifiable value, Object... hints) {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(
      YTIdentifiable object, ByteBuffer buffer, Object... hints) {
    OLinkSerializer.INSTANCE.serializeInByteBufferObject(object, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTIdentifiable deserializeFromByteBufferObject(ByteBuffer buffer) {
    return OLinkSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
  }

  @Override
  public YTIdentifiable deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    return OLinkSerializer.INSTANCE.deserializeFromByteBufferObject(offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return OLinkSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer);
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return OLinkSerializer.INSTANCE.getObjectSizeInByteBuffer(offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTIdentifiable deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OLinkSerializer.INSTANCE.deserializeFromByteBufferObject(buffer, walChanges, offset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OLinkSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset);
  }
}
