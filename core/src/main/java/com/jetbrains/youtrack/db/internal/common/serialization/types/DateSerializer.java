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
import java.util.Calendar;
import java.util.Date;

/**
 * Serializer for {@link Date} type, it serializes it without time part.
 *
 * @since 20.01.12
 */
public class DateSerializer implements BinarySerializer<Date> {

  public static final byte ID = 4;
  public static final DateSerializer INSTANCE = new DateSerializer();

  public int getObjectSize(Date object, Object... hints) {
    return LongSerializer.LONG_SIZE;
  }

  public void serialize(Date object, byte[] stream, int startPosition, Object... hints) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    dateTimeSerializer.serialize(calendar.getTime(), stream, startPosition);
  }

  public Date deserialize(byte[] stream, int startPosition) {
    DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserialize(stream, startPosition);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return LongSerializer.LONG_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return LongSerializer.LONG_SIZE;
  }

  public void serializeNativeObject(
      final Date object, byte[] stream, int startPosition, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    final DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    dateTimeSerializer.serializeNativeObject(calendar.getTime(), stream, startPosition);
  }

  public Date deserializeNativeObject(byte[] stream, int startPosition) {
    DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeNativeObject(stream, startPosition);
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return LongSerializer.LONG_SIZE;
  }

  @Override
  public Date preprocess(Date value, Object... hints) {
    if (value == null) {
      return null;
    }
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(value);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    return calendar.getTime();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(Date object, ByteBuffer buffer, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    final DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    dateTimeSerializer.serializeInByteBufferObject(calendar.getTime(), buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Date deserializeFromByteBufferObject(ByteBuffer buffer) {
    final DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeFromByteBufferObject(buffer);
  }

  @Override
  public Date deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeFromByteBufferObject(offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return LongSerializer.LONG_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return LongSerializer.LONG_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Date deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final DateTimeSerializer dateTimeSerializer = DateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return LongSerializer.LONG_SIZE;
  }
}
