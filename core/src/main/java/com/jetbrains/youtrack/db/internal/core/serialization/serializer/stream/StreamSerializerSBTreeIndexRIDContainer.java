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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream;

import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer.RID_SIZE;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BooleanSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.BonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.IndexRIDContainer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.IndexRIDContainerSBTree;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class StreamSerializerSBTreeIndexRIDContainer
    implements BinarySerializer<IndexRIDContainer> {

  public static final StreamSerializerSBTreeIndexRIDContainer INSTANCE =
      new StreamSerializerSBTreeIndexRIDContainer();

  public static final byte ID = 21;
  public static final int FILE_ID_OFFSET = 0;
  public static final int EMBEDDED_OFFSET = FILE_ID_OFFSET + LongSerializer.LONG_SIZE;
  public static final int DURABLE_OFFSET = EMBEDDED_OFFSET + BooleanSerializer.BOOLEAN_SIZE;
  public static final int SBTREE_ROOTINDEX_OFFSET =
      DURABLE_OFFSET + BooleanSerializer.BOOLEAN_SIZE;
  public static final int SBTREE_ROOTOFFSET_OFFSET =
      SBTREE_ROOTINDEX_OFFSET + LongSerializer.LONG_SIZE;

  public static final int EMBEDDED_SIZE_OFFSET = DURABLE_OFFSET + BooleanSerializer.BOOLEAN_SIZE;
  public static final int EMBEDDED_VALUES_OFFSET =
      EMBEDDED_SIZE_OFFSET + IntegerSerializer.INT_SIZE;

  public static final LongSerializer LONG_SERIALIZER = LongSerializer.INSTANCE;
  public static final BooleanSerializer BOOLEAN_SERIALIZER = BooleanSerializer.INSTANCE;
  public static final IntegerSerializer INT_SERIALIZER = IntegerSerializer.INSTANCE;
  public static final int SBTREE_CONTAINER_SIZE =
      2 * BooleanSerializer.BOOLEAN_SIZE
          + 2 * LongSerializer.LONG_SIZE
          + IntegerSerializer.INT_SIZE;
  public static final LinkSerializer LINK_SERIALIZER = LinkSerializer.INSTANCE;

  @Override
  public int getObjectSize(IndexRIDContainer object, Object... hints) {
    if (object.isEmbedded()) {
      return embeddedObjectSerializedSize(object.size());
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public void serialize(
      IndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public IndexRIDContainer deserialize(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    throw new UnsupportedOperationException("Length is not fixed");
  }

  @Override
  public void serializeNativeObject(
      IndexRIDContainer object, byte[] stream, int offset, Object... hints) {
    LONG_SERIALIZER.serializeNative(object.getFileId(), stream, offset + FILE_ID_OFFSET);

    final boolean embedded = object.isEmbedded();
    final boolean durable = object.isDurableNonTxMode();

    BOOLEAN_SERIALIZER.serializeNative(embedded, stream, offset + EMBEDDED_OFFSET);
    BOOLEAN_SERIALIZER.serializeNative(durable, stream, offset + DURABLE_OFFSET);

    if (embedded) {
      INT_SERIALIZER.serializeNative(object.size(), stream, offset + EMBEDDED_SIZE_OFFSET);

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (Identifiable ids : object) {
        LINK_SERIALIZER.serializeNativeObject(ids, stream, p);
        p += RID_SIZE;
      }
    } else {
      final IndexRIDContainerSBTree underlying = (IndexRIDContainerSBTree) object.getUnderlying();
      final BonsaiBucketPointer rootPointer = underlying.getRootPointer();
      LONG_SERIALIZER.serializeNative(
          rootPointer.getPageIndex(), stream, offset + SBTREE_ROOTINDEX_OFFSET);
      INT_SERIALIZER.serializeNative(
          rootPointer.getPageOffset(), stream, offset + SBTREE_ROOTOFFSET_OFFSET);
    }
  }

  @Override
  public IndexRIDContainer deserializeNativeObject(byte[] stream, int offset) {
    final long fileId = LONG_SERIALIZER.deserializeNative(stream, offset + FILE_ID_OFFSET);
    final boolean durable = BOOLEAN_SERIALIZER.deserializeNative(stream, offset + DURABLE_OFFSET);

    if (BOOLEAN_SERIALIZER.deserializeNative(stream, offset + EMBEDDED_OFFSET)) {
      final int size = INT_SERIALIZER.deserializeNative(stream, offset + EMBEDDED_SIZE_OFFSET);
      final Set<Identifiable> underlying = new HashSet<>(Math.max((int) (size / .75f) + 1, 16));

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeNativeObject(stream, p));
        p += RID_SIZE;
      }

      return new IndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex =
          LONG_SERIALIZER.deserializeNative(stream, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset =
          INT_SERIALIZER.deserializeNative(stream, offset + SBTREE_ROOTOFFSET_OFFSET);
      final BonsaiBucketPointer rootPointer = new BonsaiBucketPointer(pageIndex, pageOffset);
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      final IndexRIDContainerSBTree underlying =
          new IndexRIDContainerSBTree(
              fileId, rootPointer, (AbstractPaginatedStorage) db.getStorage());
      return new IndexRIDContainer(fileId, underlying, durable);
    }
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public IndexRIDContainer preprocess(IndexRIDContainer value, Object... hints) {
    return value;
  }

  private int embeddedObjectSerializedSize(int size) {
    return LongSerializer.LONG_SIZE
        + 2 * BooleanSerializer.BOOLEAN_SIZE
        + IntegerSerializer.INT_SIZE
        + size * RID_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(
      IndexRIDContainer object, ByteBuffer buffer, Object... hints) {
    buffer.putLong(object.getFileId());

    final boolean embedded = object.isEmbedded();
    final boolean durable = object.isDurableNonTxMode();

    buffer.put((byte) (embedded ? 1 : 0));
    buffer.put((byte) (durable ? 1 : 0));

    if (embedded) {
      buffer.putInt(object.size());

      for (Identifiable ids : object) {
        LINK_SERIALIZER.serializeInByteBufferObject(ids, buffer);
      }
    } else {
      final IndexRIDContainerSBTree underlying = (IndexRIDContainerSBTree) object.getUnderlying();
      final BonsaiBucketPointer rootPointer = underlying.getRootPointer();

      buffer.putLong(rootPointer.getPageIndex());
      buffer.putInt(rootPointer.getPageOffset());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexRIDContainer deserializeFromByteBufferObject(ByteBuffer buffer) {
    final long fileId = buffer.getLong();
    final boolean embedded = buffer.get() > 0;
    final boolean durable = buffer.get() > 0;

    if (embedded) {
      final int size = buffer.getInt();
      final Set<Identifiable> underlying = new HashSet<>(Math.max((int) (size / .75f) + 1, 16));

      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeFromByteBufferObject(buffer));
      }

      return new IndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex = buffer.getLong();
      final int pageOffset = buffer.getInt();

      final BonsaiBucketPointer rootPointer = new BonsaiBucketPointer(pageIndex, pageOffset);
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      final IndexRIDContainerSBTree underlying =
          new IndexRIDContainerSBTree(
              fileId, rootPointer, (AbstractPaginatedStorage) db.getStorage());
      return new IndexRIDContainer(fileId, underlying, durable);
    }
  }

  @Override
  public IndexRIDContainer deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final long fileId = buffer.getLong(offset);
    offset += Long.BYTES;

    final boolean embedded = buffer.get(offset) > 0;
    offset++;

    final boolean durable = buffer.get(offset) > 0;
    offset++;

    if (embedded) {
      final int size = buffer.getInt(offset);
      offset += Integer.BYTES;

      final Set<Identifiable> underlying = new HashSet<>(Math.max((int) (size / .75f) + 1, 16));

      for (int i = 0; i < size; i++) {
        var delta = LINK_SERIALIZER.getObjectSizeInByteBuffer(offset, buffer);
        underlying.add(LINK_SERIALIZER.deserializeFromByteBufferObject(offset, buffer));
        offset += delta;
      }

      return new IndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex = buffer.getLong(offset);
      offset += Long.BYTES;

      final int pageOffset = buffer.getInt(offset);

      final BonsaiBucketPointer rootPointer = new BonsaiBucketPointer(pageIndex, pageOffset);
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      final IndexRIDContainerSBTree underlying =
          new IndexRIDContainerSBTree(
              fileId, rootPointer, (AbstractPaginatedStorage) db.getStorage());
      return new IndexRIDContainer(fileId, underlying, durable);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    final int offset = buffer.position();

    if (buffer.get(offset + EMBEDDED_OFFSET) > 0) {
      return embeddedObjectSerializedSize(buffer.getInt(offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    if (buffer.get(offset + EMBEDDED_OFFSET) > 0) {
      return embeddedObjectSerializedSize(buffer.getInt(offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexRIDContainer deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final long fileId = walChanges.getLongValue(buffer, offset + FILE_ID_OFFSET);
    final boolean durable = walChanges.getByteValue(buffer, offset + DURABLE_OFFSET) > 0;

    if (walChanges.getByteValue(buffer, offset + EMBEDDED_OFFSET) > 0) {
      final int size = walChanges.getIntValue(buffer, offset + EMBEDDED_SIZE_OFFSET);
      final Set<Identifiable> underlying = new HashSet<>(Math.max((int) (size / .75f) + 1, 16));

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeFromByteBufferObject(buffer, walChanges, p));
        p += RID_SIZE;
      }

      return new IndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex = walChanges.getLongValue(buffer, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset = walChanges.getIntValue(buffer, offset + SBTREE_ROOTOFFSET_OFFSET);
      final BonsaiBucketPointer rootPointer = new BonsaiBucketPointer(pageIndex, pageOffset);
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      final IndexRIDContainerSBTree underlying =
          new IndexRIDContainerSBTree(
              fileId, rootPointer, (AbstractPaginatedStorage) db.getStorage());
      return new IndexRIDContainer(fileId, underlying, durable);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    if (walChanges.getByteValue(buffer, offset + EMBEDDED_OFFSET) > 0) {
      return embeddedObjectSerializedSize(
          walChanges.getIntValue(buffer, offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }
}
