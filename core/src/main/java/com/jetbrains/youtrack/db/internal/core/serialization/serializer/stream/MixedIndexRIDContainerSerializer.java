package com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.CompactedLinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.BonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.IndexRIDContainerSBTree;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.MixedIndexRIDContainer;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class MixedIndexRIDContainerSerializer
    implements BinarySerializer<MixedIndexRIDContainer> {

  public static final MixedIndexRIDContainerSerializer INSTANCE =
      new MixedIndexRIDContainerSerializer();

  public static final byte ID = 23;

  @Override
  public int getObjectSize(MixedIndexRIDContainer object, Object... hints) {
    int size =
        LongSerializer.LONG_SIZE
            + IntegerSerializer.INT_SIZE
            + IntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += IntegerSerializer.INT_SIZE + LongSerializer.LONG_SIZE; // root offset and page index

    final Set<RID> embedded = object.getEmbeddedSet();
    for (RID orid : embedded) {
      size += CompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    return size;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return IntegerSerializer.INSTANCE.deserialize(stream, startPosition);
  }

  @Override
  public void serialize(
      MixedIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    int size =
        LongSerializer.LONG_SIZE
            + IntegerSerializer.INT_SIZE
            + IntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += IntegerSerializer.INT_SIZE + LongSerializer.LONG_SIZE; // root offset and page index

    final Set<RID> embedded = object.getEmbeddedSet();
    for (RID orid : embedded) {
      size += CompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    IntegerSerializer.INSTANCE.serialize(size, stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    LongSerializer.INSTANCE.serialize(object.getFileId(), stream, startPosition);
    startPosition += LongSerializer.LONG_SIZE;

    IntegerSerializer.INSTANCE.serialize(embedded.size(), stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    for (RID orid : embedded) {
      CompactedLinkSerializer.INSTANCE.serialize(orid, stream, startPosition);
      startPosition += CompactedLinkSerializer.INSTANCE.getObjectSize(stream, startPosition);
    }

    final IndexRIDContainerSBTree tree = object.getTree();
    if (tree == null) {
      LongSerializer.INSTANCE.serialize(-1L, stream, startPosition);
      startPosition += LongSerializer.LONG_SIZE;

      IntegerSerializer.INSTANCE.serialize(-1, stream, startPosition);
    } else {
      final BonsaiBucketPointer rootPointer = tree.getRootPointer();
      LongSerializer.INSTANCE.serialize(rootPointer.getPageIndex(), stream, startPosition);
      startPosition += LongSerializer.LONG_SIZE;

      IntegerSerializer.INSTANCE.serialize(rootPointer.getPageOffset(), stream, startPosition);
    }
  }

  @Override
  public MixedIndexRIDContainer deserialize(byte[] stream, int startPosition) {
    startPosition += IntegerSerializer.INT_SIZE;

    final long fileId = LongSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += LongSerializer.LONG_SIZE;

    final int embeddedSize = IntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    final Set<RID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final RID orid =
          CompactedLinkSerializer.INSTANCE.deserialize(stream, startPosition).getIdentity();
      startPosition += CompactedLinkSerializer.INSTANCE.getObjectSize(stream, startPosition);
      hashSet.add(orid);
    }

    final long pageIndex = LongSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += LongSerializer.LONG_SIZE;

    final int offset = IntegerSerializer.INSTANCE.deserialize(stream, startPosition);

    final IndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      tree =
          new IndexRIDContainerSBTree(
              fileId,
              new BonsaiBucketPointer(pageIndex, offset),
              (AbstractPaginatedStorage) db.getStorage());
    }

    return new MixedIndexRIDContainer(fileId, hashSet, tree);
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
    return 0;
  }

  @Override
  public void serializeNativeObject(
      MixedIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    int size =
        LongSerializer.LONG_SIZE
            + IntegerSerializer.INT_SIZE
            + IntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += IntegerSerializer.INT_SIZE + LongSerializer.LONG_SIZE; // root offset and page index

    final Set<RID> embedded = object.getEmbeddedSet();
    for (RID orid : embedded) {
      size += CompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    IntegerSerializer.INSTANCE.serializeNative(size, stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    LongSerializer.INSTANCE.serializeNative(object.getFileId(), stream, startPosition);
    startPosition += LongSerializer.LONG_SIZE;

    IntegerSerializer.INSTANCE.serializeNative(embedded.size(), stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    for (RID orid : embedded) {
      CompactedLinkSerializer.INSTANCE.serializeNativeObject(orid, stream, startPosition);
      startPosition += CompactedLinkSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
    }

    final IndexRIDContainerSBTree tree = object.getTree();
    if (tree == null) {
      LongSerializer.INSTANCE.serializeNative(-1L, stream, startPosition);
      startPosition += LongSerializer.LONG_SIZE;

      IntegerSerializer.INSTANCE.serializeNative(-1, stream, startPosition);
    } else {
      final BonsaiBucketPointer rootPointer = tree.getRootPointer();
      LongSerializer.INSTANCE.serializeNative(rootPointer.getPageIndex(), stream, startPosition);
      startPosition += LongSerializer.LONG_SIZE;

      IntegerSerializer.INSTANCE.serializeNative(
          rootPointer.getPageOffset(), stream, startPosition);
    }
  }

  @Override
  public MixedIndexRIDContainer deserializeNativeObject(byte[] stream, int startPosition) {
    startPosition += IntegerSerializer.INT_SIZE;

    final long fileId = LongSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += LongSerializer.LONG_SIZE;

    final int embeddedSize = IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    final Set<RID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final RID orid =
          CompactedLinkSerializer.INSTANCE
              .deserializeNativeObject(stream, startPosition)
              .getIdentity();
      startPosition += CompactedLinkSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
      hashSet.add(orid);
    }

    final long pageIndex = LongSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += LongSerializer.LONG_SIZE;

    final int offset = IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);

    final IndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      tree =
          new IndexRIDContainerSBTree(
              fileId,
              new BonsaiBucketPointer(pageIndex, offset),
              (AbstractPaginatedStorage) db.getStorage());
    }

    return new MixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  @Override
  public MixedIndexRIDContainer preprocess(MixedIndexRIDContainer value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(
      MixedIndexRIDContainer object, ByteBuffer buffer, Object... hints) {
    int size =
        LongSerializer.LONG_SIZE
            + IntegerSerializer.INT_SIZE
            + IntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += IntegerSerializer.INT_SIZE + LongSerializer.LONG_SIZE; // root offset and page index

    final Set<RID> embedded = object.getEmbeddedSet();
    for (RID orid : embedded) {
      size += CompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    buffer.putInt(size);
    buffer.putLong(object.getFileId());
    buffer.putInt(embedded.size());

    for (RID orid : embedded) {
      CompactedLinkSerializer.INSTANCE.serializeInByteBufferObject(orid, buffer);
    }

    final IndexRIDContainerSBTree tree = object.getTree();
    if (tree == null) {
      buffer.putLong(-1);
      buffer.putInt(-1);
    } else {
      final BonsaiBucketPointer rootPointer = tree.getRootPointer();
      buffer.putLong(rootPointer.getPageIndex());
      buffer.putInt(rootPointer.getPageOffset());
    }
  }

  @Override
  public MixedIndexRIDContainer deserializeFromByteBufferObject(ByteBuffer buffer) {
    buffer.position(buffer.position() + IntegerSerializer.INT_SIZE);

    final long fileId = buffer.getLong();
    final int embeddedSize = buffer.getInt();

    final Set<RID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final RID orid =
          CompactedLinkSerializer.INSTANCE.deserializeFromByteBufferObject(buffer).getIdentity();
      hashSet.add(orid);
    }

    final long pageIndex = buffer.getLong();
    final int offset = buffer.getInt();

    final IndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      tree =
          new IndexRIDContainerSBTree(
              fileId,
              new BonsaiBucketPointer(pageIndex, offset),
              (AbstractPaginatedStorage) db.getStorage());
    }

    return new MixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public MixedIndexRIDContainer deserializeFromByteBufferObject(
      final int bufferOffset, final ByteBuffer buffer) {
    var currentPosition = bufferOffset + Integer.BYTES;
    final long fileId = buffer.getLong(currentPosition);
    currentPosition += Long.BYTES;

    final int embeddedSize = buffer.getInt(currentPosition);
    currentPosition += Integer.BYTES;

    final Set<RID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      var delta =
          CompactedLinkSerializer.INSTANCE.getObjectSizeInByteBuffer(currentPosition, buffer);
      final RID orid =
          CompactedLinkSerializer.INSTANCE
              .deserializeFromByteBufferObject(currentPosition, buffer)
              .getIdentity();
      currentPosition += delta;

      hashSet.add(orid);
    }

    final long pageIndex = buffer.getLong(currentPosition);
    currentPosition += Long.BYTES;

    final int offset = buffer.getInt(currentPosition);
    final IndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      tree =
          new IndexRIDContainerSBTree(
              fileId,
              new BonsaiBucketPointer(pageIndex, offset),
              (AbstractPaginatedStorage) db.getStorage());
    }

    return new MixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return buffer.getInt(offset);
  }

  @Override
  public MixedIndexRIDContainer deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    offset += IntegerSerializer.INT_SIZE;

    final long fileId = walChanges.getLongValue(buffer, offset);
    offset += LongSerializer.LONG_SIZE;

    final int embeddedSize = walChanges.getIntValue(buffer, offset);
    offset += IntegerSerializer.INT_SIZE;

    final Set<RID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final RID orid =
          CompactedLinkSerializer.INSTANCE
              .deserializeFromByteBufferObject(buffer, walChanges, offset)
              .getIdentity();
      offset +=
          CompactedLinkSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset);
      hashSet.add(orid);
    }

    final long pageIndex = walChanges.getLongValue(buffer, offset);
    offset += LongSerializer.LONG_SIZE;

    final int pageOffset = walChanges.getIntValue(buffer, offset);

    final IndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
      tree =
          new IndexRIDContainerSBTree(
              fileId,
              new BonsaiBucketPointer(pageIndex, pageOffset),
              (AbstractPaginatedStorage) db.getStorage());
    }

    return new MixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }
}
