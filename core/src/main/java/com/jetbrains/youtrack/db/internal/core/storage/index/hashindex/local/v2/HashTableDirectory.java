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

package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2;

import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurableComponent;
import java.io.IOException;

/**
 * @since 5/14/14
 */
public class HashTableDirectory extends DurableComponent {

  static final int ITEM_SIZE = LongSerializer.LONG_SIZE;

  private static final int LEVEL_SIZE = LocalHashTableV2.MAX_LEVEL_SIZE;

  static final int BINARY_LEVEL_SIZE = LEVEL_SIZE * ITEM_SIZE + 3 * ByteSerializer.BYTE_SIZE;

  private long fileId;

  private final long firstEntryIndex;

  HashTableDirectory(
      final String defaultExtension,
      final String name,
      final String lockName,
      final AbstractPaginatedStorage storage) {
    super(storage, name, defaultExtension, lockName);
    this.firstEntryIndex = 0;
  }

  public void create(final AtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    init(atomicOperation);
  }

  private void init(final AtomicOperation atomicOperation) throws IOException {
    CacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true);

    if (firstEntry == null) {
      firstEntry = addPage(atomicOperation, fileId);
      assert firstEntry.getPageIndex() == 0;
    }

    try (CacheEntry entry = firstEntry) {
      final DirectoryFirstPageV2 firstPage = new DirectoryFirstPageV2(entry);

      firstPage.setTreeSize(0);
      firstPage.setTombstone(-1);
    }
  }

  public void open(final AtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
    final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

    for (int i = 0; i < filledUpTo; i++) {
      try (final CacheEntry entry = loadPageForRead(atomicOperation, fileId, i)) {
        assert entry != null;
      }
    }
  }

  public void close() throws IOException {
    readCache.closeFile(fileId, true, writeCache);
  }

  public void delete(final AtomicOperation atomicOperation) throws IOException {
    deleteFile(atomicOperation, fileId);
  }

  int addNewNode(
      final byte maxLeftChildDepth,
      final byte maxRightChildDepth,
      final byte nodeLocalDepth,
      final long[] newNode,
      final AtomicOperation atomicOperation)
      throws IOException {
    int nodeIndex;
    try (final CacheEntry firstEntry =
        loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true)) {
      final DirectoryFirstPageV2 firstPage = new DirectoryFirstPageV2(firstEntry);

      final int tombstone = firstPage.getTombstone();

      if (tombstone >= 0) {
        nodeIndex = tombstone;
      } else {
        nodeIndex = firstPage.getTreeSize();
        firstPage.setTreeSize(nodeIndex + 1);
      }

      if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
        @SuppressWarnings("UnnecessaryLocalVariable") final int localNodeIndex = nodeIndex;

        firstPage.setMaxLeftChildDepth(localNodeIndex, maxLeftChildDepth);
        firstPage.setMaxRightChildDepth(localNodeIndex, maxRightChildDepth);
        firstPage.setNodeLocalDepth(localNodeIndex, nodeLocalDepth);

        if (tombstone >= 0) {
          firstPage.setTombstone((int) firstPage.getPointer(nodeIndex, 0));
        }

        for (int i = 0; i < newNode.length; i++) {
          firstPage.setPointer(localNodeIndex, i, newNode[i]);
        }

      } else {
        final int pageIndex = nodeIndex / DirectoryPageV2.NODES_PER_PAGE;
        final int localLevel = nodeIndex % DirectoryPageV2.NODES_PER_PAGE;

        CacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
        while (cacheEntry == null || cacheEntry.getPageIndex() < pageIndex) {
          if (cacheEntry != null) {
            cacheEntry.close();
          }

          cacheEntry = addPage(atomicOperation, fileId);
        }

        try {
          final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);

          page.setMaxLeftChildDepth(localLevel, maxLeftChildDepth);
          page.setMaxRightChildDepth(localLevel, maxRightChildDepth);
          page.setNodeLocalDepth(localLevel, nodeLocalDepth);

          if (tombstone >= 0) {
            firstPage.setTombstone((int) page.getPointer(localLevel, 0));
          }

          for (int i = 0; i < newNode.length; i++) {
            page.setPointer(localLevel, i, newNode[i]);
          }

        } finally {
          cacheEntry.close();
        }
      }
    }

    return nodeIndex;
  }

  void deleteNode(final int nodeIndex, final AtomicOperation atomicOperation) throws IOException {
    try (final CacheEntry firstEntry =
        loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true)) {
      final DirectoryFirstPageV2 firstPage = new DirectoryFirstPageV2(firstEntry);
      if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
        firstPage.setPointer(nodeIndex, 0, firstPage.getTombstone());
        firstPage.setTombstone(nodeIndex);
      } else {
        final int pageIndex = nodeIndex / DirectoryPageV2.NODES_PER_PAGE;
        final int localNodeIndex = nodeIndex % DirectoryPageV2.NODES_PER_PAGE;

        try (final CacheEntry cacheEntry =
            loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
          final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);

          page.setPointer(localNodeIndex, 0, firstPage.getTombstone());
          firstPage.setTombstone(nodeIndex);
        }
      }
    }
  }

  byte getMaxLeftChildDepth(final int nodeIndex, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getMaxLeftChildDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setMaxLeftChildDepth(
      final int nodeIndex, final byte maxLeftChildDepth, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setMaxLeftChildDepth(getLocalNodeIndex(nodeIndex), maxLeftChildDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  byte getMaxRightChildDepth(final int nodeIndex, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getMaxRightChildDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setMaxRightChildDepth(
      final int nodeIndex, final byte maxRightChildDepth, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setMaxRightChildDepth(getLocalNodeIndex(nodeIndex), maxRightChildDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  byte getNodeLocalDepth(final int nodeIndex, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getNodeLocalDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setNodeLocalDepth(
      final int nodeIndex, final byte localNodeDepth, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setNodeLocalDepth(getLocalNodeIndex(nodeIndex), localNodeDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  long[] getNode(final int nodeIndex, final AtomicOperation atomicOperation) throws IOException {
    final long[] node = new long[LEVEL_SIZE];
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);

    try {
      final int localNodeIndex = getLocalNodeIndex(nodeIndex);
      for (int i = 0; i < LEVEL_SIZE; i++) {
        node[i] = page.getPointer(localNodeIndex, i);
      }
    } finally {
      releasePage(page, false, atomicOperation);
    }

    return node;
  }

  void setNode(final int nodeIndex, final long[] node, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      final int localNodeIndex = getLocalNodeIndex(nodeIndex);
      for (int i = 0; i < LEVEL_SIZE; i++) {
        page.setPointer(localNodeIndex, i, node[i]);
      }
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  long getNodePointer(final int nodeIndex, final int index, final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getPointer(getLocalNodeIndex(nodeIndex), index);
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setNodePointer(
      final int nodeIndex,
      final int index,
      final long pointer,
      final AtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setPointer(getLocalNodeIndex(nodeIndex), index, pointer);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  public void clear(final AtomicOperation atomicOperation) throws IOException {
    truncateFile(atomicOperation, fileId);

    init(atomicOperation);
  }

  public void flush() {
    writeCache.flush(fileId);
  }

  private DirectoryPageV2 loadPage(
      final int nodeIndex, final boolean exclusiveLock, final AtomicOperation atomicOperation)
      throws IOException {
    if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
      final CacheEntry cacheEntry;

      if (exclusiveLock) {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true);
      } else {
        cacheEntry = loadPageForRead(atomicOperation, fileId, firstEntryIndex);
      }

      return new DirectoryFirstPageV2(cacheEntry);
    }

    final int pageIndex = nodeIndex / DirectoryPageV2.NODES_PER_PAGE;

    final CacheEntry cacheEntry;

    if (exclusiveLock) {
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
    } else {
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
    }

    return new DirectoryPageV2(cacheEntry);
  }

  private void releasePage(
      final DirectoryPageV2 page,
      final boolean exclusiveLock,
      final AtomicOperation atomicOperation)
      throws IOException {
    final CacheEntry cacheEntry = page.getCacheEntry();

    if (exclusiveLock) {
      releasePageFromWrite(atomicOperation, cacheEntry);
    } else {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private static int getLocalNodeIndex(final int nodeIndex) {
    if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
      return nodeIndex;
    }

    return (nodeIndex - DirectoryFirstPageV2.NODES_PER_PAGE) % DirectoryPageV2.NODES_PER_PAGE;
  }
}
