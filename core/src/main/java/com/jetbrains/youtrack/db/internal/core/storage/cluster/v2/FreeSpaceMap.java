package com.jetbrains.youtrack.db.internal.core.storage.cluster.v2;

import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurableComponent;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;
import java.io.IOException;
import javax.annotation.Nonnull;

public final class FreeSpaceMap extends DurableComponent {

  public static final String DEF_EXTENSION = ".fsm";

  static final int NORMALIZATION_INTERVAL =
      (int) Math.floor(DurablePage.MAX_PAGE_SIZE_BYTES / 256.0);

  private long fileId;

  public FreeSpaceMap(
      @Nonnull AbstractPaginatedStorage storage,
      @Nonnull String name,
      String extension,
      String lockName) {
    super(storage, name, extension, lockName);
  }

  public boolean exists(final AtomicOperation atomicOperation) {
    return isFileExists(atomicOperation, getFullName());
  }

  public void create(final AtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    init(atomicOperation);
  }

  public void open(final AtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
  }

  private void init(final AtomicOperation atomicOperation) throws IOException {
    try (final CacheEntry firstLevelCacheEntry = addPage(atomicOperation, fileId)) {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(firstLevelCacheEntry);
      page.init();
    }
  }

  public int findFreePage(final int requiredSize) throws IOException {
    final int normalizedSize = requiredSize / NORMALIZATION_INTERVAL + 1;

    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    final int localSecondLevelPageIndex;

    try (final CacheEntry firstLevelEntry = loadPageForRead(atomicOperation, fileId, 0)) {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(firstLevelEntry);
      localSecondLevelPageIndex = page.findPage(normalizedSize);
      if (localSecondLevelPageIndex < 0) {
        return -1;
      }
    }

    final int secondLevelPageIndex = localSecondLevelPageIndex + 1;
    try (final CacheEntry leafEntry =
        loadPageForRead(atomicOperation, fileId, secondLevelPageIndex)) {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(leafEntry);
      return page.findPage(normalizedSize)
          + localSecondLevelPageIndex * FreeSpaceMapPage.CELLS_PER_PAGE;
    }
  }

  public void updatePageFreeSpace(
      final AtomicOperation atomicOperation, final int pageIndex, final int freeSpace)
      throws IOException {

    assert pageIndex >= 0;
    assert freeSpace < DurablePage.MAX_PAGE_SIZE_BYTES;

    final int normalizedSpace = freeSpace / NORMALIZATION_INTERVAL;
    final int secondLevelPageIndex = 1 + pageIndex / FreeSpaceMapPage.CELLS_PER_PAGE;

    final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

    for (int i = 0; i < secondLevelPageIndex - filledUpTo + 1; i++) {
      try (final CacheEntry cacheEntry = addPage(atomicOperation, fileId)) {
        final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
        page.init();
      }
    }

    final int maxFreeSpaceSecondLevel;
    final int localSecondLevelPageIndex = pageIndex % FreeSpaceMapPage.CELLS_PER_PAGE;
    try (final CacheEntry leafEntry =
        loadPageForWrite(atomicOperation, fileId, secondLevelPageIndex, true)) {

      final FreeSpaceMapPage page = new FreeSpaceMapPage(leafEntry);
      maxFreeSpaceSecondLevel =
          page.updatePageMaxFreeSpace(localSecondLevelPageIndex, normalizedSpace);
    }

    try (final CacheEntry firstLevelCacheEntry =
        loadPageForWrite(atomicOperation, fileId, 0, true)) {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(firstLevelCacheEntry);
      page.updatePageMaxFreeSpace(secondLevelPageIndex - 1, maxFreeSpaceSecondLevel);
    }
  }

  public void delete(AtomicOperation atomicOperation) throws IOException {
    deleteFile(atomicOperation, fileId);
  }

  void rename(final String newName) throws IOException {
    writeCache.renameFile(fileId, newName + getExtension());
    setName(newName);
  }
}
