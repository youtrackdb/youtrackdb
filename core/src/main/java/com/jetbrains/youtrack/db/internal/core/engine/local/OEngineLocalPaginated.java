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

package com.jetbrains.youtrack.db.internal.core.engine.local;

import com.jetbrains.youtrack.db.internal.common.collection.closabledictionary.OClosableLinkedContainer;
import com.jetbrains.youtrack.db.internal.common.directmemory.OByteBufferPool;
import com.jetbrains.youtrack.db.internal.common.directmemory.ODirectMemoryAllocator.Intention;
import com.jetbrains.youtrack.db.internal.common.directmemory.OPointer;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.io.OIOUtils;
import com.jetbrains.youtrack.db.internal.common.jnr.ONative;
import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.engine.OEngineAbstract;
import com.jetbrains.youtrack.db.internal.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import com.jetbrains.youtrack.db.internal.core.storage.cache.OReadCache;
import com.jetbrains.youtrack.db.internal.core.storage.cache.chm.AsyncReadCache;
import com.jetbrains.youtrack.db.internal.core.storage.disk.OLocalPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.fs.OFile;
import java.util.ArrayList;
import java.util.List;

/**
 * @since 28.03.13
 */
public class OEngineLocalPaginated extends OEngineAbstract {

  public static final String NAME = "plocal";

  private volatile OReadCache readCache;

  protected final OClosableLinkedContainer<Long, OFile> files =
      new OClosableLinkedContainer<>(getOpenFilesLimit());

  public OEngineLocalPaginated() {
  }

  private static int getOpenFilesLimit() {
    if (GlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger() > 0) {
      final Object[] additionalArgs =
          new Object[]{GlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger()};
      OLogManager.instance()
          .info(
              OEngineLocalPaginated.class,
              "Limit of open files for disk cache will be set to %d.",
              additionalArgs);
      return GlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger();
    }

    final int defaultLimit = 512;
    final int recommendedLimit = 256 * 1024;

    return ONative.instance().getOpenFilesLimit(true, recommendedLimit, defaultLimit);
  }

  @Override
  public void startup() {
    final String userName = System.getProperty("user.name", "unknown");
    OLogManager.instance().info(this, "System is started under an effective user : `%s`", userName);
    if (ONative.instance().isOsRoot()) {
      OLogManager.instance()
          .warn(
              this,
              "You are running under the \"root\" user privileges that introduces security risks."
                  + " Please consider to run under a user dedicated to be used to run current"
                  + " server instance.");
    }

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();
    super.startup();

    final long diskCacheSize =
        calculateReadCacheMaxMemory(
            GlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024);
    final int pageSize = GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

    if (GlobalConfiguration.DIRECT_MEMORY_PREALLOCATE.getValueAsBoolean()) {
      final int pageCount = (int) (diskCacheSize / pageSize);
      OLogManager.instance().info(this, "Allocation of " + pageCount + " pages.");

      final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
      final List<OPointer> pages = new ArrayList<>(pageCount);

      for (int i = 0; i < pageCount; i++) {
        pages.add(bufferPool.acquireDirect(true, Intention.PAGE_PRE_ALLOCATION));
      }

      for (final OPointer pointer : pages) {
        bufferPool.release(pointer);
      }

      pages.clear();
    }

    readCache = new AsyncReadCache(OByteBufferPool.instance(null), diskCacheSize, pageSize, false);
  }

  private static long calculateReadCacheMaxMemory(final long cacheSize) {
    return (long)
        (cacheSize
            * ((100 - GlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0));
  }

  /**
   * @param cacheSize Cache size in bytes.
   * @see OReadCache#changeMaximumAmountOfMemory(long)
   */
  public void changeCacheSize(final long cacheSize) {
    if (readCache != null) {
      readCache.changeMaximumAmountOfMemory(calculateReadCacheMaxMemory(cacheSize));
    }

    // otherwise memory size will be set during cache initialization.
  }

  public OStorage createStorage(
      final String dbName,
      long maxWalSegSize,
      long doubleWriteLogMaxSegSize,
      int storageId,
      YouTrackDBInternal context) {
    try {

      return new OLocalPaginatedStorage(
          dbName,
          dbName,
          storageId,
          readCache,
          files,
          maxWalSegSize,
          doubleWriteLogMaxSegSize,
          context);
    } catch (Exception e) {
      final String message =
          "Error on opening database: "
              + dbName
              + ". Current location is: "
              + new java.io.File(".").getAbsolutePath();
      OLogManager.instance().error(this, message, e);

      throw YTException.wrapException(new YTDatabaseException(message), e);
    }
  }

  public String getName() {
    return NAME;
  }

  public OReadCache getReadCache() {
    return readCache;
  }

  @Override
  public String getNameFromPath(String dbPath) {
    return OIOUtils.getRelativePathIfAny(dbPath, null);
  }

  @Override
  public void shutdown() {
    try {
      readCache.clear();
      files.clear();
    } finally {
      super.shutdown();
    }
  }
}
