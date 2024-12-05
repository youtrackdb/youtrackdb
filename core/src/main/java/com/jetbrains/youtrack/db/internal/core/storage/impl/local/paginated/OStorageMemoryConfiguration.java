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

package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.config.OStorageConfigurationImpl;
import com.jetbrains.youtrack.db.internal.core.config.YTContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.nio.charset.StandardCharsets;

/**
 * @since 7/15/14
 */
public class OStorageMemoryConfiguration extends OStorageConfigurationImpl {

  private static final long serialVersionUID = 7001342008735208586L;

  private byte[] serializedContent;

  public OStorageMemoryConfiguration(AbstractPaginatedStorage iStorage) {
    super(iStorage, StandardCharsets.UTF_8);
  }

  @Override
  public OStorageConfigurationImpl load(final YTContextConfiguration configuration)
      throws YTSerializationException {
    lock.writeLock().lock();
    try {
      initConfiguration(configuration);

      try {
        fromStream(serializedContent, 0, serializedContent.length, streamCharset);
      } catch (Exception e) {
        throw YTException.wrapException(
            new YTSerializationException(
                "Cannot load database configuration. The database seems corrupted"),
            e);
      }
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void update() throws YTSerializationException {
    lock.writeLock().lock();
    try {
      try {
        serializedContent = toStream(streamCharset);
      } catch (Exception e) {
        throw YTException.wrapException(
            new YTSerializationException("Error on update storage configuration"), e);
      }
      if (updateListener != null) {
        updateListener.onUpdate(this);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
