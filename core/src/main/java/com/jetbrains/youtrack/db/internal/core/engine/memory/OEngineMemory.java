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
package com.jetbrains.youtrack.db.internal.core.engine.memory;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.io.OIOUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.engine.OEngineAbstract;
import com.jetbrains.youtrack.db.internal.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.memory.DirectMemoryStorage;

public class OEngineMemory extends OEngineAbstract {

  public static final String NAME = "memory";

  public OEngineMemory() {
  }

  public Storage createStorage(
      String url,
      long maxWalSegSize,
      long doubleWriteLogMaxSegSize,
      int storageId,
      YouTrackDBInternal context) {
    try {
      return new DirectMemoryStorage(url, url, storageId, context);
    } catch (Exception e) {
      final String message = "Error on opening in memory storage: " + url;
      LogManager.instance().error(this, message, e);

      throw YTException.wrapException(new YTDatabaseException(message), e);
    }
  }

  public String getName() {
    return NAME;
  }

  @Override
  public String getNameFromPath(String dbPath) {
    return OIOUtils.getRelativePathIfAny(dbPath, null);
  }

  @Override
  public void startup() {
    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();
    super.startup();
  }
}
