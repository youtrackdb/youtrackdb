/*
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
 */

package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.engine.OEngineAbstract;
import com.jetbrains.youtrack.db.internal.core.exception.YTStorageException;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;

/**
 * Remote engine implementation.
 */
public class OEngineRemote extends OEngineAbstract {

  public static final String NAME = "remote";
  public static final String PREFIX = NAME + ":";

  public OEngineRemote() {
  }

  public OStorage createStorage(
      final String iURL,
      long maxWalSegSize,
      long doubleWriteLogMaxSegSize,
      int storageId,
      YouTrackDBInternal context) {
    throw new YTStorageException("deprecated");
  }

  @Override
  public void startup() {
    super.startup();
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }

  @Override
  public String getNameFromPath(String dbPath) {
    return dbPath;
  }

  public String getName() {
    return NAME;
  }
}
