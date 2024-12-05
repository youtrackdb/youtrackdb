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
package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local;

import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.exception.YTConfigurationException;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDictionary;
import com.jetbrains.youtrack.db.internal.core.index.OIndexFactory;
import com.jetbrains.youtrack.db.internal.core.index.OIndexInternal;
import com.jetbrains.youtrack.db.internal.core.index.OIndexMetadata;
import com.jetbrains.youtrack.db.internal.core.index.OIndexNotUnique;
import com.jetbrains.youtrack.db.internal.core.index.OIndexUnique;
import com.jetbrains.youtrack.db.internal.core.index.YTIndexException;
import com.jetbrains.youtrack.db.internal.core.index.engine.OBaseIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.OIndexEngine;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.index.engine.OHashTableIndexEngine;
import com.jetbrains.youtrack.db.internal.core.storage.index.engine.ORemoteIndexEngine;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public final class OHashIndexFactory implements OIndexFactory {

  private static final Set<String> TYPES;
  public static final String HASH_INDEX_ALGORITHM = "HASH_INDEX";
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>(4);
    types.add(YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());
    types.add(YTClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString());
    types.add(YTClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>(1);
    algorithms.add(HASH_INDEX_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  /**
   * Index types :
   *
   * <ul>
   *   <li>UNIQUE
   *   <li>NOTUNIQUE
   *   <li>FULLTEXT
   *   <li>DICTIONARY
   * </ul>
   */
  public Set<String> getTypes() {
    return TYPES;
  }

  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public OIndexInternal createIndex(final Storage storage, final OIndexMetadata im)
      throws YTConfigurationException {
    int version = im.getVersion();
    final String indexType = im.getType();
    final String algorithm = im.getAlgorithm();

    if (version < 0) {
      version = getLastVersion(algorithm);
      im.setVersion(version);
    }

    if (YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString().equals(indexType)) {
      return new OIndexUnique(im, storage);
    } else if (YTClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(indexType)) {
      return new OIndexNotUnique(im, storage);
    } else if (YTClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString().equals(indexType)) {
      return new OIndexDictionary(im, storage);
    }

    throw new YTConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    return OHashTableIndexEngine.VERSION;
  }

  @Override
  public OBaseIndexEngine createIndexEngine(Storage storage, IndexEngineData data) {
    final OIndexEngine indexEngine;

    final String storageType = storage.getType();
    AbstractPaginatedStorage realStorage = (AbstractPaginatedStorage) storage;
    switch (storageType) {
      case "memory":
      case "plocal":
        indexEngine =
            new OHashTableIndexEngine(
                data.getName(), data.getIndexId(), realStorage, data.getVersion());
        break;
      case "distributed":
        // DISTRIBUTED CASE: HANDLE IT AS FOR LOCAL
        indexEngine =
            new OHashTableIndexEngine(
                data.getName(), data.getIndexId(), realStorage, data.getVersion());
        break;
      case "remote":
        indexEngine = new ORemoteIndexEngine(data.getIndexId(), data.getName());
        break;
      default:
        throw new YTIndexException("Unsupported storage type: " + storageType);
    }

    return indexEngine;
  }
}
