/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.exception.YTConfigurationException;
import com.jetbrains.youtrack.db.internal.core.index.engine.OBaseIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.v1.OCellBTreeIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.v1.OCellBTreeMultiValueIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.v1.OCellBTreeSingleValueIndexEngine;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.index.engine.ORemoteIndexEngine;
import com.jetbrains.youtrack.db.internal.core.storage.index.engine.OSBTreeIndexEngine;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Default YouTrackDB index factory for indexes based on SBTree.<br> Supports index types:
 *
 * <ul>
 *   <li>UNIQUE
 *   <li>NOTUNIQUE
 *   <li>FULLTEXT
 *   <li>DICTIONARY
 * </ul>
 */
public class ODefaultIndexFactory implements OIndexFactory {

  private static final String SBTREE_ALGORITHM = "SBTREE";
  public static final String SBTREE_BONSAI_VALUE_CONTAINER = "SBTREEBONSAISET";
  public static final String NONE_VALUE_CONTAINER = "NONE";
  static final String CELL_BTREE_ALGORITHM = "CELL_BTREE";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>();
    types.add(YTClass.INDEX_TYPE.UNIQUE.toString());
    types.add(YTClass.INDEX_TYPE.NOTUNIQUE.toString());
    types.add(YTClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(YTClass.INDEX_TYPE.DICTIONARY.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>();
    algorithms.add(SBTREE_ALGORITHM);
    algorithms.add(CELL_BTREE_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  static boolean isMultiValueIndex(final String indexType) {
    switch (YTClass.INDEX_TYPE.valueOf(indexType)) {
      case UNIQUE:
      case UNIQUE_HASH_INDEX:
      case DICTIONARY:
      case DICTIONARY_HASH_INDEX:
        return false;
    }

    return true;
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

  public OIndexInternal createIndex(Storage storage, OIndexMetadata im)
      throws YTConfigurationException {
    int version = im.getVersion();
    final String indexType = im.getType();
    final String algorithm = im.getAlgorithm();

    if (version < 0) {
      version = getLastVersion(algorithm);
      im.setVersion(version);
    }

    if (YTClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(im, storage);
    } else if (YTClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(im, storage);
    } else if (YTClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      LogManager.instance()
          .warn(
              ODefaultIndexFactory.class,
              "You are creating native full text index instance. That is unsafe because this type"
                  + " of index is deprecated and will be removed in future.");
      return new OIndexFullText(im, storage);
    } else if (YTClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new OIndexDictionary(im, storage);
    }

    throw new YTConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    switch (algorithm) {
      case SBTREE_ALGORITHM:
        return OSBTreeIndexEngine.VERSION;
      case CELL_BTREE_ALGORITHM:
        return OCellBTreeIndexEngine.VERSION;
    }

    throw new IllegalStateException("Invalid algorithm name " + algorithm);
  }

  @Override
  public OBaseIndexEngine createIndexEngine(Storage storage, IndexEngineData data) {

    if (data.getAlgorithm() == null) {
      throw new YTIndexException("Name of algorithm is not specified");
    }
    final OBaseIndexEngine indexEngine;
    String storageType = storage.getType();

    if (storageType.equals("distributed")) {
      storageType = storage.getType();
    }

    switch (storageType) {
      case "memory":
      case "plocal":
        AbstractPaginatedStorage realStorage = (AbstractPaginatedStorage) storage;
        switch (data.getAlgorithm()) {
          case SBTREE_ALGORITHM:
            indexEngine =
                new OSBTreeIndexEngine(
                    data.getIndexId(), data.getName(), realStorage, data.getVersion());
            break;
          case CELL_BTREE_ALGORITHM:
            if (data.isMultivalue()) {
              indexEngine =
                  new OCellBTreeMultiValueIndexEngine(
                      data.getIndexId(), data.getName(), realStorage, data.getVersion());
            } else {
              indexEngine =
                  new OCellBTreeSingleValueIndexEngine(
                      data.getIndexId(), data.getName(), realStorage, data.getVersion());
            }
            break;
          default:
            throw new IllegalStateException("Invalid name of algorithm :'" + "'");
        }
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
