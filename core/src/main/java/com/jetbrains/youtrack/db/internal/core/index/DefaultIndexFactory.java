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
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.core.index.engine.BaseIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.v1.CellBTreeIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.v1.CellBTreeSingleValueIndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.v1.CellBTreeMultiValueIndexEngine;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.index.engine.RemoteIndexEngine;
import com.jetbrains.youtrack.db.internal.core.storage.index.engine.SBTreeIndexEngine;
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
public class DefaultIndexFactory implements IndexFactory {

  private static final String SBTREE_ALGORITHM = "SBTREE";
  public static final String SBTREE_BONSAI_VALUE_CONTAINER = "SBTREEBONSAISET";
  public static final String NONE_VALUE_CONTAINER = "NONE";
  static final String CELL_BTREE_ALGORITHM = "CELL_BTREE";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>();
    types.add(SchemaClass.INDEX_TYPE.UNIQUE.toString());
    types.add(SchemaClass.INDEX_TYPE.NOTUNIQUE.toString());
    types.add(SchemaClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(SchemaClass.INDEX_TYPE.DICTIONARY.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>();
    algorithms.add(SBTREE_ALGORITHM);
    algorithms.add(CELL_BTREE_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  static boolean isMultiValueIndex(final String indexType) {
    switch (SchemaClass.INDEX_TYPE.valueOf(indexType)) {
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

  public IndexInternal createIndex(Storage storage, IndexMetadata im)
      throws ConfigurationException {
    int version = im.getVersion();
    final String indexType = im.getType();
    final String algorithm = im.getAlgorithm();

    if (version < 0) {
      version = getLastVersion(algorithm);
      im.setVersion(version);
    }

    if (SchemaClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new IndexUnique(im, storage);
    } else if (SchemaClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new IndexNotUnique(im, storage);
    } else if (SchemaClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      LogManager.instance()
          .warn(
              DefaultIndexFactory.class,
              "You are creating native full text index instance. That is unsafe because this type"
                  + " of index is deprecated and will be removed in future.");
      return new IndexFullText(im, storage);
    } else if (SchemaClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new IndexDictionary(im, storage);
    }

    throw new ConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    switch (algorithm) {
      case SBTREE_ALGORITHM:
        return SBTreeIndexEngine.VERSION;
      case CELL_BTREE_ALGORITHM:
        return CellBTreeIndexEngine.VERSION;
    }

    throw new IllegalStateException("Invalid algorithm name " + algorithm);
  }

  @Override
  public BaseIndexEngine createIndexEngine(Storage storage, IndexEngineData data) {

    if (data.getAlgorithm() == null) {
      throw new IndexException("Name of algorithm is not specified");
    }
    final BaseIndexEngine indexEngine;
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
                new SBTreeIndexEngine(
                    data.getIndexId(), data.getName(), realStorage, data.getVersion());
            break;
          case CELL_BTREE_ALGORITHM:
            if (data.isMultivalue()) {
              indexEngine =
                  new CellBTreeMultiValueIndexEngine(
                      data.getIndexId(), data.getName(), realStorage, data.getVersion());
            } else {
              indexEngine =
                  new CellBTreeSingleValueIndexEngine(
                      data.getIndexId(), data.getName(), realStorage, data.getVersion());
            }
            break;
          default:
            throw new IllegalStateException("Invalid name of algorithm :'" + "'");
        }
        break;
      case "remote":
        indexEngine = new RemoteIndexEngine(data.getIndexId(), data.getName());
        break;
      default:
        throw new IndexException("Unsupported storage type: " + storageType);
    }

    return indexEngine;
  }
}
