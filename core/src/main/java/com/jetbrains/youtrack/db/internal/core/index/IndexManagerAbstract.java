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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.common.concur.resource.CloseableInStorage;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.dictionary.Dictionary;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Set;

/**
 * Abstract class to manage indexes.
 */
public interface IndexManagerAbstract extends CloseableInStorage {

  String CONFIG_INDEXES = "indexes";
  String DICTIONARY_NAME = "dictionary";

  void recreateIndexes(DatabaseSessionInternal database);

  default void create() {
    throw new UnsupportedOperationException();
  }

  boolean autoRecreateIndexesAfterCrash(DatabaseSessionInternal database);

  Index createIndex(
      DatabaseSessionInternal database,
      final String iName,
      final String iType,
      IndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final ProgressListener progressListener,
      EntityImpl metadata);

  Index createIndex(
      DatabaseSessionInternal database,
      final String iName,
      final String iType,
      IndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final ProgressListener progressListener,
      EntityImpl metadata,
      String algorithm);

  void waitTillIndexRestore();

  void removeClassPropertyIndex(DatabaseSessionInternal session, Index idx);

  void dropIndex(DatabaseSessionInternal database, String iIndexName);

  void reload(DatabaseSessionInternal session);

  void addClusterToIndex(DatabaseSessionInternal session, String clusterName, String indexName);

  void load(DatabaseSessionInternal database);

  void removeClusterFromIndex(DatabaseSessionInternal session, String clusterName,
      String indexName);

  void save(DatabaseSessionInternal session);

  void getClassRawIndexes(String name, Collection<Index> indexes2);

  EntityImpl getConfiguration(DatabaseSessionInternal session);

  String getDefaultClusterName();

  void setDefaultClusterName(DatabaseSessionInternal database, String defaultClusterName2);

  Dictionary<Record> getDictionary(DatabaseSessionInternal database);

  Set<Index> getClassInvolvedIndexes(
      DatabaseSessionInternal database, String className, Collection<String> fields);

  Set<Index> getClassInvolvedIndexes(
      DatabaseSessionInternal database, String className, String... fields);

  boolean areIndexed(String className, String... fields);

  boolean areIndexed(final String className, final Collection<String> fields);

  void getClassIndexes(
      DatabaseSessionInternal database, String className, Collection<Index> indexes2);

  Set<Index> getClassIndexes(DatabaseSessionInternal database, String className);

  Index getClassIndex(DatabaseSessionInternal database, String className, String indexName);

  IndexUnique getClassUniqueIndex(String className);

  Index getClassAutoShardingIndex(DatabaseSessionInternal database, String className);

  void create(DatabaseSessionInternal database);

  Collection<? extends Index> getIndexes(DatabaseSessionInternal database);

  Index getIndex(DatabaseSessionInternal database, String iName);

  boolean existsIndex(String iName);

  EntityImpl getDocument(DatabaseSessionInternal session);

  EntityImpl toStream(DatabaseSessionInternal session);

  Index getRawIndex(String indexName);

  Index preProcessBeforeReturn(DatabaseSessionInternal database, Index index);
}
