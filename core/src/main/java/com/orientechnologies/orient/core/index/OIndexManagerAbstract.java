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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.Collection;
import java.util.Set;

/**
 * Abstract class to manage indexes.
 */
public interface OIndexManagerAbstract extends OCloseable {

  String CONFIG_INDEXES = "indexes";
  String DICTIONARY_NAME = "dictionary";

  void recreateIndexes(YTDatabaseSessionInternal database);

  default void create() {
    throw new UnsupportedOperationException();
  }

  boolean autoRecreateIndexesAfterCrash(YTDatabaseSessionInternal database);

  OIndex createIndex(
      YTDatabaseSessionInternal database,
      final String iName,
      final String iType,
      OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final OProgressListener progressListener,
      YTDocument metadata);

  OIndex createIndex(
      YTDatabaseSessionInternal database,
      final String iName,
      final String iType,
      OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final OProgressListener progressListener,
      YTDocument metadata,
      String algorithm);

  void waitTillIndexRestore();

  void removeClassPropertyIndex(YTDatabaseSessionInternal session, OIndex idx);

  void dropIndex(YTDatabaseSessionInternal database, String iIndexName);

  void reload(YTDatabaseSessionInternal session);

  void addClusterToIndex(YTDatabaseSessionInternal session, String clusterName, String indexName);

  void load(YTDatabaseSessionInternal database);

  void removeClusterFromIndex(YTDatabaseSessionInternal session, String clusterName,
      String indexName);

  void save(YTDatabaseSessionInternal session);

  void getClassRawIndexes(String name, Collection<OIndex> indexes2);

  YTDocument getConfiguration(YTDatabaseSessionInternal session);

  String getDefaultClusterName();

  void setDefaultClusterName(YTDatabaseSessionInternal database, String defaultClusterName2);

  ODictionary<YTRecord> getDictionary(YTDatabaseSessionInternal database);

  Set<OIndex> getClassInvolvedIndexes(
      YTDatabaseSessionInternal database, String className, Collection<String> fields);

  Set<OIndex> getClassInvolvedIndexes(
      YTDatabaseSessionInternal database, String className, String... fields);

  boolean areIndexed(String className, String... fields);

  boolean areIndexed(final String className, final Collection<String> fields);

  void getClassIndexes(
      YTDatabaseSessionInternal database, String className, Collection<OIndex> indexes2);

  Set<OIndex> getClassIndexes(YTDatabaseSessionInternal database, String className);

  OIndex getClassIndex(YTDatabaseSessionInternal database, String className, String indexName);

  OIndexUnique getClassUniqueIndex(String className);

  OIndex getClassAutoShardingIndex(YTDatabaseSessionInternal database, String className);

  void create(YTDatabaseSessionInternal database);

  Collection<? extends OIndex> getIndexes(YTDatabaseSessionInternal database);

  OIndex getIndex(YTDatabaseSessionInternal database, String iName);

  boolean existsIndex(String iName);

  YTDocument getDocument(YTDatabaseSessionInternal session);

  YTDocument toStream(YTDatabaseSessionInternal session);

  OIndex getRawIndex(String indexName);

  OIndex preProcessBeforeReturn(YTDatabaseSessionInternal database, OIndex index);
}
