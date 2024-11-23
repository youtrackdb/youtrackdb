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
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Set;

/**
 * Abstract class to manage indexes.
 */
public interface OIndexManagerAbstract extends OCloseable {

  String CONFIG_INDEXES = "indexes";
  String DICTIONARY_NAME = "dictionary";

  void recreateIndexes(ODatabaseSessionInternal database);

  default void create() {
    throw new UnsupportedOperationException();
  }

  boolean autoRecreateIndexesAfterCrash(ODatabaseSessionInternal database);

  OIndex createIndex(
      ODatabaseSessionInternal database,
      final String iName,
      final String iType,
      OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final OProgressListener progressListener,
      ODocument metadata);

  OIndex createIndex(
      ODatabaseSessionInternal database,
      final String iName,
      final String iType,
      OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final OProgressListener progressListener,
      ODocument metadata,
      String algorithm);

  void waitTillIndexRestore();

  void removeClassPropertyIndex(ODatabaseSessionInternal session, OIndex idx);

  void dropIndex(ODatabaseSessionInternal database, String iIndexName);

  void reload(ODatabaseSessionInternal session);

  void addClusterToIndex(ODatabaseSessionInternal session, String clusterName, String indexName);

  void load(ODatabaseSessionInternal database);

  void removeClusterFromIndex(ODatabaseSessionInternal session, String clusterName,
      String indexName);

  void save(ODatabaseSessionInternal session);

  void getClassRawIndexes(String name, Collection<OIndex> indexes2);

  ODocument getConfiguration(ODatabaseSessionInternal session);

  String getDefaultClusterName();

  void setDefaultClusterName(ODatabaseSessionInternal database, String defaultClusterName2);

  ODictionary<ORecord> getDictionary(ODatabaseSessionInternal database);

  Set<OIndex> getClassInvolvedIndexes(
      ODatabaseSessionInternal database, String className, Collection<String> fields);

  Set<OIndex> getClassInvolvedIndexes(
      ODatabaseSessionInternal database, String className, String... fields);

  boolean areIndexed(String className, String... fields);

  boolean areIndexed(final String className, final Collection<String> fields);

  void getClassIndexes(
      ODatabaseSessionInternal database, String className, Collection<OIndex> indexes2);

  Set<OIndex> getClassIndexes(ODatabaseSessionInternal database, String className);

  OIndex getClassIndex(ODatabaseSessionInternal database, String className, String indexName);

  OIndexUnique getClassUniqueIndex(String className);

  OIndex getClassAutoShardingIndex(ODatabaseSessionInternal database, String className);

  void create(ODatabaseSessionInternal database);

  Collection<? extends OIndex> getIndexes(ODatabaseSessionInternal database);

  OIndex getIndex(ODatabaseSessionInternal database, String iName);

  boolean existsIndex(String iName);

  ODocument getDocument(ODatabaseSessionInternal session);

  ODocument toStream(ODatabaseSessionInternal session);

  OIndex getRawIndex(String indexName);

  OIndex preProcessBeforeReturn(ODatabaseSessionInternal database, OIndex index);
}
