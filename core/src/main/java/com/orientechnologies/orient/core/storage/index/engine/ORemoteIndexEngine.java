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
package com.orientechnologies.orient.core.storage.index.engine;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.IndexEngineData;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.index.engine.IndexEngineValidator;
import com.orientechnologies.orient.core.index.engine.IndexEngineValuesTransformer;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * @since 18.07.13
 */
public class ORemoteIndexEngine implements OIndexEngine {

  private final String name;
  private final int id;

  public ORemoteIndexEngine(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  @Override
  public void updateUniqueIndexVersion(final Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(final Object key) {
    return 0; // not implemented
  }

  @Override
  public void init(OIndexMetadata metadata) {
  }

  @Override
  public void flush() {
  }

  public void create(OAtomicOperation atomicOperation, IndexEngineData data) throws IOException {
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
  }

  @Override
  public void load(IndexEngineData data) {
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) {
    return false;
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {
  }

  @Override
  public void close() {
  }

  @Override
  public Object get(YTDatabaseSessionInternal session, Object key) {
    return null;
  }

  @Override
  public void put(YTDatabaseSessionInternal session, OAtomicOperation atomicOperation, Object key,
      Object value) {
  }

  @Override
  public void update(
      YTDatabaseSessionInternal session, OAtomicOperation atomicOperation, Object key,
      OIndexKeyUpdater<Object> updater) {
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation,
      Object key,
      YTRID value,
      IndexEngineValidator<Object, YTRID> validator) {
    return false;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> iterateEntriesBetween(
      YTDatabaseSessionInternal session, Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("stream");
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("stream");
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> iterateEntriesMinor(
      Object toKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("stream");
  }

  @Override
  public long size(IndexEngineValuesTransformer transformer) {
    return 0;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> stream(IndexEngineValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("stream");
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> descStream(
      IndexEngineValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<Object> keyStream() {
    throw new UnsupportedOperationException("keyStream");
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    throw new UnsupportedOperationException(
        "atomic locking is not supported by remote index engine");
  }
}
