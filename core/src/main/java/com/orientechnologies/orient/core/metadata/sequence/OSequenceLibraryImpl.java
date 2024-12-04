/*
 *
 *  *  Copyright YouTrackDB
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

package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.schema.YTClassImpl;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @since 3/2/2015
 */
public class OSequenceLibraryImpl {

  private final Map<String, YTSequence> sequences = new ConcurrentHashMap<String, YTSequence>();
  private final AtomicBoolean reloadNeeded = new AtomicBoolean(false);

  public static void create(YTDatabaseSessionInternal database) {
    init(database);
  }

  public synchronized void load(final YTDatabaseSessionInternal db) {
    sequences.clear();

    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass(YTSequence.CLASS_NAME)) {
      try (final OResultSet result = db.query("SELECT FROM " + YTSequence.CLASS_NAME)) {
        while (result.hasNext()) {
          OResult res = result.next();

          final YTSequence sequence =
              OSequenceHelper.createSequence((YTDocument) res.getElement().get());
          sequences.put(sequence.getName().toUpperCase(Locale.ENGLISH), sequence);
        }
      }
    }
  }

  public void close() {
    sequences.clear();
  }

  public synchronized Set<String> getSequenceNames(YTDatabaseSessionInternal database) {
    reloadIfNeeded(database);
    return sequences.keySet();
  }

  public synchronized int getSequenceCount(YTDatabaseSessionInternal database) {
    reloadIfNeeded(database);
    return sequences.size();
  }

  public YTSequence getSequence(final YTDatabaseSessionInternal database, final String iName) {
    final String name = iName.toUpperCase(Locale.ENGLISH);
    reloadIfNeeded(database);
    YTSequence seq;
    synchronized (this) {
      seq = sequences.get(name);
      if (seq == null) {
        load(database);
        seq = sequences.get(name);
      }
    }

    return seq;
  }

  public synchronized YTSequence createSequence(
      final YTDatabaseSessionInternal database,
      final String iName,
      final SEQUENCE_TYPE sequenceType,
      final YTSequence.CreateParams params) {
    init(database);
    reloadIfNeeded(database);

    final String key = iName.toUpperCase(Locale.ENGLISH);
    validateSequenceNoExists(key);

    final YTSequence sequence = OSequenceHelper.createSequence(sequenceType, params, iName);
    sequences.put(key, sequence);

    return sequence;
  }

  public synchronized void dropSequence(
      final YTDatabaseSessionInternal database, final String iName) {
    final YTSequence seq = getSequence(database, iName);
    if (seq != null) {
      try {
        database.delete(seq.docRid);
        sequences.remove(iName.toUpperCase(Locale.ENGLISH));
      } catch (ONeedRetryException e) {
        var rec = database.load(seq.docRid);
        rec.delete();
      }
    }
  }

  public void onSequenceCreated(
      final YTDatabaseSessionInternal database, final YTDocument iDocument) {
    init(database);

    String name = YTSequence.getSequenceName(iDocument);
    if (name == null) {
      return;
    }

    name = name.toUpperCase(Locale.ENGLISH);

    final YTSequence seq = getSequence(database, name);

    if (seq != null) {
      return;
    }

    final YTSequence sequence = OSequenceHelper.createSequence(iDocument);

    sequences.put(name, sequence);
    onSequenceLibraryUpdate(database);
  }

  public void onSequenceDropped(
      final YTDatabaseSessionInternal database, final YTDocument iDocument) {
    String name = YTSequence.getSequenceName(iDocument);
    if (name == null) {
      return;
    }

    name = name.toUpperCase(Locale.ENGLISH);

    sequences.remove(name);
    onSequenceLibraryUpdate(database);
  }

  private static void init(final YTDatabaseSessionInternal database) {
    if (database.getMetadata().getSchema().existsClass(YTSequence.CLASS_NAME)) {
      return;
    }

    final YTClassImpl sequenceClass =
        (YTClassImpl) database.getMetadata().getSchema().createClass(YTSequence.CLASS_NAME);
    YTSequence.initClass(database, sequenceClass);
  }

  private void validateSequenceNoExists(final String iName) {
    if (sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence '" + iName + "' already exists");
    }
  }

  private static void onSequenceLibraryUpdate(YTDatabaseSessionInternal database) {
    for (OMetadataUpdateListener one : database.getSharedContext().browseListeners()) {
      one.onSequenceLibraryUpdate(database, database.getName());
    }
  }

  private void reloadIfNeeded(YTDatabaseSessionInternal database) {
    if (reloadNeeded.get()) {
      load(database);
      reloadNeeded.set(false);
    }
  }

  public void update() {
    reloadNeeded.set(true);
  }
}
