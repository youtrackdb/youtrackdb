/*
 *
 *  *  Copyright OxygenDB
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
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

  private final Map<String, OSequence> sequences = new ConcurrentHashMap<String, OSequence>();
  private final AtomicBoolean reloadNeeded = new AtomicBoolean(false);

  public static void create(ODatabaseSessionInternal database) {
    init(database);
  }

  public synchronized void load(final ODatabaseSessionInternal db) {
    sequences.clear();

    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass(OSequence.CLASS_NAME)) {
      try (final OResultSet result = db.query("SELECT FROM " + OSequence.CLASS_NAME)) {
        while (result.hasNext()) {
          OResult res = result.next();

          final OSequence sequence =
              OSequenceHelper.createSequence((ODocument) res.getElement().get());
          sequences.put(sequence.getName().toUpperCase(Locale.ENGLISH), sequence);
        }
      }
    }
  }

  public void close() {
    sequences.clear();
  }

  public synchronized Set<String> getSequenceNames(ODatabaseSessionInternal database) {
    reloadIfNeeded(database);
    return sequences.keySet();
  }

  public synchronized int getSequenceCount(ODatabaseSessionInternal database) {
    reloadIfNeeded(database);
    return sequences.size();
  }

  public OSequence getSequence(final ODatabaseSessionInternal database, final String iName) {
    final String name = iName.toUpperCase(Locale.ENGLISH);
    reloadIfNeeded(database);
    OSequence seq;
    synchronized (this) {
      seq = sequences.get(name);
      if (seq == null) {
        load(database);
        seq = sequences.get(name);
      }
    }

    return seq;
  }

  public synchronized OSequence createSequence(
      final ODatabaseSessionInternal database,
      final String iName,
      final SEQUENCE_TYPE sequenceType,
      final OSequence.CreateParams params) {
    init(database);
    reloadIfNeeded(database);

    final String key = iName.toUpperCase(Locale.ENGLISH);
    validateSequenceNoExists(key);

    final OSequence sequence = OSequenceHelper.createSequence(sequenceType, params, iName);
    sequences.put(key, sequence);

    return sequence;
  }

  public synchronized void dropSequence(
      final ODatabaseSessionInternal database, final String iName) {
    final OSequence seq = getSequence(database, iName);
    if (seq != null) {
      try {
        database.delete(seq.docRid);
        sequences.remove(iName.toUpperCase(Locale.ENGLISH));
      } catch (ONeedRetryException e) {
        var rec = database.load(seq.docRid, null, false);
        rec.delete();
      }
    }
  }

  public void onSequenceCreated(
      final ODatabaseSessionInternal database, final ODocument iDocument) {
    init(database);

    String name = OSequence.getSequenceName(iDocument);
    if (name == null) {
      return;
    }

    name = name.toUpperCase(Locale.ENGLISH);

    final OSequence seq = getSequence(database, name);

    if (seq != null) {
      return;
    }

    final OSequence sequence = OSequenceHelper.createSequence(iDocument);

    sequences.put(name, sequence);
    onSequenceLibraryUpdate(database);
  }

  public void onSequenceDropped(
      final ODatabaseSessionInternal database, final ODocument iDocument) {
    String name = OSequence.getSequenceName(iDocument);
    if (name == null) {
      return;
    }

    name = name.toUpperCase(Locale.ENGLISH);

    sequences.remove(name);
    onSequenceLibraryUpdate(database);
  }

  private static void init(final ODatabaseSessionInternal database) {
    if (database.getMetadata().getSchema().existsClass(OSequence.CLASS_NAME)) {
      return;
    }

    final OClassImpl sequenceClass =
        (OClassImpl) database.getMetadata().getSchema().createClass(OSequence.CLASS_NAME);
    OSequence.initClass(database, sequenceClass);
  }

  private void validateSequenceNoExists(final String iName) {
    if (sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence '" + iName + "' already exists");
    }
  }

  private static void onSequenceLibraryUpdate(ODatabaseSessionInternal database) {
    for (OMetadataUpdateListener one : database.getSharedContext().browseListeners()) {
      one.onSequenceLibraryUpdate(database, database.getName());
    }
  }

  private void reloadIfNeeded(ODatabaseSessionInternal database) {
    if (reloadNeeded.get()) {
      load(database);
      reloadNeeded.set(false);
    }
  }

  public void update() {
    reloadNeeded.set(true);
  }
}
