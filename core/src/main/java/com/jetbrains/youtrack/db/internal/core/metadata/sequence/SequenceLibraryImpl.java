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

package com.jetbrains.youtrack.db.internal.core.metadata.sequence;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.common.concur.NeedRetryException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.MetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.exception.SequenceException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence.SEQUENCE_TYPE;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @since 3/2/2015
 */
public class SequenceLibraryImpl {

  private final Map<String, Sequence> sequences = new ConcurrentHashMap<String, Sequence>();
  private final AtomicBoolean reloadNeeded = new AtomicBoolean(false);

  public static void create(DatabaseSessionInternal database) {
    init(database);
  }

  public synchronized void load(final DatabaseSessionInternal db) {
    sequences.clear();

    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass(Sequence.CLASS_NAME)) {
      try (final ResultSet result = db.query("SELECT FROM " + Sequence.CLASS_NAME)) {
        while (result.hasNext()) {
          Result res = result.next();

          final Sequence sequence =
              SequenceHelper.createSequence((EntityImpl) res.getEntity().get());
          sequences.put(sequence.getName(db).toUpperCase(Locale.ENGLISH), sequence);
        }
      }
    }
  }

  public void close() {
    sequences.clear();
  }

  public synchronized Set<String> getSequenceNames(DatabaseSessionInternal database) {
    reloadIfNeeded(database);
    return sequences.keySet();
  }

  public synchronized int getSequenceCount(DatabaseSessionInternal database) {
    reloadIfNeeded(database);
    return sequences.size();
  }

  public Sequence getSequence(final DatabaseSessionInternal database, final String iName) {
    final String name = iName.toUpperCase(Locale.ENGLISH);
    reloadIfNeeded(database);
    Sequence seq;
    synchronized (this) {
      seq = sequences.get(name);
      if (seq == null) {
        load(database);
        seq = sequences.get(name);
      }
    }

    return seq;
  }

  public synchronized Sequence createSequence(
      final DatabaseSessionInternal db,
      final String iName,
      final SEQUENCE_TYPE sequenceType,
      final Sequence.CreateParams params) {
    init(db);
    reloadIfNeeded(db);

    final String key = iName.toUpperCase(Locale.ENGLISH);
    validateSequenceNoExists(key);

    final Sequence sequence = SequenceHelper.createSequence(db, sequenceType, params, iName);
    sequences.put(key, sequence);

    return sequence;
  }

  public synchronized void dropSequence(
      final DatabaseSessionInternal database, final String iName) {
    final Sequence seq = getSequence(database, iName);
    if (seq != null) {
      try {
        database.delete(seq.entityRid);
        sequences.remove(iName.toUpperCase(Locale.ENGLISH));
      } catch (NeedRetryException e) {
        var rec = database.load(seq.entityRid);
        rec.delete();
      }
    }
  }

  public void onSequenceCreated(
      final DatabaseSessionInternal database, final EntityImpl entity) {
    init(database);

    String name = Sequence.getSequenceName(entity);
    if (name == null) {
      return;
    }

    name = name.toUpperCase(Locale.ENGLISH);

    final Sequence seq = getSequence(database, name);

    if (seq != null) {
      return;
    }

    final Sequence sequence = SequenceHelper.createSequence(entity);

    sequences.put(name, sequence);
    onSequenceLibraryUpdate(database);
  }

  public void onSequenceDropped(
      final DatabaseSessionInternal database, final EntityImpl entity) {
    String name = Sequence.getSequenceName(entity);
    if (name == null) {
      return;
    }

    name = name.toUpperCase(Locale.ENGLISH);

    sequences.remove(name);
    onSequenceLibraryUpdate(database);
  }

  private static void init(final DatabaseSessionInternal database) {
    if (database.getMetadata().getSchema().existsClass(Sequence.CLASS_NAME)) {
      return;
    }

    final SchemaClassImpl sequenceClass =
        (SchemaClassImpl) database.getMetadata().getSchema().createClass(Sequence.CLASS_NAME);
    Sequence.initClass(database, sequenceClass);
  }

  private void validateSequenceNoExists(final String iName) {
    if (sequences.containsKey(iName)) {
      throw new SequenceException("Sequence '" + iName + "' already exists");
    }
  }

  private static void onSequenceLibraryUpdate(DatabaseSessionInternal database) {
    for (MetadataUpdateListener one : database.getSharedContext().browseListeners()) {
      one.onSequenceLibraryUpdate(database, database.getName());
    }
  }

  private void reloadIfNeeded(DatabaseSessionInternal database) {
    if (reloadNeeded.get()) {
      load(database);
      reloadNeeded.set(false);
    }
  }

  public void update() {
    reloadNeeded.set(true);
  }
}
