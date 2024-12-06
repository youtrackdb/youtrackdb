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

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMultiValue;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Handles indexing when records change.
 */
public class ClassIndexManager {

  public static void checkIndexesAfterCreate(
      EntityImpl entity, DatabaseSessionInternal database) {
    entity = checkForLoading(database, entity);
    processIndexOnCreate(database, entity);
  }

  public static void reIndex(DatabaseSessionInternal session, EntityImpl entity,
      Index index) {
    entity = checkForLoading(session, entity);
    addIndexEntry(session, entity, entity.getIdentity(), index);
  }

  public static void processIndexOnCreate(DatabaseSessionInternal database,
      EntityImpl entity) {
    final SchemaImmutableClass cls = EntityInternalUtils.getImmutableSchemaClass(database, entity);
    if (cls != null) {
      final Collection<Index> indexes = cls.getRawIndexes();
      addIndexesEntries(database, entity, indexes);
    }
  }

  public static void checkIndexesAfterUpdate(
      EntityImpl entity, DatabaseSessionInternal database) {
    entity = checkForLoading(database, entity);
    processIndexOnUpdate(database, entity);
  }

  public static void processIndexOnUpdate(DatabaseSessionInternal database,
      EntityImpl entity) {
    final SchemaImmutableClass cls = EntityInternalUtils.getImmutableSchemaClass(database, entity);
    if (cls == null) {
      return;
    }

    final Collection<Index> indexes = cls.getRawIndexes();
    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(entity.getDirtyFields()));
      if (!dirtyFields.isEmpty()) {
        for (final Index index : indexes) {
          processIndexUpdate(database, entity, dirtyFields, index);
        }
      }
    }
  }

  public static void checkIndexesAfterDelete(
      EntityImpl entity, DatabaseSessionInternal database) {
    processIndexOnDelete(database, entity);
  }

  private static void processCompositeIndexUpdate(
      DatabaseSessionInternal session,
      final Index index,
      final Set<String> dirtyFields,
      final EntityImpl iRecord) {
    final CompositeIndexDefinition indexDefinition =
        (CompositeIndexDefinition) index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    final String multiValueField = indexDefinition.getMultiValueField();

    for (final String indexField : indexFields) {
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField)) {
            if (dirtyFields.contains(field)) {
              origValues.add(iRecord.getOriginalValue(field));
            } else {
              origValues.add(iRecord.field(field));
            }
          }
        }

        if (multiValueField == null) {
          final Object origValue = indexDefinition.createValue(session, origValues);
          final Object newValue = indexDefinition.getDocumentValueToIndex(session, iRecord);

          if (!indexDefinition.isNullValuesIgnored() || origValue != null) {
            addRemove(session, index, origValue, iRecord);
          }

          if (!indexDefinition.isNullValuesIgnored() || newValue != null) {
            addPut(session, index, newValue, iRecord.getIdentity());
          }
        } else {
          final MultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
              iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine == null) {
            if (dirtyFields.contains(multiValueField)) {
              origValues.add(
                  indexDefinition.getMultiValueDefinitionIndex(),
                  iRecord.getOriginalValue(multiValueField));
            } else {
              origValues.add(
                  indexDefinition.getMultiValueDefinitionIndex(), iRecord.field(multiValueField));
            }

            final Object origValue = indexDefinition.createValue(session, origValues);
            final Object newValue = indexDefinition.getDocumentValueToIndex(session, iRecord);

            processIndexUpdateFieldAssignment(session, index, iRecord, origValue, newValue);
          } else {
            // in case of null values support and empty collection field we put null placeholder in
            // place where collection item should be located so we can not use "fast path" to
            // update index values
            if (dirtyFields.size() == 1 && indexDefinition.isNullValuesIgnored()) {
              final Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
              keysToAdd.defaultReturnValue(-1);
              final Object2IntOpenHashMap<CompositeKey> keysToRemove =
                  new Object2IntOpenHashMap<>();
              keysToRemove.defaultReturnValue(-1);

              for (MultiValueChangeEvent<?, ?> changeEvent :
                  multiValueChangeTimeLine.getMultiValueChangeEvents()) {
                indexDefinition.processChangeEvent(
                    session, changeEvent, keysToAdd, keysToRemove, origValues.toArray());
              }

              for (final Object keyToRemove : keysToRemove.keySet()) {
                addRemove(session, index, keyToRemove, iRecord);
              }

              for (final Object keyToAdd : keysToAdd.keySet()) {
                addPut(session, index, keyToAdd, iRecord.getIdentity());
              }
            } else {
              @SuppressWarnings("rawtypes") final TrackedMultiValue fieldValue = iRecord.field(
                  multiValueField);
              @SuppressWarnings("unchecked") final Object restoredMultiValue =
                  fieldValue.returnOriginalState(session,
                      multiValueChangeTimeLine.getMultiValueChangeEvents());

              origValues.add(indexDefinition.getMultiValueDefinitionIndex(), restoredMultiValue);

              final Object origValue = indexDefinition.createValue(session, origValues);
              final Object newValue = indexDefinition.getDocumentValueToIndex(session, iRecord);

              processIndexUpdateFieldAssignment(session, index, iRecord, origValue, newValue);
            }
          }
        }
        return;
      }
    }
  }

  private static void processSingleIndexUpdate(
      final Index index,
      final Set<String> dirtyFields,
      final EntityImpl iRecord,
      DatabaseSessionInternal session) {
    final IndexDefinition indexDefinition = index.getDefinition();
    final List<String> indexFields = indexDefinition.getFields();

    if (indexFields.isEmpty()) {
      return;
    }

    final String indexField = indexFields.get(0);
    if (!dirtyFields.contains(indexField)) {
      return;
    }

    final MultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
        iRecord.getCollectionTimeLine(indexField);
    if (multiValueChangeTimeLine != null) {
      final IndexDefinitionMultiValue indexDefinitionMultiValue =
          (IndexDefinitionMultiValue) indexDefinition;
      final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
      keysToAdd.defaultReturnValue(-1);
      final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
      keysToRemove.defaultReturnValue(-1);

      for (MultiValueChangeEvent<?, ?> changeEvent :
          multiValueChangeTimeLine.getMultiValueChangeEvents()) {
        indexDefinitionMultiValue.processChangeEvent(session, changeEvent, keysToAdd, keysToRemove);
      }

      for (final Object keyToRemove : keysToRemove.keySet()) {
        addRemove(session, index, keyToRemove, iRecord);
      }

      for (final Object keyToAdd : keysToAdd.keySet()) {
        addPut(session, index, keyToAdd, iRecord.getIdentity());
      }

    } else {
      final Object origValue =
          indexDefinition.createValue(session, iRecord.getOriginalValue(indexField));
      final Object newValue = indexDefinition.getDocumentValueToIndex(session, iRecord);

      processIndexUpdateFieldAssignment(session, index, iRecord, origValue, newValue);
    }
  }

  private static void processIndexUpdateFieldAssignment(
      DatabaseSessionInternal session, Index index, EntityImpl iRecord, final Object origValue,
      final Object newValue) {
    final IndexDefinition indexDefinition = index.getDefinition();
    if ((origValue instanceof Collection) && (newValue instanceof Collection)) {
      final Set<Object> valuesToRemove = new HashSet<>((Collection<?>) origValue);
      final Set<Object> valuesToAdd = new HashSet<>((Collection<?>) newValue);

      valuesToRemove.removeAll((Collection<?>) newValue);
      valuesToAdd.removeAll((Collection<?>) origValue);

      for (final Object valueToRemove : valuesToRemove) {
        if (!indexDefinition.isNullValuesIgnored() || valueToRemove != null) {
          addRemove(session, index, valueToRemove, iRecord);
        }
      }

      for (final Object valueToAdd : valuesToAdd) {
        if (!indexDefinition.isNullValuesIgnored() || valueToAdd != null) {
          addPut(session, index, valueToAdd, iRecord);
        }
      }
    } else {
      deleteIndexKey(session, index, iRecord, origValue);
      if (newValue instanceof Collection) {
        for (final Object newValueItem : (Collection<?>) newValue) {
          addPut(session, index, newValueItem, iRecord.getIdentity());
        }
      } else if (!indexDefinition.isNullValuesIgnored() || newValue != null) {
        addPut(session, index, newValue, iRecord.getIdentity());
      }
    }
  }

  private static boolean processCompositeIndexDelete(
      DatabaseSessionInternal session,
      final Index index,
      final Set<String> dirtyFields,
      final EntityImpl iRecord) {
    final CompositeIndexDefinition indexDefinition =
        (CompositeIndexDefinition) index.getDefinition();

    final String multiValueField = indexDefinition.getMultiValueField();

    final List<String> indexFields = indexDefinition.getFields();
    for (final String indexField : indexFields) {
      // REMOVE IT
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField)) {
            if (dirtyFields.contains(field)) {
              origValues.add(iRecord.getOriginalValue(field));
            } else {
              origValues.add(iRecord.field(field));
            }
          }
        }

        if (multiValueField != null) {
          final MultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
              iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine != null) {
            @SuppressWarnings("rawtypes") final TrackedMultiValue fieldValue = iRecord.field(
                multiValueField);
            @SuppressWarnings("unchecked") final Object restoredMultiValue =
                fieldValue.returnOriginalState(session,
                    multiValueChangeTimeLine.getMultiValueChangeEvents());
            origValues.add(indexDefinition.getMultiValueDefinitionIndex(), restoredMultiValue);
          } else if (dirtyFields.contains(multiValueField)) {
            origValues.add(
                indexDefinition.getMultiValueDefinitionIndex(),
                iRecord.getOriginalValue(multiValueField));
          } else {
            origValues.add(
                indexDefinition.getMultiValueDefinitionIndex(), iRecord.field(multiValueField));
          }
        }

        final Object origValue = indexDefinition.createValue(session, origValues);
        deleteIndexKey(session, index, iRecord, origValue);
        return true;
      }
    }
    return false;
  }

  private static void deleteIndexKey(
      DatabaseSessionInternal session, final Index index, final EntityImpl iRecord,
      final Object origValue) {
    final IndexDefinition indexDefinition = index.getDefinition();
    if (origValue instanceof Collection) {
      for (final Object valueItem : (Collection<?>) origValue) {
        if (!indexDefinition.isNullValuesIgnored() || valueItem != null) {
          addRemove(session, index, valueItem, iRecord);
        }
      }
    } else if (!indexDefinition.isNullValuesIgnored() || origValue != null) {
      addRemove(session, index, origValue, iRecord);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static boolean processSingleIndexDelete(
      DatabaseSessionInternal session,
      final Index index,
      final Set<String> dirtyFields,
      final EntityImpl iRecord) {
    final IndexDefinition indexDefinition = index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    if (indexFields.isEmpty()) {
      return false;
    }

    final String indexField = indexFields.iterator().next();
    if (dirtyFields.contains(indexField)) {
      final MultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
          iRecord.getCollectionTimeLine(indexField);

      final Object origValue;
      if (multiValueChangeTimeLine != null) {
        final TrackedMultiValue fieldValue = iRecord.field(indexField);
        final Object restoredMultiValue =
            fieldValue.returnOriginalState(session,
                multiValueChangeTimeLine.getMultiValueChangeEvents());
        origValue = indexDefinition.createValue(session, restoredMultiValue);
      } else {
        origValue = indexDefinition.createValue(session, iRecord.getOriginalValue(indexField));
      }
      deleteIndexKey(session, index, iRecord, origValue);
      return true;
    }
    return false;
  }

  private static EntityImpl checkForLoading(DatabaseSessionInternal session,
      final EntityImpl iRecord) {
    if (iRecord.isUnloaded()) {
      try {
        return session.load(iRecord.getIdentity());
      } catch (final RecordNotFoundException e) {
        throw BaseException.wrapException(
            new IndexException("Error during loading of record with id " + iRecord.getIdentity()),
            e);
      }
    }
    return iRecord;
  }

  public static void processIndexUpdate(
      DatabaseSessionInternal session,
      EntityImpl entity,
      Set<String> dirtyFields,
      Index index) {
    if (index.getDefinition() instanceof CompositeIndexDefinition) {
      processCompositeIndexUpdate(session, index, dirtyFields, entity);
    } else {
      processSingleIndexUpdate(index, dirtyFields, entity, session);
    }
  }

  public static void addIndexesEntries(
      DatabaseSessionInternal session, EntityImpl entity, final Collection<Index> indexes) {
    // STORE THE RECORD IF NEW, OTHERWISE ITS RID
    final Identifiable rid = entity.getIdentity();

    for (final Index index : indexes) {
      addIndexEntry(session, entity, rid, index);
    }
  }

  private static void addIndexEntry(
      DatabaseSessionInternal session, EntityImpl entity, Identifiable rid, Index index) {
    final IndexDefinition indexDefinition = index.getDefinition();
    final Object key = indexDefinition.getDocumentValueToIndex(session, entity);
    if (key instanceof Collection) {
      for (final Object keyItem : (Collection<?>) key) {
        if (!indexDefinition.isNullValuesIgnored() || keyItem != null) {
          addPut(session, index, keyItem, rid);
        }
      }
    } else if (!indexDefinition.isNullValuesIgnored() || key != null) {
      addPut(session, index, key, rid);
    }
  }

  public static void processIndexOnDelete(DatabaseSessionInternal database,
      EntityImpl entity) {
    final SchemaImmutableClass cls = EntityInternalUtils.getImmutableSchemaClass(database, entity);
    if (cls == null) {
      return;
    }

    final Collection<Index> indexes = new ArrayList<>(cls.getRawIndexes());

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(entity.getDirtyFields()));

      if (!dirtyFields.isEmpty()) {
        // REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
        final Iterator<Index> indexIterator = indexes.iterator();

        while (indexIterator.hasNext()) {
          final Index index = indexIterator.next();

          final boolean result;
          if (index.getDefinition() instanceof CompositeIndexDefinition) {
            result = processCompositeIndexDelete(database, index, dirtyFields, entity);
          } else {
            result = processSingleIndexDelete(database, index, dirtyFields, entity);
          }

          if (result) {
            indexIterator.remove();
          }
        }
      }
    }

    // REMOVE INDEX OF ENTRIES FOR THE NON CHANGED ONLY VALUES
    for (final Index index : indexes) {
      final Object key = index.getDefinition().getDocumentValueToIndex(database, entity);
      deleteIndexKey(database, index, entity, key);
    }
  }

  private static void addPut(DatabaseSessionInternal session, Index index, Object key,
      Identifiable value) {
    index.put(session, key, value);
  }

  private static void addRemove(DatabaseSessionInternal session, Index index, Object key,
      Identifiable value) {
    index.remove(session, key, value);
  }
}
