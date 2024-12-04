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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
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
public class OClassIndexManager {

  public static void checkIndexesAfterCreate(
      YTDocument document, YTDatabaseSessionInternal database) {
    document = checkForLoading(database, document);
    processIndexOnCreate(database, document);
  }

  public static void reIndex(YTDatabaseSessionInternal session, YTDocument document, OIndex index) {
    document = checkForLoading(session, document);
    addIndexEntry(session, document, document.getIdentity(), index);
  }

  public static void processIndexOnCreate(YTDatabaseSessionInternal database, YTDocument document) {
    final YTImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, document);
    if (cls != null) {
      final Collection<OIndex> indexes = cls.getRawIndexes();
      addIndexesEntries(database, document, indexes);
    }
  }

  public static void checkIndexesAfterUpdate(
      YTDocument iDocument, YTDatabaseSessionInternal database) {
    iDocument = checkForLoading(database, iDocument);
    processIndexOnUpdate(database, iDocument);
  }

  public static void processIndexOnUpdate(YTDatabaseSessionInternal database,
      YTDocument iDocument) {
    final YTImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls == null) {
      return;
    }

    final Collection<OIndex> indexes = cls.getRawIndexes();
    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(iDocument.getDirtyFields()));
      if (!dirtyFields.isEmpty()) {
        for (final OIndex index : indexes) {
          processIndexUpdate(database, iDocument, dirtyFields, index);
        }
      }
    }
  }

  public static void checkIndexesAfterDelete(
      YTDocument iDocument, YTDatabaseSessionInternal database) {
    processIndexOnDelete(database, iDocument);
  }

  private static void processCompositeIndexUpdate(
      YTDatabaseSessionInternal session,
      final OIndex index,
      final Set<String> dirtyFields,
      final YTDocument iRecord) {
    final OCompositeIndexDefinition indexDefinition =
        (OCompositeIndexDefinition) index.getDefinition();

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
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
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
              final Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
              keysToAdd.defaultReturnValue(-1);
              final Object2IntOpenHashMap<OCompositeKey> keysToRemove =
                  new Object2IntOpenHashMap<>();
              keysToRemove.defaultReturnValue(-1);

              for (OMultiValueChangeEvent<?, ?> changeEvent :
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
              @SuppressWarnings("rawtypes") final OTrackedMultiValue fieldValue = iRecord.field(
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
      final OIndex index,
      final Set<String> dirtyFields,
      final YTDocument iRecord,
      YTDatabaseSessionInternal session) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final List<String> indexFields = indexDefinition.getFields();

    if (indexFields.isEmpty()) {
      return;
    }

    final String indexField = indexFields.get(0);
    if (!dirtyFields.contains(indexField)) {
      return;
    }

    final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
        iRecord.getCollectionTimeLine(indexField);
    if (multiValueChangeTimeLine != null) {
      final OIndexDefinitionMultiValue indexDefinitionMultiValue =
          (OIndexDefinitionMultiValue) indexDefinition;
      final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
      keysToAdd.defaultReturnValue(-1);
      final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
      keysToRemove.defaultReturnValue(-1);

      for (OMultiValueChangeEvent<?, ?> changeEvent :
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
      YTDatabaseSessionInternal session, OIndex index, YTDocument iRecord, final Object origValue,
      final Object newValue) {
    final OIndexDefinition indexDefinition = index.getDefinition();
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
      YTDatabaseSessionInternal session,
      final OIndex index,
      final Set<String> dirtyFields,
      final YTDocument iRecord) {
    final OCompositeIndexDefinition indexDefinition =
        (OCompositeIndexDefinition) index.getDefinition();

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
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
              iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine != null) {
            @SuppressWarnings("rawtypes") final OTrackedMultiValue fieldValue = iRecord.field(
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
      YTDatabaseSessionInternal session, final OIndex index, final YTDocument iRecord,
      final Object origValue) {
    final OIndexDefinition indexDefinition = index.getDefinition();
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
      YTDatabaseSessionInternal session,
      final OIndex index,
      final Set<String> dirtyFields,
      final YTDocument iRecord) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    if (indexFields.isEmpty()) {
      return false;
    }

    final String indexField = indexFields.iterator().next();
    if (dirtyFields.contains(indexField)) {
      final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
          iRecord.getCollectionTimeLine(indexField);

      final Object origValue;
      if (multiValueChangeTimeLine != null) {
        final OTrackedMultiValue fieldValue = iRecord.field(indexField);
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

  private static YTDocument checkForLoading(YTDatabaseSessionInternal session,
      final YTDocument iRecord) {
    if (iRecord.isUnloaded()) {
      try {
        return session.load(iRecord.getIdentity());
      } catch (final ORecordNotFoundException e) {
        throw OException.wrapException(
            new OIndexException("Error during loading of record with id " + iRecord.getIdentity()),
            e);
      }
    }
    return iRecord;
  }

  public static void processIndexUpdate(
      YTDatabaseSessionInternal session,
      YTDocument iDocument,
      Set<String> dirtyFields,
      OIndex index) {
    if (index.getDefinition() instanceof OCompositeIndexDefinition) {
      processCompositeIndexUpdate(session, index, dirtyFields, iDocument);
    } else {
      processSingleIndexUpdate(index, dirtyFields, iDocument, session);
    }
  }

  public static void addIndexesEntries(
      YTDatabaseSessionInternal session, YTDocument document, final Collection<OIndex> indexes) {
    // STORE THE RECORD IF NEW, OTHERWISE ITS RID
    final YTIdentifiable rid = document.getIdentity();

    for (final OIndex index : indexes) {
      addIndexEntry(session, document, rid, index);
    }
  }

  private static void addIndexEntry(
      YTDatabaseSessionInternal session, YTDocument document, YTIdentifiable rid, OIndex index) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final Object key = indexDefinition.getDocumentValueToIndex(session, document);
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

  public static void processIndexOnDelete(YTDatabaseSessionInternal database,
      YTDocument iDocument) {
    final YTImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls == null) {
      return;
    }

    final Collection<OIndex> indexes = new ArrayList<>(cls.getRawIndexes());

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(iDocument.getDirtyFields()));

      if (!dirtyFields.isEmpty()) {
        // REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
        final Iterator<OIndex> indexIterator = indexes.iterator();

        while (indexIterator.hasNext()) {
          final OIndex index = indexIterator.next();

          final boolean result;
          if (index.getDefinition() instanceof OCompositeIndexDefinition) {
            result = processCompositeIndexDelete(database, index, dirtyFields, iDocument);
          } else {
            result = processSingleIndexDelete(database, index, dirtyFields, iDocument);
          }

          if (result) {
            indexIterator.remove();
          }
        }
      }
    }

    // REMOVE INDEX OF ENTRIES FOR THE NON CHANGED ONLY VALUES
    for (final OIndex index : indexes) {
      final Object key = index.getDefinition().getDocumentValueToIndex(database, iDocument);
      deleteIndexKey(database, index, iDocument, key);
    }
  }

  private static void addPut(YTDatabaseSessionInternal session, OIndex index, Object key,
      YTIdentifiable value) {
    index.put(session, key, value);
  }

  private static void addRemove(YTDatabaseSessionInternal session, OIndex index, Object key,
      YTIdentifiable value) {
    index.remove(session, key, value);
  }
}
