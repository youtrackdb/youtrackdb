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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string;

import com.jetbrains.youtrack.db.internal.common.collection.OLazyIterator;
import com.jetbrains.youtrack.db.internal.common.collection.OMultiCollectionIterator;
import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.ODocumentSerializable;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.string.OStringBuilderSerializable;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.string.OStringSerializerEmbedded;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings({"unchecked", "serial"})
public abstract class ORecordSerializerCSVAbstract extends ORecordSerializerStringAbstract {

  public static final char FIELD_VALUE_SEPARATOR = ':';

  /**
   * Serialize the link.
   *
   * @param buffer
   * @param iParentRecord
   * @param iLinked       Can be an instance of YTRID or a Record<?>
   * @return
   */
  private static YTIdentifiable linkToStream(
      final StringBuilder buffer, final EntityImpl iParentRecord, Object iLinked) {
    if (iLinked == null)
    // NULL REFERENCE
    {
      return null;
    }

    YTIdentifiable resultRid = null;
    YTRID rid;

    if (iLinked instanceof YTRID) {
      // JUST THE REFERENCE
      rid = (YTRID) iLinked;

      assert rid.getIdentity().isValid() || ODatabaseRecordThreadLocal.instance().get().isRemote()
          : "Impossible to serialize invalid link " + rid.getIdentity();
      resultRid = rid;
    } else {
      if (iLinked instanceof String) {
        iLinked = new YTRecordId((String) iLinked);
      }

      if (!(iLinked instanceof YTIdentifiable)) {
        throw new IllegalArgumentException(
            "Invalid object received. Expected a YTIdentifiable but received type="
                + iLinked.getClass().getName()
                + " and value="
                + iLinked);
      }

      // RECORD
      Record iLinkedRecord = ((YTIdentifiable) iLinked).getRecord();
      rid = iLinkedRecord.getIdentity();

      assert rid.getIdentity().isValid() || ODatabaseRecordThreadLocal.instance().get().isRemote()
          : "Impossible to serialize invalid link " + rid.getIdentity();

      final var database = ODatabaseRecordThreadLocal.instance().get();
      if (iParentRecord != null) {
        if (!database.isRetainRecords())
        // REPLACE CURRENT RECORD WITH ITS ID: THIS SAVES A LOT OF MEMORY
        {
          resultRid = iLinkedRecord.getIdentity();
        }
      }
    }

    if (rid.isValid()) {
      rid.toString(buffer);
    }

    return resultRid;
  }

  public Object fieldFromStream(
      YTDatabaseSessionInternal db, final RecordAbstract iSourceRecord,
      final YTType iType,
      YTClass iLinkedClass,
      YTType iLinkedType,
      final String iName,
      final String iValue) {

    if (iValue == null) {
      return null;
    }

    switch (iType) {
      case EMBEDDEDLIST:
      case EMBEDDEDSET:
        return embeddedCollectionFromStream(db,
            (EntityImpl) iSourceRecord, iType, iLinkedClass, iLinkedType, iValue);

      case LINKSET:
      case LINKLIST: {
        if (iValue.length() == 0) {
          return null;
        }

        // REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
        final String value =
            iValue.startsWith("[") || iValue.startsWith("<")
                ? iValue.substring(1, iValue.length() - 1)
                : iValue;

        if (iType == YTType.LINKLIST) {
          return unserializeList(db, (EntityImpl) iSourceRecord, value);
        } else {
          return unserializeSet(db, (EntityImpl) iSourceRecord, value);
        }
      }

      case LINKMAP: {
        if (iValue.length() == 0) {
          return null;
        }

        // REMOVE BEGIN & END MAP CHARACTERS
        String value = iValue.substring(1, iValue.length() - 1);

        @SuppressWarnings("rawtypes") final Map map = new LinkMap((EntityImpl) iSourceRecord,
            EntityImpl.RECORD_TYPE);

        if (value.length() == 0) {
          return map;
        }

        final List<String> items =
            OStringSerializerHelper.smartSplit(
                value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);

        // EMBEDDED LITERALS
        for (String item : items) {
          if (item != null && !item.isEmpty()) {
            final List<String> entry =
                OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
            if (!entry.isEmpty()) {
              String mapValue = entry.get(1);
              if (mapValue != null && !mapValue.isEmpty()) {
                mapValue = mapValue.substring(1);
              }
              map.put(
                  fieldTypeFromStream(db, (EntityImpl) iSourceRecord, YTType.STRING,
                      entry.get(0)),
                  new YTRecordId(mapValue));
            }
          }
        }
        return map;
      }

      case EMBEDDEDMAP:
        return embeddedMapFromStream(db, (EntityImpl) iSourceRecord, iLinkedType, iValue, iName);

      case LINK:
        if (iValue.length() > 1) {
          int pos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
          if (pos > -1) {
            ODatabaseRecordThreadLocal.instance()
                .get()
                .getMetadata()
                .getImmutableSchemaSnapshot()
                .getClass(iValue.substring(1, pos));
          } else {
            pos = 0;
          }

          final String linkAsString = iValue.substring(pos + 1);
          try {
            return new YTRecordId(linkAsString);
          } catch (IllegalArgumentException e) {
            LogManager.instance()
                .error(
                    this,
                    "Error on unmarshalling field '%s' of record '%s': value '%s' is not a link",
                    e,
                    iName,
                    iSourceRecord,
                    linkAsString);
            return new ChangeableRecordId();
          }
        } else {
          return null;
        }

      case EMBEDDED:
        if (iValue.length() > 2) {
          // REMOVE BEGIN & END EMBEDDED CHARACTERS
          final String value = iValue.substring(1, iValue.length() - 1);

          final Object embeddedObject = OStringSerializerEmbedded.INSTANCE.fromStream(db, value);
          if (embeddedObject instanceof EntityImpl) {
            ODocumentInternal.addOwner((EntityImpl) embeddedObject, iSourceRecord);
          }

          // RECORD
          return embeddedObject;
        } else {
          return null;
        }
      case LINKBAG:
        final String value =
            iValue.charAt(0) == OStringSerializerHelper.BAG_BEGIN
                ? iValue.substring(1, iValue.length() - 1)
                : iValue;
        return RidBag.fromStream(db, value);
      default:
        return fieldTypeFromStream(db, (EntityImpl) iSourceRecord, iType, iValue);
    }
  }

  public Map<String, Object> embeddedMapFromStream(
      YTDatabaseSessionInternal db, final EntityImpl iSourceDocument,
      final YTType iLinkedType,
      final String iValue,
      final String iName) {
    if (iValue.length() == 0) {
      return null;
    }

    // REMOVE BEGIN & END MAP CHARACTERS
    String value = iValue.substring(1, iValue.length() - 1);

    @SuppressWarnings("rawtypes")
    Map map;
    if (iLinkedType == YTType.LINK || iLinkedType == YTType.EMBEDDED) {
      map = new LinkMap(iSourceDocument, EntityImpl.RECORD_TYPE);
    } else {
      map = new TrackedMap<Object>(iSourceDocument);
    }

    if (value.length() == 0) {
      return map;
    }

    final List<String> items =
        OStringSerializerHelper.smartSplit(
            value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);

    // EMBEDDED LITERALS

    for (String item : items) {
      if (item != null && !item.isEmpty()) {
        final List<String> entries =
            OStringSerializerHelper.smartSplit(
                item, OStringSerializerHelper.ENTRY_SEPARATOR, true, false);
        if (!entries.isEmpty()) {
          final Object mapValueObject;
          if (entries.size() > 1) {
            String mapValue = entries.get(1);

            final YTType linkedType;

            if (iLinkedType == null) {
              if (!mapValue.isEmpty()) {
                linkedType = getType(mapValue);
                if ((iName == null
                    || iSourceDocument.fieldType(iName) == null
                    || iSourceDocument.fieldType(iName) != YTType.EMBEDDEDMAP)
                    && isConvertToLinkedMap(map, linkedType)) {
                  // CONVERT IT TO A LAZY MAP
                  map = new LinkMap(iSourceDocument, EntityImpl.RECORD_TYPE);
                } else if (map instanceof LinkMap && linkedType != YTType.LINK) {
                  map = new TrackedMap<Object>(iSourceDocument, map, null);
                }
              } else {
                linkedType = YTType.EMBEDDED;
              }
            } else {
              linkedType = iLinkedType;
            }

            if (linkedType == YTType.EMBEDDED && mapValue.length() >= 2) {
              mapValue = mapValue.substring(1, mapValue.length() - 1);
            }

            mapValueObject = fieldTypeFromStream(db, iSourceDocument, linkedType, mapValue);

            if (mapValueObject != null && mapValueObject instanceof EntityImpl) {
              ODocumentInternal.addOwner((EntityImpl) mapValueObject, iSourceDocument);
            }
          } else {
            mapValueObject = null;
          }

          final Object key = fieldTypeFromStream(db, iSourceDocument, YTType.STRING,
              entries.get(0));
          try {
            map.put(key, mapValueObject);
          } catch (ClassCastException e) {
            throw YTException.wrapException(
                new YTSerializationException(
                    "Cannot load map because the type was not the expected: key="
                        + key
                        + "(type "
                        + key.getClass()
                        + "), value="
                        + mapValueObject
                        + "(type "
                        + key.getClass()
                        + ")"),
                e);
          }
        }
      }
    }

    return map;
  }

  public void fieldToStream(
      final EntityImpl iRecord,
      final StringBuilder iOutput,
      final YTType iType,
      final YTClass iLinkedClass,
      final YTType iLinkedType,
      final String iName,
      final Object iValue,
      final boolean iSaveOnlyDirty) {
    if (iValue == null) {
      return;
    }

    final long timer = PROFILER.startChrono();

    switch (iType) {
      case LINK: {
        if (!(iValue instanceof YTIdentifiable)) {
          throw new YTSerializationException(
              "Found an unexpected type during marshalling of a LINK where a YTIdentifiable (YTRID"
                  + " or any Record) was expected. The string representation of the object is: "
                  + iValue);
        }

        if (!((YTIdentifiable) iValue).getIdentity().isValid()
            && iValue instanceof EntityImpl
            && ((EntityImpl) iValue).isEmbedded()) {
          // WRONG: IT'S EMBEDDED!
          fieldToStream(
              iRecord,
              iOutput,
              YTType.EMBEDDED,
              iLinkedClass,
              iLinkedType,
              iName,
              iValue,
              iSaveOnlyDirty);
        } else {
          final Object link = linkToStream(iOutput, iRecord, iValue);
          if (link != null)
          // OVERWRITE CONTENT
          {
            iRecord.field(iName, link);
          }
          PROFILER.stopChrono(
              PROFILER.getProcessMetric("serializer.record.string.link2string"),
              "Serialize link to string",
              timer);
        }
        break;
      }

      case LINKLIST: {
        iOutput.append(OStringSerializerHelper.LIST_BEGIN);
        final LinkList coll;
        final Iterator<YTIdentifiable> it;
        if (iValue instanceof OMultiCollectionIterator<?>) {
          final OMultiCollectionIterator<YTIdentifiable> iterator =
              (OMultiCollectionIterator<YTIdentifiable>) iValue;
          iterator.reset();
          it = iterator;
          coll = null;
        } else if (!(iValue instanceof LinkList)) {
          // FIRST TIME: CONVERT THE ENTIRE COLLECTION
          coll = new LinkList(iRecord);

          if (iValue.getClass().isArray()) {
            Iterable<Object> iterab = OMultiValue.getMultiValueIterable(iValue);
            for (Object i : iterab) {
              coll.add((YTIdentifiable) i);
            }
          } else {
            coll.addAll((Collection<? extends YTIdentifiable>) iValue);
            ((Collection<? extends YTIdentifiable>) iValue).clear();
          }

          iRecord.field(iName, coll);
          it = coll.rawIterator();
        } else {
          // LAZY LIST
          coll = (LinkList) iValue;
          it = coll.rawIterator();
        }

        if (it != null && it.hasNext()) {
          final StringBuilder buffer = new StringBuilder(128);
          for (int items = 0; it.hasNext(); items++) {
            if (items > 0) {
              buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);
            }

            final YTIdentifiable item = it.next();

            final YTIdentifiable newRid = linkToStream(buffer, iRecord, item);
            if (newRid != null) {
              ((OLazyIterator<YTIdentifiable>) it).update(newRid);
            }
          }

          if (coll != null) {
            coll.convertRecords2Links();
          }

          iOutput.append(buffer);
        }

        iOutput.append(OStringSerializerHelper.LIST_END);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.linkList2string"),
            "Serialize linklist to string",
            timer);
        break;
      }

      case LINKSET: {
        if (!(iValue instanceof OStringBuilderSerializable coll)) {
          final Collection<YTIdentifiable> coll;
          // FIRST TIME: CONVERT THE ENTIRE COLLECTION
          if (!(iValue instanceof LinkSet)) {
            final LinkSet set = new LinkSet(iRecord);
            set.addAll((Collection<YTIdentifiable>) iValue);
            iRecord.field(iName, set);
            coll = set;
          } else {
            coll = (Collection<YTIdentifiable>) iValue;
          }

          serializeSet(coll, iOutput);

        } else {
          // LAZY SET
          coll.toStream(iOutput);
        }

        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.linkSet2string"),
            "Serialize linkset to string",
            timer);
        break;
      }

      case LINKMAP: {
        iOutput.append(OStringSerializerHelper.MAP_BEGIN);

        Map<Object, Object> map = (Map<Object, Object>) iValue;

        boolean invalidMap = false;
        int items = 0;
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          if (items++ > 0) {
            iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);
          }

          fieldTypeToString(iOutput, YTType.STRING, entry.getKey());
          iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);
          final Object link = linkToStream(iOutput, iRecord, entry.getValue());

          if (link != null && !invalidMap)
          // IDENTITY IS CHANGED, RE-SET INTO THE COLLECTION TO RECOMPUTE THE HASH
          {
            invalidMap = true;
          }
        }

        if (invalidMap) {
          final LinkMap newMap = new LinkMap(iRecord, EntityImpl.RECORD_TYPE);

          // REPLACE ALL CHANGED ITEMS
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            newMap.put(entry.getKey(), (YTIdentifiable) entry.getValue());
          }
          map.clear();
          iRecord.field(iName, newMap);
        }

        iOutput.append(OStringSerializerHelper.MAP_END);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.linkMap2string"),
            "Serialize linkmap to string",
            timer);
        break;
      }

      case EMBEDDED:
        if (iValue instanceof Record) {
          iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
          toString((Record) iValue, iOutput, null, true);
          iOutput.append(OStringSerializerHelper.EMBEDDED_END);
        } else if (iValue instanceof ODocumentSerializable) {
          final EntityImpl doc = ((ODocumentSerializable) iValue).toDocument();
          doc.field(ODocumentSerializable.CLASS_NAME, iValue.getClass().getName());

          iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
          toString(doc, iOutput, null, true);
          iOutput.append(OStringSerializerHelper.EMBEDDED_END);

        } else if (iValue != null) {
          iOutput.append(iValue);
        }
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embed2string"),
            "Serialize embedded to string",
            timer);
        break;

      case EMBEDDEDLIST:
        embeddedCollectionToStream(
            null, iOutput, iLinkedClass, iLinkedType, iValue, iSaveOnlyDirty, false);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedList2string"),
            "Serialize embeddedlist to string",
            timer);
        break;

      case EMBEDDEDSET:
        embeddedCollectionToStream(
            null, iOutput, iLinkedClass, iLinkedType, iValue, iSaveOnlyDirty, true);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedSet2string"),
            "Serialize embeddedset to string",
            timer);
        break;

      case EMBEDDEDMAP: {
        embeddedMapToStream(null, iOutput, iLinkedClass, iLinkedType, iValue, iSaveOnlyDirty);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedMap2string"),
            "Serialize embeddedmap to string",
            timer);
        break;
      }

      case LINKBAG: {
        iOutput.append(OStringSerializerHelper.BAG_BEGIN);
        ((RidBag) iValue).toStream(iOutput);
        iOutput.append(OStringSerializerHelper.BAG_END);
        break;
      }

      default:
        fieldTypeToString(iOutput, iType, iValue);
    }
  }

  public void embeddedMapToStream(
      YTDatabaseSession iDatabase,
      final StringBuilder iOutput,
      final YTClass iLinkedClass,
      YTType iLinkedType,
      final Object iValue,
      final boolean iSaveOnlyDirty) {
    iOutput.append(OStringSerializerHelper.MAP_BEGIN);

    if (iValue != null) {
      int items = 0;
      // EMBEDDED OBJECTS
      for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
        if (items > 0) {
          iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);
        }

        if (o != null) {
          fieldTypeToString(iOutput, YTType.STRING, o.getKey());
          iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);

          if (o.getValue() instanceof EntityImpl
              && ((EntityImpl) o.getValue()).getIdentity().isValid()) {
            fieldTypeToString(iOutput, YTType.LINK, o.getValue());
          } else if (o.getValue() instanceof Record
              || o.getValue() instanceof ODocumentSerializable) {
            final EntityImpl record;
            if (o.getValue() instanceof EntityImpl) {
              record = (EntityImpl) o.getValue();
            } else if (o.getValue() instanceof ODocumentSerializable) {
              record = ((ODocumentSerializable) o.getValue()).toDocument();
              record.field(ODocumentSerializable.CLASS_NAME, o.getValue().getClass().getName());
            } else {
              record = null;
            }
            iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
            toString(record, iOutput, null, true);
            iOutput.append(OStringSerializerHelper.EMBEDDED_END);
          } else if (o.getValue() instanceof Set<?>) {
            // SUB SET
            fieldTypeToString(iOutput, YTType.EMBEDDEDSET, o.getValue());
          } else if (o.getValue() instanceof Collection<?>) {
            // SUB LIST
            fieldTypeToString(iOutput, YTType.EMBEDDEDLIST, o.getValue());
          } else if (o.getValue() instanceof Map<?, ?>) {
            // SUB MAP
            fieldTypeToString(iOutput, YTType.EMBEDDEDMAP, o.getValue());
          } else {
            // EMBEDDED LITERALS
            if (iLinkedType == null && o.getValue() != null) {
              fieldTypeToString(
                  iOutput, YTType.getTypeByClass(o.getValue().getClass()), o.getValue());
            } else {
              fieldTypeToString(iOutput, iLinkedType, o.getValue());
            }
          }
        }
        items++;
      }
    }

    iOutput.append(OStringSerializerHelper.MAP_END);
  }

  public Object embeddedCollectionFromStream(
      YTDatabaseSessionInternal db, final EntityImpl iDocument,
      final YTType iType,
      YTClass iLinkedClass,
      final YTType iLinkedType,
      final String iValue) {
    if (iValue.length() == 0) {
      return null;
    }

    // REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
    final String value;
    if (iValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
        || iValue.charAt(0) == OStringSerializerHelper.SET_BEGIN) {
      value = iValue.substring(1, iValue.length() - 1);
    } else {
      value = iValue;
    }

    Collection<?> coll;
    if (iLinkedType == YTType.LINK) {
      if (iDocument != null) {
        coll =
            (iType == YTType.EMBEDDEDLIST
                ? unserializeList(db, iDocument, value)
                : unserializeSet(db, iDocument, value));
      } else {
        if (iType == YTType.EMBEDDEDLIST) {
          coll = unserializeList(db, iDocument, value);
        } else {
          return unserializeSet(db, iDocument, value);
        }
      }
    } else {
      coll =
          iType == YTType.EMBEDDEDLIST
              ? new TrackedList<Object>(iDocument)
              : new TrackedSet<Object>(iDocument);
    }

    if (value.length() == 0) {
      return coll;
    }

    YTType linkedType;

    final List<String> items =
        OStringSerializerHelper.smartSplit(
            value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);
    for (String item : items) {
      Object objectToAdd = null;
      linkedType = null;

      if (item.equals("null"))
      // NULL VALUE
      {
        objectToAdd = null;
      } else if (item.length() > 2 && item.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
        // REMOVE EMBEDDED BEGIN/END CHARS
        item = item.substring(1, item.length() - 1);

        if (!item.isEmpty()) {
          // EMBEDDED RECORD, EXTRACT THE CLASS NAME IF DIFFERENT BY THE PASSED (SUB-CLASS OR IT WAS
          // PASSED NULL)
          iLinkedClass = OStringSerializerHelper.getRecordClassName(item, iLinkedClass);

          if (iLinkedClass != null) {
            EntityImpl doc = new EntityImpl();
            objectToAdd = fromString(db, item, doc, null);
            ODocumentInternal.fillClassNameIfNeeded(doc, iLinkedClass.getName());
          } else
          // EMBEDDED OBJECT
          {
            objectToAdd = fieldTypeFromStream(db, iDocument, YTType.EMBEDDED, item);
          }
        }
      } else {
        if (linkedType == null) {
          final char begin = item.length() > 0 ? item.charAt(0) : OStringSerializerHelper.LINK;

          // AUTO-DETERMINE LINKED TYPE
          if (begin == OStringSerializerHelper.LINK) {
            linkedType = YTType.LINK;
          } else {
            linkedType = getType(item);
          }

          if (linkedType == null) {
            throw new IllegalArgumentException(
                "Linked type cannot be null. Probably the serialized type has not stored the type"
                    + " along with data");
          }
        }

        if (iLinkedType == YTType.CUSTOM) {
          item = item.substring(1, item.length() - 1);
        }

        objectToAdd = fieldTypeFromStream(db, iDocument, linkedType, item);
      }

      if (objectToAdd != null
          && objectToAdd instanceof EntityImpl
          && coll instanceof RecordElement) {
        ODocumentInternal.addOwner((EntityImpl) objectToAdd, (RecordElement) coll);
      }

      ((Collection<Object>) coll).add(objectToAdd);
    }

    return coll;
  }

  public StringBuilder embeddedCollectionToStream(
      YTDatabaseSession iDatabase,
      final StringBuilder iOutput,
      final YTClass iLinkedClass,
      final YTType iLinkedType,
      final Object iValue,
      final boolean iSaveOnlyDirty,
      final boolean iSet) {
    iOutput.append(iSet ? OStringSerializerHelper.SET_BEGIN : OStringSerializerHelper.LIST_BEGIN);

    final Iterator<Object> iterator = (Iterator<Object>) OMultiValue.getMultiValueIterator(iValue);

    YTType linkedType = iLinkedType;

    for (int i = 0; iterator.hasNext(); ++i) {
      final Object o = iterator.next();

      if (i > 0) {
        iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);
      }

      if (o == null) {
        iOutput.append("null");
        continue;
      }

      YTIdentifiable id = null;
      EntityImpl doc = null;

      final YTClass linkedClass;
      if (!(o instanceof YTIdentifiable)) {
        if (iLinkedType == null) {
          linkedType = YTType.getTypeByClass(o.getClass());
        }

        linkedClass = iLinkedClass;
      } else {
        id = (YTIdentifiable) o;

        if (iLinkedType == null)
        // AUTO-DETERMINE LINKED TYPE
        {
          if (id.getIdentity().isValid()) {
            linkedType = YTType.LINK;
          } else {
            linkedType = YTType.EMBEDDED;
          }
        }

        if (id instanceof EntityImpl) {
          doc = (EntityImpl) id;

          if (doc.hasOwners()) {
            linkedType = YTType.EMBEDDED;
          }

          assert linkedType == YTType.EMBEDDED
              || id.getIdentity().isValid()
              || ODatabaseRecordThreadLocal.instance().get().isRemote()
              : "Impossible to serialize invalid link " + id.getIdentity();

          linkedClass = ODocumentInternal.getImmutableSchemaClass(doc);
        } else {
          linkedClass = null;
        }
      }

      if (id != null && linkedType != YTType.LINK) {
        iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
      }

      if (linkedType == YTType.EMBEDDED && o instanceof YTIdentifiable) {
        toString(((YTIdentifiable) o).getRecord(), iOutput, null);
      } else if (linkedType != YTType.LINK && (linkedClass != null || doc != null)) {
        toString(doc, iOutput, null, true);
      } else {
        // EMBEDDED LITERALS
        if (iLinkedType == null) {
          if (o != null) {
            linkedType = YTType.getTypeByClass(o.getClass());
          }
        } else if (iLinkedType == YTType.CUSTOM) {
          iOutput.append(OStringSerializerHelper.CUSTOM_TYPE);
        }
        fieldTypeToString(iOutput, linkedType, o);
      }

      if (id != null && linkedType != YTType.LINK) {
        iOutput.append(OStringSerializerHelper.EMBEDDED_END);
      }
    }

    iOutput.append(iSet ? OStringSerializerHelper.SET_END : OStringSerializerHelper.LIST_END);
    return iOutput;
  }

  protected boolean isConvertToLinkedMap(Map<?, ?> map, final YTType linkedType) {
    boolean convert = (linkedType == YTType.LINK && !(map instanceof LinkMap));
    if (convert) {
      for (Object value : map.values()) {
        if (!(value instanceof YTIdentifiable)) {
          return false;
        }
      }
    }
    return convert;
  }

  private void serializeSet(final Collection<YTIdentifiable> coll, final StringBuilder iOutput) {
    iOutput.append(OStringSerializerHelper.SET_BEGIN);
    int i = 0;
    for (YTIdentifiable rid : coll) {
      if (i++ > 0) {
        iOutput.append(',');
      }

      iOutput.append(rid.getIdentity().toString());
    }
    iOutput.append(OStringSerializerHelper.SET_END);
  }

  private LinkList unserializeList(YTDatabaseSessionInternal db, final EntityImpl iSourceRecord,
      final String value) {
    final LinkList coll = new LinkList(iSourceRecord);
    final List<String> items =
        OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);
    for (String item : items) {
      if (item.isEmpty()) {
        coll.add(new ChangeableRecordId());
      } else {
        if (item.startsWith("#")) {
          coll.add(new YTRecordId(item));
        } else {
          final Record doc = fromString(db, item);
          if (doc instanceof EntityImpl) {
            ODocumentInternal.addOwner((EntityImpl) doc, iSourceRecord);
          }

          coll.add(doc);
        }
      }
    }
    return coll;
  }

  private LinkSet unserializeSet(YTDatabaseSessionInternal db, final EntityImpl iSourceRecord,
      final String value) {
    final LinkSet coll = new LinkSet(iSourceRecord);
    final List<String> items =
        OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);
    for (String item : items) {
      if (item.isEmpty()) {
        coll.add(new ChangeableRecordId());
      } else {
        if (item.startsWith("#")) {
          coll.add(new YTRecordId(item));
        } else {
          final Record doc = fromString(db, item);
          if (doc instanceof EntityImpl) {
            ODocumentInternal.addOwner((EntityImpl) doc, iSourceRecord);
          }

          coll.add(doc);
        }
      }
    }
    return coll;
  }
}
