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
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Helper class to find reference in records.
 */
public class FindReferenceHelper {

  public static List<EntityImpl> findReferences(final Set<RID> iRecordIds,
      final String classList) {
    final var db = DatabaseRecordThreadLocal.instance().get();

    final Map<RID, Set<RID>> map = new HashMap<RID, Set<RID>>();
    for (RID rid : iRecordIds) {
      map.put(rid, new HashSet<>());
    }

    if (classList == null || classList.isEmpty()) {
      for (String clusterName : db.getClusterNames()) {
        browseCluster(db, iRecordIds, map, clusterName);
      }
    } else {
      final List<String> classes = StringSerializerHelper.smartSplit(classList, ',');
      for (String clazz : classes) {
        if (clazz.startsWith("CLUSTER:")) {
          browseCluster(
              db,
              iRecordIds,
              map,
              clazz.substring(clazz.indexOf("CLUSTER:") + "CLUSTER:".length()));
        } else {
          browseClass(db, iRecordIds, map, clazz);
        }
      }
    }

    final List<EntityImpl> result = new ArrayList<EntityImpl>();
    for (Entry<RID, Set<RID>> entry : map.entrySet()) {
      final EntityImpl entity = new EntityImpl();
      result.add(entity);

      entity.field("rid", entry.getKey());
      entity.field("referredBy", entry.getValue());
    }

    return result;
  }

  private static void browseCluster(
      final DatabaseSession iDatabase,
      final Set<RID> iSourceRIDs,
      final Map<RID, Set<RID>> map,
      final String iClusterName) {
    for (Record record : ((DatabaseSessionInternal) iDatabase).browseCluster(iClusterName)) {
      if (record instanceof EntityImpl) {
        try {
          for (String fieldName : ((EntityImpl) record).fieldNames()) {
            Object value = ((EntityImpl) record).field(fieldName);
            checkObject(iSourceRIDs, map, value, record);
          }
        } catch (Exception e) {
          LogManager.instance()
              .debug(FindReferenceHelper.class, "Error reading record " + record.getIdentity(), e);
        }
      }
    }
  }

  private static void browseClass(
      final DatabaseSessionInternal db,
      Set<RID> iSourceRIDs,
      final Map<RID, Set<RID>> map,
      final String iClassName) {
    final SchemaClass clazz = db.getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (clazz == null) {
      throw new CommandExecutionException("Class '" + iClassName + "' was not found");
    }

    for (int i : clazz.getClusterIds()) {
      browseCluster(db, iSourceRIDs, map, db.getClusterNameById(i));
    }
  }

  private static void checkObject(
      final Set<RID> iSourceRIDs,
      final Map<RID, Set<RID>> map,
      final Object value,
      final Record iRootObject) {
    if (value instanceof Identifiable) {
      checkRecord(iSourceRIDs, map, (Identifiable) value, iRootObject);
    } else if (value instanceof Collection<?>) {
      checkCollection(iSourceRIDs, map, (Collection<?>) value, iRootObject);
    } else if (value instanceof Map<?, ?>) {
      checkMap(iSourceRIDs, map, (Map<?, ?>) value, iRootObject);
    }
  }

  private static void checkCollection(
      final Set<RID> iSourceRIDs,
      final Map<RID, Set<RID>> map,
      final Collection<?> values,
      final Record iRootObject) {
    for (Object value : values) {
      checkObject(iSourceRIDs, map, value, iRootObject);
    }
  }

  private static void checkMap(
      final Set<RID> iSourceRIDs,
      final Map<RID, Set<RID>> map,
      final Map<?, ?> values,
      final Record iRootObject) {
    final Iterator<?> it;
    if (values instanceof LinkMap) {
      it = ((LinkMap) values).rawIterator();
    } else {
      it = values.values().iterator();
    }
    while (it.hasNext()) {
      checkObject(iSourceRIDs, map, it.next(), iRootObject);
    }
  }

  private static void checkRecord(
      final Set<RID> iSourceRIDs,
      final Map<RID, Set<RID>> map,
      final Identifiable value,
      final Record iRootObject) {
    if (iSourceRIDs.contains(value.getIdentity())) {
      map.get(value.getIdentity()).add(iRootObject.getIdentity());
    } else if (!((RecordId) value.getIdentity()).isValid()
        && value.getRecord() instanceof EntityImpl) {
      // embedded entity
      EntityImpl entity = value.getRecord();
      for (String fieldName : entity.fieldNames()) {
        Object fieldValue = entity.field(fieldName);
        checkObject(iSourceRIDs, map, fieldValue, iRootObject);
      }
    }
  }
}
