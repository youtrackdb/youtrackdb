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

package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.GlobalProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ImmutableSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.PropertyAccess;
import com.jetbrains.youtrack.db.internal.core.metadata.security.PropertyEncryption;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class DocumentInternal {

  public static void convertAllMultiValuesToTrackedVersions(EntityImpl document) {
    document.convertAllMultiValuesToTrackedVersions();
  }

  public static void addOwner(EntityImpl oDocument, RecordElement iOwner) {
    oDocument.addOwner(iOwner);
  }

  public static void removeOwner(EntityImpl oDocument, RecordElement iOwner) {
    oDocument.removeOwner(iOwner);
  }

  public static void rawField(
      final EntityImpl oDocument,
      final String iFieldName,
      final Object iFieldValue,
      final PropertyType iFieldType) {
    oDocument.rawField(iFieldName, iFieldValue, iFieldType);
  }

  public static boolean rawContainsField(final EntityImpl oDocument, final String iFiledName) {
    return oDocument.rawContainsField(iFiledName);
  }

  public static SchemaImmutableClass getImmutableSchemaClass(
      final DatabaseSessionInternal database, final EntityImpl oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchemaClass(database);
  }

  public static SchemaImmutableClass getImmutableSchemaClass(final EntityImpl oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchemaClass();
  }

  public static ImmutableSchema getImmutableSchema(final EntityImpl oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchema();
  }

  public static GlobalProperty getGlobalPropertyById(final EntityImpl oDocument, final int id) {
    return oDocument.getGlobalPropertyById(id);
  }

  public static void fillClassNameIfNeeded(final EntityImpl oDocument, String className) {
    oDocument.fillClassIfNeed(className);
  }

  public static Set<Entry<String, EntityEntry>> rawEntries(final EntityImpl document) {
    return document.getRawEntries();
  }

  public static EntityEntry rawEntry(final EntityImpl document, String propertyName) {
    return document.fields.get(propertyName);
  }

  public static List<Entry<String, EntityEntry>> filteredEntries(final EntityImpl document) {
    return document.getFilteredEntries();
  }

  public static void clearTrackData(final EntityImpl document) {
    document.clearTrackData();
  }

  public static void checkClass(EntityImpl doc, DatabaseSessionInternal database) {
    doc.checkClass(database);
  }

  public static void autoConvertValueToClass(DatabaseSessionInternal database, EntityImpl doc) {
    doc.autoConvertFieldsToClass(database);
  }

  public static Object getRawProperty(EntityImpl doc, String propertyName) {
    if (doc == null) {
      return null;
    }
    return doc.getRawProperty(propertyName);
  }

  public static <RET> RET rawPropertyRead(Entity element, String propertyName) {
    if (element == null) {
      return null;
    }
    return ((EntityImpl) element).rawField(propertyName);
  }

  public static void setPropertyAccess(EntityImpl doc, PropertyAccess propertyAccess) {
    doc.propertyAccess = propertyAccess;
  }

  public static PropertyAccess getPropertyAccess(EntityImpl doc) {
    return doc.propertyAccess;
  }

  public static void setPropertyEncryption(EntityImpl doc,
      PropertyEncryption propertyEncryption) {
    doc.propertyEncryption = propertyEncryption;
  }

  public static PropertyEncryption getPropertyEncryption(EntityImpl doc) {
    return doc.propertyEncryption;
  }

  public static void clearTransactionTrackData(EntityImpl doc) {
    doc.clearTransactionTrackData();
  }

  public static Iterator<String> iteratePropertieNames(EntityImpl doc) {
    return doc.calculatePropertyNames().iterator();
  }
}
