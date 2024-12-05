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

package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.RecordElement;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.metadata.security.PropertyAccess;
import com.orientechnologies.orient.core.metadata.security.PropertyEncryption;
import com.orientechnologies.orient.core.record.YTEntity;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class ODocumentInternal {

  public static void convertAllMultiValuesToTrackedVersions(YTEntityImpl document) {
    document.convertAllMultiValuesToTrackedVersions();
  }

  public static void addOwner(YTEntityImpl oDocument, RecordElement iOwner) {
    oDocument.addOwner(iOwner);
  }

  public static void removeOwner(YTEntityImpl oDocument, RecordElement iOwner) {
    oDocument.removeOwner(iOwner);
  }

  public static void rawField(
      final YTEntityImpl oDocument,
      final String iFieldName,
      final Object iFieldValue,
      final YTType iFieldType) {
    oDocument.rawField(iFieldName, iFieldValue, iFieldType);
  }

  public static boolean rawContainsField(final YTEntityImpl oDocument, final String iFiledName) {
    return oDocument.rawContainsField(iFiledName);
  }

  public static YTImmutableClass getImmutableSchemaClass(
      final YTDatabaseSessionInternal database, final YTEntityImpl oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchemaClass(database);
  }

  public static YTImmutableClass getImmutableSchemaClass(final YTEntityImpl oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchemaClass();
  }

  public static YTImmutableSchema getImmutableSchema(final YTEntityImpl oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchema();
  }

  public static OGlobalProperty getGlobalPropertyById(final YTEntityImpl oDocument, final int id) {
    return oDocument.getGlobalPropertyById(id);
  }

  public static void fillClassNameIfNeeded(final YTEntityImpl oDocument, String className) {
    oDocument.fillClassIfNeed(className);
  }

  public static Set<Entry<String, EntityEntry>> rawEntries(final YTEntityImpl document) {
    return document.getRawEntries();
  }

  public static EntityEntry rawEntry(final YTEntityImpl document, String propertyName) {
    return document.fields.get(propertyName);
  }

  public static List<Entry<String, EntityEntry>> filteredEntries(final YTEntityImpl document) {
    return document.getFilteredEntries();
  }

  public static void clearTrackData(final YTEntityImpl document) {
    document.clearTrackData();
  }

  public static void checkClass(YTEntityImpl doc, YTDatabaseSessionInternal database) {
    doc.checkClass(database);
  }

  public static void autoConvertValueToClass(YTDatabaseSessionInternal database, YTEntityImpl doc) {
    doc.autoConvertFieldsToClass(database);
  }

  public static Object getRawProperty(YTEntityImpl doc, String propertyName) {
    if (doc == null) {
      return null;
    }
    return doc.getRawProperty(propertyName);
  }

  public static <RET> RET rawPropertyRead(YTEntity element, String propertyName) {
    if (element == null) {
      return null;
    }
    return ((YTEntityImpl) element).rawField(propertyName);
  }

  public static void setPropertyAccess(YTEntityImpl doc, PropertyAccess propertyAccess) {
    doc.propertyAccess = propertyAccess;
  }

  public static PropertyAccess getPropertyAccess(YTEntityImpl doc) {
    return doc.propertyAccess;
  }

  public static void setPropertyEncryption(YTEntityImpl doc,
      PropertyEncryption propertyEncryption) {
    doc.propertyEncryption = propertyEncryption;
  }

  public static PropertyEncryption getPropertyEncryption(YTEntityImpl doc) {
    return doc.propertyEncryption;
  }

  public static void clearTransactionTrackData(YTEntityImpl doc) {
    doc.clearTransactionTrackData();
  }

  public static Iterator<String> iteratePropertieNames(YTEntityImpl doc) {
    return doc.calculatePropertyNames().iterator();
  }
}
