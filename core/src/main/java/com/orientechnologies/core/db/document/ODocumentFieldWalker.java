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
package com.orientechnologies.core.db.document;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.OIdentifiableMultiValue;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class allows to walk through all fields of single document using instance of
 * {@link ODocumentFieldVisitor} class.
 *
 * <p>Only current document and embedded documents will be walked. Which means that all embedded
 * collections will be visited too and all embedded documents which are contained in this
 * collections also will be visited.
 *
 * <p>Fields values can be updated/converted too. If method {@link
 * ODocumentFieldVisitor#visitField(YTDatabaseSessionInternal,
 * YTType, YTType, Object)} will return new value original value will be updated but returned result
 * will not be visited by {@link ODocumentFieldVisitor} instance.
 *
 * <p>If currently processed value is collection or map of embedded documents or embedded document
 * itself then method {@link ODocumentFieldVisitor#goDeeper(YTType, YTType, Object)} is called, if it
 * returns false then this collection will not be visited by {@link ODocumentFieldVisitor}
 * instance.
 *
 * <p>Fields will be visited till method {@link ODocumentFieldVisitor#goFurther(YTType, YTType,
 * Object, Object)} returns true.
 */
public class ODocumentFieldWalker {

  public YTEntityImpl walkDocument(
      YTDatabaseSessionInternal session, YTEntityImpl document, ODocumentFieldVisitor fieldWalker) {
    final Set<YTEntityImpl> walked = Collections.newSetFromMap(new IdentityHashMap<>());

    YTEntityImpl doc;
    if (document.getIdentity().isValid()) {
      doc = session.bindToSession(document);
    } else {
      doc = document;
    }

    walkDocument(session, doc, fieldWalker, walked);
    walked.clear();
    return doc;
  }

  private void walkDocument(
      YTDatabaseSessionInternal session,
      YTEntityImpl document,
      ODocumentFieldVisitor fieldWalker,
      Set<YTEntityImpl> walked) {
    if (document.isUnloaded()) {
      throw new IllegalStateException("Document is unloaded");
    }

    if (walked.contains(document)) {
      return;
    }

    walked.add(document);
    boolean oldLazyLoad = document.isLazyLoad();
    document.setLazyLoad(false);

    final boolean updateMode = fieldWalker.updateMode();

    final YTClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    for (String fieldName : document.fieldNames()) {

      final YTType concreteType = document.fieldType(fieldName);
      YTType fieldType = concreteType;

      YTType linkedType = null;
      if (fieldType == null && clazz != null) {
        YTProperty property = clazz.getProperty(fieldName);
        if (property != null) {
          fieldType = property.getType();
          linkedType = property.getLinkedType();
        }
      }

      Object fieldValue = document.field(fieldName, fieldType);
      Object newValue = fieldWalker.visitField(session, fieldType, linkedType, fieldValue);

      boolean updated;
      if (updateMode) {
        updated =
            updateFieldValueIfChanged(document, fieldName, fieldValue, newValue, concreteType);
      } else {
        updated = false;
      }

      // exclude cases when:
      // 1. value was updated.
      // 2. we use link types.
      // 3. document is not not embedded.
      if (!updated
          && fieldValue != null
          && !(YTType.LINK.equals(fieldType)
          || YTType.LINKBAG.equals(fieldType)
          || YTType.LINKLIST.equals(fieldType)
          || YTType.LINKSET.equals(fieldType)
          || (fieldValue instanceof OIdentifiableMultiValue))) {
        if (fieldWalker.goDeeper(fieldType, linkedType, fieldValue)) {
          if (fieldValue instanceof Map) {
            walkMap(session, (Map) fieldValue, fieldType, fieldWalker, walked);
          } else if (fieldValue instanceof YTEntityImpl doc) {
            if (YTType.EMBEDDED.equals(fieldType) || doc.isEmbedded()) {
              var fdoc = (YTEntityImpl) fieldValue;
              if (fdoc.isUnloaded()) {
                throw new IllegalStateException("Document is unloaded");
              }
              walkDocument(session, fdoc, fieldWalker);
            }
          } else if (com.orientechnologies.common.collection.OMultiValue.isIterable(fieldValue)) {
            walkIterable(
                session,
                com.orientechnologies.common.collection.OMultiValue.getMultiValueIterable(
                    fieldValue),
                fieldType,
                fieldWalker,
                walked);
          }
        }
      }

      if (!fieldWalker.goFurther(fieldType, linkedType, fieldValue, newValue)) {
        document.setLazyLoad(oldLazyLoad);
        return;
      }
    }

    document.setLazyLoad(oldLazyLoad);
  }

  private void walkMap(
      YTDatabaseSessionInternal session,
      Map map,
      YTType fieldType,
      ODocumentFieldVisitor fieldWalker,
      Set<YTEntityImpl> walked) {
    for (Object value : map.values()) {
      if (value instanceof YTEntityImpl doc) {
        // only embedded documents are walked
        if (YTType.EMBEDDEDMAP.equals(fieldType) || doc.isEmbedded()) {
          walkDocument(session, (YTEntityImpl) value, fieldWalker, walked);
        }
      }
    }
  }

  private void walkIterable(
      YTDatabaseSessionInternal session,
      Iterable iterable,
      YTType fieldType,
      ODocumentFieldVisitor fieldWalker,
      Set<YTEntityImpl> walked) {
    for (Object value : iterable) {
      if (value instanceof YTEntityImpl doc) {
        // only embedded documents are walked
        if (YTType.EMBEDDEDLIST.equals(fieldType)
            || YTType.EMBEDDEDSET.equals(fieldType)
            || doc.isEmbedded()) {
          walkDocument(session, (YTEntityImpl) value, fieldWalker, walked);
        }
      }
    }
  }

  private static boolean updateFieldValueIfChanged(
      YTEntityImpl document,
      String fieldName,
      Object fieldValue,
      Object newValue,
      YTType concreteType) {
    if (fieldValue != newValue) {
      document.field(fieldName, newValue, concreteType);
      return true;
    }

    return false;
  }
}
