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
package com.jetbrains.youtrack.db.internal.core.db.document;

import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.IdentifiableMultiValue;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Property;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentInternal;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class allows to walk through all fields of single document using instance of
 * {@link EntityPropertiesVisitor} class.
 *
 * <p>Only current document and embedded documents will be walked. Which means that all embedded
 * collections will be visited too and all embedded documents which are contained in this
 * collections also will be visited.
 *
 * <p>Fields values can be updated/converted too. If method {@link
 * EntityPropertiesVisitor#visitField(DatabaseSessionInternal, PropertyType, PropertyType, Object)}
 * will return new value original value will be updated but returned result will not be visited by
 * {@link EntityPropertiesVisitor} instance.
 *
 * <p>If currently processed value is collection or map of embedded documents or embedded document
 * itself then method {@link EntityPropertiesVisitor#goDeeper(PropertyType, PropertyType, Object)}
 * is called, if it returns false then this collection will not be visited by
 * {@link EntityPropertiesVisitor} instance.
 *
 * <p>Fields will be visited till method
 * {@link EntityPropertiesVisitor#goFurther(PropertyType, PropertyType,
 * Object, Object)} returns true.
 */
public class EntityFieldWalker {

  public EntityImpl walkDocument(
      DatabaseSessionInternal session, EntityImpl document, EntityPropertiesVisitor fieldWalker) {
    final Set<EntityImpl> walked = Collections.newSetFromMap(new IdentityHashMap<>());

    EntityImpl doc;
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
      DatabaseSessionInternal session,
      EntityImpl document,
      EntityPropertiesVisitor fieldWalker,
      Set<EntityImpl> walked) {
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

    final SchemaClass clazz = DocumentInternal.getImmutableSchemaClass(document);
    for (String fieldName : document.fieldNames()) {

      final PropertyType concreteType = document.fieldType(fieldName);
      PropertyType fieldType = concreteType;

      PropertyType linkedType = null;
      if (fieldType == null && clazz != null) {
        Property property = clazz.getProperty(fieldName);
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
          && !(PropertyType.LINK.equals(fieldType)
          || PropertyType.LINKBAG.equals(fieldType)
          || PropertyType.LINKLIST.equals(fieldType)
          || PropertyType.LINKSET.equals(fieldType)
          || (fieldValue instanceof IdentifiableMultiValue))) {
        if (fieldWalker.goDeeper(fieldType, linkedType, fieldValue)) {
          if (fieldValue instanceof Map) {
            walkMap(session, (Map) fieldValue, fieldType, fieldWalker, walked);
          } else if (fieldValue instanceof EntityImpl doc) {
            if (PropertyType.EMBEDDED.equals(fieldType) || doc.isEmbedded()) {
              var fdoc = (EntityImpl) fieldValue;
              if (fdoc.isUnloaded()) {
                throw new IllegalStateException("Document is unloaded");
              }
              walkDocument(session, fdoc, fieldWalker);
            }
          } else if (MultiValue.isIterable(fieldValue)) {
            walkIterable(
                session,
                MultiValue.getMultiValueIterable(
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
      DatabaseSessionInternal session,
      Map map,
      PropertyType fieldType,
      EntityPropertiesVisitor fieldWalker,
      Set<EntityImpl> walked) {
    for (Object value : map.values()) {
      if (value instanceof EntityImpl doc) {
        // only embedded documents are walked
        if (PropertyType.EMBEDDEDMAP.equals(fieldType) || doc.isEmbedded()) {
          walkDocument(session, (EntityImpl) value, fieldWalker, walked);
        }
      }
    }
  }

  private void walkIterable(
      DatabaseSessionInternal session,
      Iterable iterable,
      PropertyType fieldType,
      EntityPropertiesVisitor fieldWalker,
      Set<EntityImpl> walked) {
    for (Object value : iterable) {
      if (value instanceof EntityImpl doc) {
        // only embedded documents are walked
        if (PropertyType.EMBEDDEDLIST.equals(fieldType)
            || PropertyType.EMBEDDEDSET.equals(fieldType)
            || doc.isEmbedded()) {
          walkDocument(session, (EntityImpl) value, fieldWalker, walked);
        }
      }
    }
  }

  private static boolean updateFieldValueIfChanged(
      EntityImpl document,
      String fieldName,
      Object fieldValue,
      Object newValue,
      PropertyType concreteType) {
    if (fieldValue != newValue) {
      document.field(fieldName, newValue, concreteType);
      return true;
    }

    return false;
  }
}
