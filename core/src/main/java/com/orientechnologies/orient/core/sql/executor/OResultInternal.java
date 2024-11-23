package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OElementInternal;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 */
public class OResultInternal implements OResult {

  protected Map<String, Object> content;
  protected Map<String, Object> temporaryContent;
  protected Map<String, Object> metadata;
  @Nullable
  protected OIdentifiable identifiable;

  public OResultInternal() {
    content = new LinkedHashMap<>();
  }

  public OResultInternal(OIdentifiable ident) {
    setIdentifiable(ident);
  }

  public void setProperty(String name, Object value) {
    assert identifiable == null;
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (content == null) {
      throw new IllegalStateException("Impossible to mutate result set");
    }
    checkType(value);
    if (value instanceof OResult && ((OResult) value).isElement()) {
      content.put(name, ((OResult) value).getElement().get());
    } else {
      content.put(name, value);
    }
  }

  @Nullable
  @Override
  public ORID getRecordId() {
    if (identifiable == null) {
      return null;
    }

    return identifiable.getIdentity();
  }

  private void checkType(Object value) {
    if (value == null) {
      return;
    }
    if (OType.isSimpleType(value) || value instanceof Character) {
      return;
    }
    if (value instanceof OIdentifiable) {
      return;
    }
    if (value instanceof OResult) {
      return;
    }
    if (value instanceof Collection || value instanceof Map) {
      return;
    }
    if (value instanceof OSerializableStream || value instanceof Serializable) {
      return;
    }
    throw new IllegalArgumentException(
        "Invalid property value for OResult: " + value + " - " + value.getClass().getName());
  }

  public void setTemporaryProperty(String name, Object value) {
    if (temporaryContent == null) {
      temporaryContent = new HashMap<>();
    }
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (value instanceof OResult && ((OResult) value).isElement()) {
      temporaryContent.put(name, ((OResult) value).getElement().get());
    } else {
      temporaryContent.put(name, value);
    }
  }

  public Object getTemporaryProperty(String name) {
    if (name == null || temporaryContent == null) {
      return null;
    }
    return temporaryContent.get(name);
  }

  public Set<String> getTemporaryProperties() {
    return temporaryContent == null ? Collections.emptySet() : temporaryContent.keySet();
  }

  public void removeProperty(String name) {
    if (content != null) {
      content.remove(name);
    }
  }

  public <T> T getProperty(String name) {
    loadIdentifiable();
    T result = null;
    if (content != null && content.containsKey(name)) {
      result = (T) wrap(content.get(name));
    } else {
      if (isElement()) {
        result = (T) wrap(ODocumentInternal.rawPropertyRead((OElement) identifiable, name));
      }
    }
    if (result instanceof OIdentifiable && ((OIdentifiable) result).getIdentity().isPersistent()) {
      result = (T) ((OIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public OElement getElementProperty(String name) {
    loadIdentifiable();

    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isElement()) {
        result = ODocumentInternal.rawPropertyRead((OElement) identifiable, name);
      }
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? (OElement) result : null;
  }

  @Override
  public OVertex getVertexProperty(String name) {
    loadIdentifiable();
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isElement()) {
        result = ODocumentInternal.rawPropertyRead((OElement) identifiable, name);
      }
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? ((OElement) result).asVertex().orElse(null) : null;
  }

  @Override
  public OEdge getEdgeProperty(String name) {
    loadIdentifiable();
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isElement()) {
        result = ODocumentInternal.rawPropertyRead((OElement) identifiable, name);
      }
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? ((OElement) result).asEdge().orElse(null) : null;
  }

  @Override
  public OBlob getBlobProperty(String name) {
    loadIdentifiable();
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isElement()) {
        result = ODocumentInternal.rawPropertyRead((OElement) identifiable, name);
      }
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OBlob ? (OBlob) result : null;
  }

  private static Object wrap(Object input) {
    if (input instanceof OElementInternal elem && !((OElement) input).getIdentity().isValid()) {
      OResultInternal result = new OResultInternal();
      for (String prop : elem.getPropertyNamesInternal()) {
        result.setProperty(prop, elem.getPropertyInternal(prop));
      }
      elem.getSchemaType().ifPresent(x -> result.setProperty("@class", x.getName()));
      return result;
    } else {
      if (isEmbeddedList(input)) {
        return ((List) input).stream().map(OResultInternal::wrap).collect(Collectors.toList());
      } else {
        if (isEmbeddedSet(input)) {
          Stream mappedSet = ((Set) input).stream().map(OResultInternal::wrap);
          if (input instanceof LinkedHashSet<?>) {
            return mappedSet.collect(Collectors.toCollection(LinkedHashSet::new));
          } else {
            return mappedSet.collect(Collectors.toSet());
          }
        } else {
          if (isEmbeddedMap(input)) {
            Map result = new HashMap();
            for (Map.Entry<Object, Object> o : ((Map<Object, Object>) input).entrySet()) {
              result.put(o.getKey(), wrap(o.getValue()));
            }
            return result;
          }
        }
      }
    }
    return input;
  }

  private static boolean isEmbeddedSet(Object input) {
    return input instanceof Set && OType.getTypeByValue(input) == OType.EMBEDDEDSET;
  }

  private static boolean isEmbeddedMap(Object input) {
    return input instanceof Map && OType.getTypeByValue(input) == OType.EMBEDDEDMAP;
  }

  private static boolean isEmbeddedList(Object input) {
    return input instanceof List && OType.getTypeByValue(input) == OType.EMBEDDEDLIST;
  }

  public Collection<String> getPropertyNames() {
    loadIdentifiable();
    if (isElement()) {
      return ((OElement) identifiable).getPropertyNames();
    } else {
      if (content != null) {
        return new LinkedHashSet<>(content.keySet());
      } else {
        return Collections.emptySet();
      }
    }
  }

  public boolean hasProperty(String propName) {
    loadIdentifiable();
    if (isElement() && ((OElement) identifiable).hasProperty(propName)) {
      return true;
    }
    if (content != null) {
      return content.containsKey(propName);
    }
    return false;
  }

  @Override
  public boolean isElement() {
    if (identifiable == null) {
      return false;
    }

    if (identifiable instanceof OElement) {
      return true;
    }

    try {
      identifiable = identifiable.getRecord();
    } catch (ORecordNotFoundException e) {
      identifiable = null;
    }

    return identifiable instanceof OElement;
  }

  public Optional<OElement> getElement() {
    loadIdentifiable();
    if (isElement()) {
      return Optional.ofNullable((OElement) identifiable);
    }
    return Optional.empty();
  }

  @Override
  public OElement asElement() {
    loadIdentifiable();
    if (isElement()) {
      return (OElement) identifiable;
    }

    return null;
  }

  @Override
  public OElement toElement() {
    if (isElement()) {
      return getElement().get();
    }
    ODocument doc = new ODocument();
    for (String s : getPropertyNames()) {
      if (s == null) {
        continue;
      } else {
        if (s.equalsIgnoreCase("@rid")) {
          Object newRid = getProperty(s);
          if (newRid instanceof OIdentifiable) {
            newRid = ((OIdentifiable) newRid).getIdentity();
          } else {
            continue;
          }
          ORecordId oldId = (ORecordId) doc.getIdentity();
          oldId.setClusterId(((ORID) newRid).getClusterId());
          oldId.setClusterPosition(((ORID) newRid).getClusterPosition());
        } else {
          if (s.equalsIgnoreCase("@version")) {
            Object v = getProperty(s);
            if (v instanceof Number) {
              ORecordInternal.setVersion(doc, ((Number) v).intValue());
            } else {
              continue;
            }
          } else {
            if (s.equalsIgnoreCase("@class")) {
              doc.setClassName(getProperty(s));
            } else {
              doc.setProperty(s, convertToElement(getProperty(s)));
            }
          }
        }
      }
    }
    return doc;
  }

  @Override
  public Optional<ORID> getIdentity() {
    if (identifiable != null) {
      return Optional.of(identifiable.getIdentity());
    }
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return this.identifiable == null;
  }

  @Override
  public Optional<ORecord> getRecord() {
    loadIdentifiable();
    return Optional.ofNullable((ORecord) this.identifiable);
  }

  @Override
  public boolean isBlob() {
    loadIdentifiable();
    return this.identifiable instanceof OBlob;
  }

  @Override
  public Optional<OBlob> getBlob() {
    loadIdentifiable();

    if (isBlob()) {
      return Optional.ofNullable((OBlob) this.identifiable);
    }
    return Optional.empty();
  }

  @Override
  public Object getMetadata(String key) {
    if (key == null) {
      return null;
    }
    return metadata == null ? null : metadata.get(key);
  }

  public void setMetadata(String key, Object value) {
    if (key == null) {
      return;
    }
    checkType(value);
    if (metadata == null) {
      metadata = new HashMap<>();
    }
    metadata.put(key, value);
  }

  public void clearMetadata() {
    metadata = null;
  }

  public void removeMetadata(String key) {
    if (key == null || metadata == null) {
      return;
    }
    metadata.remove(key);
  }

  public void addMetadata(Map<String, Object> values) {
    if (values == null) {
      return;
    }
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.putAll(values);
  }

  @Override
  public Set<String> getMetadataKeys() {
    return metadata == null ? Collections.emptySet() : metadata.keySet();
  }

  private Object convertToElement(Object property) {
    if (property instanceof OResult) {
      return ((OResult) property).toElement();
    }
    if (property instanceof List) {
      return ((List) property).stream().map(x -> convertToElement(x)).collect(Collectors.toList());
    }

    if (property instanceof Set) {
      return ((Set) property).stream().map(x -> convertToElement(x)).collect(Collectors.toSet());
    }

    if (property instanceof Map) {
      Map<Object, Object> result = new HashMap<>();
      Map<Object, Object> prop = ((Map) property);
      for (Map.Entry<Object, Object> o : prop.entrySet()) {
        result.put(o.getKey(), convertToElement(o.getValue()));
      }
    }

    return property;
  }

  public void loadIdentifiable() {
    if (identifiable == null) {
      return;
    }

    if (identifiable instanceof OElement elem) {
      if (elem.isUnloaded()) {
        var id = elem.getIdentity();
        if (id != null && id.isValid()) {
          try {
            identifiable = ODatabaseSessionInternal.getActiveSession().bindToSession(elem);
          } catch (ORecordNotFoundException rnf) {
            identifiable = null;
          }
        }
      }

      return;
    }

    if (identifiable instanceof OContextualRecordId) {
      this.addMetadata(((OContextualRecordId) identifiable).getContext());
    }

    try {
      this.identifiable = identifiable.getRecord();
    } catch (ORecordNotFoundException rnf) {
      identifiable = null;
    }
  }

  public void setIdentifiable(OIdentifiable identifiable) {
    this.identifiable = identifiable;
    this.content = null;
  }

  @Override
  public String toString() {
    if (identifiable != null) {
      return identifiable.toString();
    }
    return "{\n"
        + content.entrySet().stream()
        .map(x -> x.getKey() + ": " + x.getValue())
        .reduce("", (a, b) -> a + b + "\n")
        + "}\n";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OResultInternal resultObj)) {
      return false;
    }
    if (identifiable != null) {
      if (!resultObj.getElement().isPresent()) {
        return false;
      }
      return identifiable.equals(resultObj.getElement().get());
    } else {
      if (resultObj.getElement().isPresent()) {
        return false;
      }
      if (content != null) {
        return this.content.equals(resultObj.content);
      } else {
        return resultObj.content == null;
      }
    }
  }

  @Override
  public int hashCode() {
    if (identifiable != null) {
      return identifiable.hashCode();
    }
    if (content != null) {
      return content.hashCode();
    } else {
      return super.hashCode();
    }
  }

  public void bindToCache(ODatabaseSessionInternal db) {
    if (isRecord()) {
      ORecordAbstract rec = identifiable.getRecord();
      var identity = rec.getIdentity();
      ORecordAbstract cached = db.getLocalCache().findRecord(identity);

      if (cached == rec) {
        return;
      }

      if (cached != null) {
        if (!cached.isDirty()) {
          rec.copyTo(cached);
        }
        identifiable = cached;
      } else {
        if (!identity.isPersistent()) {
          return;
        }

        db.getLocalCache().updateRecord(rec);
      }
    }
  }
}
