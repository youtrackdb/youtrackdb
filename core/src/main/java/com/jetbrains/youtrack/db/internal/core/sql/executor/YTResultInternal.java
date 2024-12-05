package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTContextualRecordId;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.Blob;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
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
public class YTResultInternal implements YTResult {

  protected Map<String, Object> content;
  protected Map<String, Object> temporaryContent;
  protected Map<String, Object> metadata;

  @Nullable
  protected YTIdentifiable identifiable;

  @Nullable
  protected YTDatabaseSessionInternal session;

  public YTResultInternal(@Nullable YTDatabaseSessionInternal session) {
    content = new LinkedHashMap<>();
    this.session = session;
  }

  public YTResultInternal(@Nullable YTDatabaseSessionInternal session, YTIdentifiable ident) {
    setIdentifiable(ident);
    this.session = session;
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
    if (value instanceof YTResult && ((YTResult) value).isEntity()) {
      content.put(name, ((YTResult) value).getEntity().get());
    } else {
      content.put(name, value);
    }
  }

  @Override
  public boolean isRecord() {
    return identifiable != null;
  }

  @Nullable
  @Override
  public YTRID getRecordId() {
    if (identifiable == null) {
      return null;
    }

    return identifiable.getIdentity();
  }

  private static void checkType(Object value) {
    if (value == null) {
      return;
    }
    if (YTType.isSimpleType(value) || value instanceof Character) {
      return;
    }
    if (value instanceof YTIdentifiable) {
      return;
    }
    if (value instanceof YTResult) {
      return;
    }
    if (value instanceof Collection || value instanceof Map) {
      return;
    }
    if (value instanceof Serializable) {
      return;
    }
    throw new IllegalArgumentException(
        "Invalid property value for YTResult: " + value + " - " + value.getClass().getName());
  }

  public void setTemporaryProperty(String name, Object value) {
    if (temporaryContent == null) {
      temporaryContent = new HashMap<>();
    }
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (value instanceof YTResult && ((YTResult) value).isEntity()) {
      temporaryContent.put(name, ((YTResult) value).getEntity().get());
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
      result = (T) wrap(session, content.get(name));
    } else {
      if (isEntity()) {
        result = (T) wrap(session,
            ODocumentInternal.rawPropertyRead((Entity) identifiable, name));
      }
    }
    if (result instanceof YTIdentifiable && ((YTIdentifiable) result).getIdentity()
        .isPersistent()) {
      result = (T) ((YTIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public Entity getEntityProperty(String name) {
    loadIdentifiable();

    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isEntity()) {
        result = ODocumentInternal.rawPropertyRead((Entity) identifiable, name);
      }
    }

    if (result instanceof YTResult) {
      result = ((YTResult) result).getRecord().orElse(null);
    }

    if (result instanceof YTRID) {
      result = ((YTRID) result).getRecord();
    }

    return result instanceof Entity ? (Entity) result : null;
  }

  @Override
  public Vertex getVertexProperty(String name) {
    loadIdentifiable();
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isEntity()) {
        result = ODocumentInternal.rawPropertyRead((Entity) identifiable, name);
      }
    }

    if (result instanceof YTResult) {
      result = ((YTResult) result).getRecord().orElse(null);
    }

    if (result instanceof YTRID) {
      result = ((YTRID) result).getRecord();
    }

    return result instanceof Entity ? ((Entity) result).asVertex().orElse(null) : null;
  }

  @Override
  public Edge getEdgeProperty(String name) {
    loadIdentifiable();
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isEntity()) {
        result = ODocumentInternal.rawPropertyRead((Entity) identifiable, name);
      }
    }

    if (result instanceof YTResult) {
      result = ((YTResult) result).getRecord().orElse(null);
    }

    if (result instanceof YTRID) {
      result = ((YTRID) result).getRecord();
    }

    return result instanceof Entity ? ((Entity) result).asEdge().orElse(null) : null;
  }

  @Override
  public Blob getBlobProperty(String name) {
    loadIdentifiable();
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else {
      if (isEntity()) {
        result = ODocumentInternal.rawPropertyRead((Entity) identifiable, name);
      }
    }

    if (result instanceof YTResult) {
      result = ((YTResult) result).getRecord().orElse(null);
    }

    if (result instanceof YTRID) {
      result = ((YTRID) result).getRecord();
    }

    return result instanceof Blob ? (Blob) result : null;
  }

  private static Object wrap(YTDatabaseSessionInternal session, Object input) {
    if (input instanceof EntityInternal elem && !((Entity) input).getIdentity().isValid()) {
      YTResultInternal result = new YTResultInternal(session);
      for (String prop : elem.getPropertyNamesInternal()) {
        result.setProperty(prop, elem.getPropertyInternal(prop));
      }
      elem.getSchemaType().ifPresent(x -> result.setProperty("@class", x.getName()));
      return result;
    } else {
      if (isEmbeddedList(input)) {
        return ((List) input).stream().map(in -> wrap(session, in)).collect(Collectors.toList());
      } else {
        if (isEmbeddedSet(input)) {
          Stream mappedSet = ((Set) input).stream().map(in -> wrap(session, in));
          if (input instanceof LinkedHashSet<?>) {
            return mappedSet.collect(Collectors.toCollection(LinkedHashSet::new));
          } else {
            return mappedSet.collect(Collectors.toSet());
          }
        } else {
          if (isEmbeddedMap(input)) {
            Map result = new HashMap();
            for (Map.Entry<Object, Object> o : ((Map<Object, Object>) input).entrySet()) {
              result.put(o.getKey(), wrap(session, o.getValue()));
            }
            return result;
          }
        }
      }
    }
    return input;
  }

  private static boolean isEmbeddedSet(Object input) {
    return input instanceof Set && YTType.getTypeByValue(input) == YTType.EMBEDDEDSET;
  }

  private static boolean isEmbeddedMap(Object input) {
    return input instanceof Map && YTType.getTypeByValue(input) == YTType.EMBEDDEDMAP;
  }

  private static boolean isEmbeddedList(Object input) {
    return input instanceof List && YTType.getTypeByValue(input) == YTType.EMBEDDEDLIST;
  }

  public Collection<String> getPropertyNames() {
    loadIdentifiable();
    if (isEntity()) {
      return ((Entity) identifiable).getPropertyNames();
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
    if (isEntity() && ((Entity) identifiable).hasProperty(propName)) {
      return true;
    }
    if (content != null) {
      return content.containsKey(propName);
    }
    return false;
  }

  @Override
  public boolean isEntity() {
    if (identifiable == null) {
      return false;
    }

    if (identifiable instanceof Entity) {
      return true;
    }

    try {
      identifiable = identifiable.getRecord();
    } catch (YTRecordNotFoundException e) {
      identifiable = null;
    }

    return identifiable instanceof Entity;
  }

  public Optional<Entity> getEntity() {
    loadIdentifiable();
    if (isEntity()) {
      return Optional.ofNullable((Entity) identifiable);
    }
    return Optional.empty();
  }

  @Override
  public Entity asEntity() {
    loadIdentifiable();
    if (isEntity()) {
      return (Entity) identifiable;
    }

    return null;
  }

  @Override
  public Entity toEntity() {
    if (isEntity()) {
      return getEntity().get();
    }
    EntityImpl doc = new EntityImpl();
    for (String s : getPropertyNames()) {
      if (s == null) {
        continue;
      } else {
        if (s.equalsIgnoreCase("@rid")) {
          Object newRid = getProperty(s);
          if (newRid instanceof YTIdentifiable) {
            newRid = ((YTIdentifiable) newRid).getIdentity();
          } else {
            continue;
          }
          YTRecordId oldId = (YTRecordId) doc.getIdentity();
          oldId.setClusterId(((YTRID) newRid).getClusterId());
          oldId.setClusterPosition(((YTRID) newRid).getClusterPosition());
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
  public Optional<YTRID> getIdentity() {
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
  public Optional<Record> getRecord() {
    loadIdentifiable();
    return Optional.ofNullable((Record) this.identifiable);
  }

  @Override
  public boolean isBlob() {
    loadIdentifiable();
    return this.identifiable instanceof Blob;
  }

  @Override
  public Optional<Blob> getBlob() {
    loadIdentifiable();

    if (isBlob()) {
      return Optional.ofNullable((Blob) this.identifiable);
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
    if (property instanceof YTResult) {
      return ((YTResult) property).toEntity();
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

    if (identifiable instanceof Entity elem) {
      if (elem.isUnloaded()) {
        try {
          if (session == null) {
            throw new IllegalStateException("There is no active session to load element");
          }

          identifiable = session.bindToSession(elem);
        } catch (YTRecordNotFoundException rnf) {
          identifiable = null;
        }
      }

      return;
    }

    if (identifiable instanceof YTContextualRecordId) {
      this.addMetadata(((YTContextualRecordId) identifiable).getContext());
    }

    try {
      if (session == null) {
        throw new IllegalStateException("There is no active session to load element");
      }

      this.identifiable = session.load(identifiable.getIdentity());
    } catch (YTRecordNotFoundException rnf) {
      identifiable = null;
    }
  }

  public void setIdentifiable(YTIdentifiable identifiable) {
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
    if (!(obj instanceof YTResultInternal resultObj)) {
      return false;
    }
    if (identifiable != null) {
      if (!resultObj.getEntity().isPresent()) {
        return false;
      }
      return identifiable.equals(resultObj.getEntity().get());
    } else {
      if (resultObj.getEntity().isPresent()) {
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


  public void bindToCache(YTDatabaseSessionInternal db) {
    if (identifiable instanceof RecordAbstract record) {
      var identity = record.getIdentity();
      var tx = db.getTransaction();

      if (tx.isActive()) {
        var recordEntry = tx.getRecordEntry(identity);
        if (recordEntry != null) {
          if (recordEntry.type == ORecordOperation.DELETED) {
            identifiable = identity;
            return;
          }

          identifiable = recordEntry.record;
          return;
        }
      }

      RecordAbstract cached = db.getLocalCache().findRecord(identity);

      if (cached == record) {
        return;
      }

      if (cached != null) {
        if (!cached.isDirty()) {
          cached.fromStream(record.toStream());
          cached.setIdentity((YTRecordId) record.getIdentity());
          cached.setVersion(record.getVersion());

          assert !cached.isDirty();
        }
        identifiable = cached;
      } else {
        if (!identity.isPersistent()) {
          return;
        }

        db.getLocalCache().updateRecord(record);
      }
    }
  }
}
