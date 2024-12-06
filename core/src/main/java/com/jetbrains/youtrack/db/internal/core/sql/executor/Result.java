package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.Blob;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.lang.reflect.Array;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public interface Result {

  /**
   * returns a property from the result
   *
   * @param name the property name
   * @param <T>
   * @return the property value. If the property value is a persistent record, it only returns the
   * RID. See also {@link #getEntityProperty(String)} {@link #getVertexProperty(String)}
   * {@link #getEdgeProperty(String)} {@link #getBlobProperty(String)}
   */
  <T> T getProperty(String name);

  /**
   * returns an Entity property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an Entity
   */
  Entity getEntityProperty(String name);

  /**
   * returns an Vertex property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an Vertex
   */
  Vertex getVertexProperty(String name);

  /**
   * returns an Edge property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an Edge
   */
  Edge getEdgeProperty(String name);

  /**
   * returns an Blob property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an Blob
   */
  Blob getBlobProperty(String name);

  Collection<String> getPropertyNames();

  Optional<RID> getIdentity();

  @Nullable
  RID getRecordId();

  boolean isEntity();

  Optional<Entity> getEntity();

  @Nullable
  Entity asEntity();

  @Nullable
  Entity toEntity();

  default boolean isVertex() {
    return getEntity().map(x -> x.isVertex()).orElse(false);
  }

  default Optional<Vertex> getVertex() {
    return getEntity().flatMap(x -> x.asVertex());
  }

  @Nullable
  default Vertex toVertex() {
    var element = toEntity();
    if (element == null) {
      return null;
    }

    return element.toVertex();
  }

  default boolean isEdge() {
    return getEntity().map(x -> x.isEdge()).orElse(false);
  }

  default Optional<Edge> getEdge() {
    return getEntity().flatMap(x -> x.asEdge());
  }

  @Nullable
  default Edge toEdge() {
    var element = toEntity();
    if (element == null) {
      return null;
    }

    return element.toEdge();
  }

  boolean isBlob();

  Optional<Blob> getBlob();

  Optional<Record> getRecord();

  boolean isRecord();

  boolean isProjection();

  /**
   * return metadata related to current result given a key
   *
   * @param key the metadata key
   * @return metadata related to current result given a key
   */
  Object getMetadata(String key);

  /**
   * return all the metadata keys available
   *
   * @return all the metadata keys available
   */
  Set<String> getMetadataKeys();

  default String toJSON() {
    if (isEntity()) {
      return getEntity().get().toJSON();
    }
    StringBuilder result = new StringBuilder();
    result.append("{");
    boolean first = true;
    for (String prop : getPropertyNames()) {
      if (!first) {
        result.append(", ");
      }
      result.append(toJson(prop));
      result.append(": ");
      result.append(toJson(getProperty(prop)));
      first = false;
    }
    result.append("}");
    return result.toString();
  }

  default String toJson(Object val) {
    String jsonVal = null;
    if (val == null) {
      jsonVal = "null";
    } else if (val instanceof String) {
      jsonVal = "\"" + encode(val.toString()) + "\"";
    } else if (val instanceof Number || val instanceof Boolean) {
      jsonVal = val.toString();
    } else if (val instanceof Result) {
      jsonVal = ((Result) val).toJSON();
    } else if (val instanceof Entity) {
      RID id = ((Entity) val).getIdentity();
      if (id.isPersistent()) {
        //        jsonVal = "{\"@rid\":\"" + id + "\"}"; //TODO enable this syntax when Studio and
        // the parsing are OK
        jsonVal = "\"" + id + "\"";
      } else {
        jsonVal = ((Entity) val).toJSON();
      }
    } else if (val instanceof RID) {
      //      jsonVal = "{\"@rid\":\"" + val + "\"}"; //TODO enable this syntax when Studio and the
      // parsing are OK
      jsonVal = "\"" + val + "\"";
    } else if (val instanceof Iterable) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      Iterator iterator = ((Iterable) val).iterator();
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Iterator iterator) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Map) {
      StringBuilder builder = new StringBuilder();
      builder.append("{");
      boolean first = true;
      Map<Object, Object> map = (Map) val;
      for (Map.Entry entry : map.entrySet()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(entry.getKey()));
        builder.append(": ");
        builder.append(toJson(entry.getValue()));
        first = false;
      }
      builder.append("}");
      jsonVal = builder.toString();
    } else if (val instanceof byte[]) {
      jsonVal = "\"" + Base64.getEncoder().encodeToString((byte[]) val) + "\"";
    } else if (val instanceof Date) {
      jsonVal = "\"" + DateHelper.getDateTimeFormatInstance().format(val) + "\"";
    } else if (val.getClass().isArray()) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      for (int i = 0; i < Array.getLength(val); i++) {
        if (i > 0) {
          builder.append(", ");
        }
        builder.append(toJson(Array.get(val, i)));
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else {
      throw new UnsupportedOperationException(
          "Cannot convert " + val + " - " + val.getClass() + " to JSON");
    }
    return jsonVal;
  }

  default String encode(String s) {
    return IOUtils.encodeJsonString(s);
  }

  boolean hasProperty(String varName);
}
