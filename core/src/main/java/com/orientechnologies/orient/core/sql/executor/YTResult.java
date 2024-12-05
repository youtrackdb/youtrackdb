package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.util.ODateHelper;
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
public interface YTResult {

  /**
   * returns a property from the result
   *
   * @param name the property name
   * @param <T>
   * @return the property value. If the property value is a persistent record, it only returns the
   * RID. See also {@link #getElementProperty(String)} {@link #getVertexProperty(String)}
   * {@link #getEdgeProperty(String)} {@link #getBlobProperty(String)}
   */
  <T> T getProperty(String name);

  /**
   * returns an YTEntity property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an YTEntity
   */
  YTEntity getElementProperty(String name);

  /**
   * returns an YTVertex property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an YTVertex
   */
  YTVertex getVertexProperty(String name);

  /**
   * returns an YTEdge property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an YTEdge
   */
  YTEdge getEdgeProperty(String name);

  /**
   * returns an YTBlob property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an YTBlob
   */
  YTBlob getBlobProperty(String name);

  Collection<String> getPropertyNames();

  Optional<YTRID> getIdentity();

  @Nullable
  YTRID getRecordId();

  boolean isElement();

  Optional<YTEntity> getElement();

  @Nullable
  YTEntity asElement();

  @Nullable
  YTEntity toElement();

  default boolean isVertex() {
    return getElement().map(x -> x.isVertex()).orElse(false);
  }

  default Optional<YTVertex> getVertex() {
    return getElement().flatMap(x -> x.asVertex());
  }

  @Nullable
  default YTVertex toVertex() {
    var element = toElement();
    if (element == null) {
      return null;
    }

    return element.toVertex();
  }

  default boolean isEdge() {
    return getElement().map(x -> x.isEdge()).orElse(false);
  }

  default Optional<YTEdge> getEdge() {
    return getElement().flatMap(x -> x.asEdge());
  }

  @Nullable
  default YTEdge toEdge() {
    var element = toElement();
    if (element == null) {
      return null;
    }

    return element.toEdge();
  }

  boolean isBlob();

  Optional<YTBlob> getBlob();

  Optional<YTRecord> getRecord();

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
    if (isElement()) {
      return getElement().get().toJSON();
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
    } else if (val instanceof YTResult) {
      jsonVal = ((YTResult) val).toJSON();
    } else if (val instanceof YTEntity) {
      YTRID id = ((YTEntity) val).getIdentity();
      if (id.isPersistent()) {
        //        jsonVal = "{\"@rid\":\"" + id + "\"}"; //TODO enable this syntax when Studio and
        // the parsing are OK
        jsonVal = "\"" + id + "\"";
      } else {
        jsonVal = ((YTEntity) val).toJSON();
      }
    } else if (val instanceof YTRID) {
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
      jsonVal = "\"" + ODateHelper.getDateTimeFormatInstance().format(val) + "\"";
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
    return OIOUtils.encodeJsonString(s);
  }

  boolean hasProperty(String varName);
}
