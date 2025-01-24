package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.Collection;
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

  default boolean isVertex() {
    return getEntity().map(x -> x.isVertex()).orElse(false);
  }

  default Optional<Vertex> getVertex() {
    return getEntity().flatMap(x -> x.asVertex());
  }

  @Nullable
  default Vertex toVertex() {
    var entity = asEntity();
    if (entity == null) {
      return null;
    }

    return entity.toVertex();
  }

  default boolean isEdge() {
    return getEntity().map(x -> x.isEdge()).orElse(false);
  }

  default Optional<Edge> getEdge() {
    return getEntity().flatMap(x -> x.asEdge());
  }

  @Nullable
  default Edge toEdge() {
    var entity = asEntity();
    if (entity == null) {
      return null;
    }

    return entity.toEdge();
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

  Map<String, ?> toMap();

  String toJSON();

  boolean hasProperty(String varName);
}
