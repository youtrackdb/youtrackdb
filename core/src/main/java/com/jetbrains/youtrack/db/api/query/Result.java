package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.StatefulEdge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Result {

  /**
   * returns a property from the result
   *
   * @param name the property name
   * @return the property value. If the property value is a persistent record, it only returns the
   * RID. See also {@link #getEntityProperty(String)} {@link #getVertexProperty(String)}
   * {@link #getEdgeProperty(String)} {@link #getBlobProperty(String)}
   */
  @Nullable
  <T> T getProperty(@Nonnull String name);

  /**
   * returns an Entity property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an Entity
   */
  @Nullable
  Entity getEntityProperty(@Nonnull String name);

  /**
   * Returns the property value as an Vertex. If the property is a link, it will be loaded and
   * returned as an Vertex. If the property is an Vertex, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an Vertex
   * @throws DatabaseException if the property is not an Vertex
   */
  @Nullable
  default Vertex getVertexProperty(@Nonnull String propertyName) {
    var entity = getEntityProperty(propertyName);
    if (entity == null) {
      return null;
    }

    return entity.castToVertex();
  }

  /**
   * Returns the property value as an Edge. If the property is a link, it will be loaded and
   * returned as an Edge. If the property is an Edge, exception will be thrown.
   *
   * @param propertyName the property name
   * @return the property value as an Edge
   * @throws DatabaseException if the property is not an Edge
   */
  @Nullable
  default Edge getEdgeProperty(@Nonnull String propertyName) {
    var entity = getEntityProperty(propertyName);
    if (entity == null) {
      return null;
    }

    return entity.castToStateFullEdge();
  }

  /**
   * returns an Blob property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an Blob
   */
  Blob getBlobProperty(String name);

  /**
   * This method similar to {@link #getProperty(String)} bun unlike before mentioned method it does
   * not load link automatically.
   *
   * @param name the name of the link property
   * @return the link property value, or null if the property does not exist
   * @throws IllegalArgumentException if requested property is not a link.
   * @see #getProperty(String)
   */
  @Nullable
  Identifiable getLinkProperty(@Nonnull String name);

  /**
   * Returns all the names of defined properties
   *
   * @return all the names of defined properties
   */
  @Nonnull
  Collection<String> getPropertyNames();

  @Nullable
  RID getIdentity();

  boolean isEntity();

  @Nonnull
  Entity castToEntity();

  @Nullable
  Entity asEntity();

  default boolean isVertex() {
    var entity = asEntity();
    if (entity == null) {
      return false;
    }
    return entity.isVertex();
  }

  @Nonnull
  default Vertex castToVertex() {
    return castToEntity().castToVertex();
  }

  @Nullable
  default Vertex asVertex() {
    var entity = asEntity();

    if (entity == null) {
      return null;
    }
    return entity.asVertex();
  }

  boolean isEdge();

  @Nonnull
  Edge castToEdge();

  @Nullable
  Edge asEdge();

  default boolean isStatefulEdge() {
    var entity = asEntity();
    if (entity == null) {
      return false;
    }
    return entity.isStatefulEdge();
  }

  @Nonnull
  default StatefulEdge castToStateFullEdge() {
    return castToEntity().castToStateFullEdge();
  }

  @Nullable
  default StatefulEdge asRegularEdge() {
    var entity = asEntity();
    if (entity == null) {
      return null;
    }
    return entity.asRegularEdge();
  }

  boolean isBlob();

  @Nonnull
  Blob castToBlob();

  @Nullable
  Blob asBlob();

  @Nonnull
  DBRecord castToRecord();

  @Nullable
  DBRecord asRecord();

  boolean isRecord();

  boolean isProjection();

  /**
   * Returns the result as <code>Map</code>. If the result has identity, then the @rid entry is
   * added. If the result is an entity that has a class, then the @class entry is added. If entity
   * is embedded, then the @embedded entry is added.
   */
  @Nonnull
  Map<String, Object> toMap();

  @Nonnull
  String toJSON();

  boolean hasProperty(@Nonnull String varName);

  /**
   * @return Returns session to which given record is bound or <code>null</code> if record is
   * unloaded.
   */
  @Nullable
  DatabaseSession getBoundedToSession();

  /**
   * Detach the result from the session. If result contained a record, it will be converted into
   * record id.
   */
  @Nonnull
  Result detach();
}
