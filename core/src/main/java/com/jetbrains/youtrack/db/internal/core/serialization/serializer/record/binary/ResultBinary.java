/*
 * Copyright 2018 YouTrackDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ImmutableSchema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public class ResultBinary implements Result {

  private final EntitySerializer serializer;
  @Nullable
  private final RecordId id;
  private final byte[] bytes;
  private final int offset;
  private final int fieldLength;
  private final ImmutableSchema schema;
  private final DatabaseSessionInternal session;

  public ResultBinary(
      DatabaseSessionInternal session,
      ImmutableSchema schema,
      byte[] bytes,
      int offset,
      int fieldLength,
      EntitySerializer serializer) {
    this.schema = schema;
    this.id = null;
    this.bytes = bytes;
    this.serializer = serializer;
    this.offset = offset;
    this.fieldLength = fieldLength;
    this.session = session;

  }

  public ResultBinary(
      DatabaseSessionInternal session,
      byte[] bytes,
      int offset,
      int fieldLength,
      EntitySerializer serializer,
      @Nullable RecordId id) {
    schema = session.getMetadata().getImmutableSchemaSnapshot();
    this.id = id;
    this.bytes = bytes;
    this.serializer = serializer;
    this.offset = offset;
    this.fieldLength = fieldLength;
    this.session = session;
  }

  public int getFieldLength() {
    return fieldLength;
  }

  public int getOffset() {
    return offset;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public <T> T getProperty(String name) {
    assert session != null && session.assertIfNotActive();

    var bytes = new BytesContainer(this.bytes);
    bytes.skip(offset);
    return serializer.deserializeFieldTyped(session, bytes, name, id == null, schema, null);
  }

  @Override
  public Entity getEntityProperty(String name) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Vertex getVertexProperty(String name) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Edge getEdgeProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Blob getBlobProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<String> getPropertyNames() {
    assert session != null && session.assertIfNotActive();

    final var container = new BytesContainer(bytes);
    container.skip(offset);
    // TODO: use something more correct that new EntityImpl
    var fields = serializer.getFieldNames(session, new EntityImpl(session), container, id == null);
    return new HashSet<>(Arrays.asList(fields));
  }

  @Override
  public Optional<RID> getIdentity() {
    assert session != null && session.assertIfNotActive();
    return Optional.ofNullable(id);
  }

  @Nullable
  @Override
  public RID getRecordId() {
    assert session != null && session.assertIfNotActive();
    return id;
  }

  @Override
  public boolean isEntity() {
    assert session != null && session.assertIfNotActive();
    return true;
  }

  @Override
  public boolean isRecord() {
    assert session != null && session.assertIfNotActive();
    return true;
  }

  @Override
  public Optional<Entity> getEntity() {
    assert session != null && session.assertIfNotActive();
    return Optional.of(asEntity());
  }

  @Override
  public Entity asEntity() {
    assert session != null && session.assertIfNotActive();
    return toEntityImpl();
  }

  @Override
  public boolean isBlob() {
    assert session != null && session.assertIfNotActive();
    return false;
  }

  @Override
  public Map<String, ?> toMap() {
    assert session != null && session.assertIfNotActive();
    return toEntityImpl().toMap();
  }

  @Override
  public String toJSON() {
    assert session != null && session.assertIfNotActive();
    return toEntityImpl().toJSON();
  }

  @Override
  public Optional<Blob> getBlob() {
    assert session != null && session.assertIfNotActive();
    return Optional.empty();
  }

  @Override
  public Optional<DBRecord> getRecord() {
    assert session != null && session.assertIfNotActive();
    return Optional.of(toEntityImpl());
  }

  @Override
  public boolean isProjection() {
    assert session != null && session.assertIfNotActive();
    return false;
  }

  @Override
  public Object getMetadata(String key) {
    assert session != null && session.assertIfNotActive();
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    assert session != null && session.assertIfNotActive();
    return null;
  }

  @Override
  public boolean hasProperty(String varName) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Nullable
  @Override
  public DatabaseSession getBoundedToSession() {
    return session;
  }

  private EntityImpl toEntityImpl() {
    var entity = new EntityImpl(session);
    var bytes = new BytesContainer(this.bytes);
    bytes.skip(offset);

    serializer.deserialize(session, entity, bytes);
    return entity;
  }
}
