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
package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public class YTResultBinary implements YTResult {

  private final ODocumentSerializer serializer;
  @Nullable
  private final YTRecordId id;
  private final byte[] bytes;
  private final int offset;
  private final int fieldLength;
  private final YTImmutableSchema schema;
  private final YTDatabaseSessionInternal db;

  public YTResultBinary(
      YTDatabaseSessionInternal db,
      YTImmutableSchema schema,
      byte[] bytes,
      int offset,
      int fieldLength,
      ODocumentSerializer serializer) {
    this.schema = schema;
    this.id = null;
    this.bytes = bytes;
    this.serializer = serializer;
    this.offset = offset;
    this.fieldLength = fieldLength;
    this.db = db;

  }

  public YTResultBinary(
      YTDatabaseSessionInternal db,
      byte[] bytes,
      int offset,
      int fieldLength,
      ODocumentSerializer serializer,
      @Nullable YTRecordId id) {
    schema = db.getMetadata().getImmutableSchemaSnapshot();
    this.id = id;
    this.bytes = bytes;
    this.serializer = serializer;
    this.offset = offset;
    this.fieldLength = fieldLength;
    this.db = db;
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
    BytesContainer bytes = new BytesContainer(this.bytes);
    bytes.skip(offset);
    return serializer.deserializeFieldTyped(db, bytes, name, id == null, schema, null);
  }

  @Override
  public YTEntity getEntityProperty(String name) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public YTVertex getVertexProperty(String name) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public YTEdge getEdgeProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public YTBlob getBlobProperty(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<String> getPropertyNames() {
    final BytesContainer container = new BytesContainer(bytes);
    container.skip(offset);
    // TODO: use something more correct that new YTDocument
    String[] fields = serializer.getFieldNames(new YTDocument(), container, id == null);
    return new HashSet<>(Arrays.asList(fields));
  }

  @Override
  public Optional<YTRID> getIdentity() {
    return Optional.ofNullable(id);
  }

  @Nullable
  @Override
  public YTRID getRecordId() {
    return id;
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Override
  public boolean isRecord() {
    return true;
  }

  @Override
  public Optional<YTEntity> getEntity() {
    return Optional.of(toDocument());
  }

  @Override
  public YTEntity asEntity() {
    return toDocument();
  }

  @Override
  public YTEntity toEntity() {
    return toDocument();
  }

  @Override
  public boolean isBlob() {
    return false;
  }

  @Override
  public Optional<YTBlob> getBlob() {
    return Optional.empty();
  }

  @Override
  public Optional<YTRecord> getRecord() {
    return Optional.of(toDocument());
  }

  @Override
  public boolean isProjection() {
    return false;
  }

  @Override
  public Object getMetadata(String key) {
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    return null;
  }

  @Override
  public boolean hasProperty(String varName) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  private YTDocument toDocument() {
    YTDocument doc = new YTDocument();
    BytesContainer bytes = new BytesContainer(this.bytes);
    bytes.skip(offset);

    serializer.deserialize(db, doc, bytes);
    return doc;
  }
}
