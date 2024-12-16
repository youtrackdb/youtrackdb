package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerBinary;

public class DocumentSchemafullBinarySerializationTest
    extends DocumentSchemafullSerializationTest {

  public DocumentSchemafullBinarySerializationTest() {
    super(new RecordSerializerBinary());
  }
}
