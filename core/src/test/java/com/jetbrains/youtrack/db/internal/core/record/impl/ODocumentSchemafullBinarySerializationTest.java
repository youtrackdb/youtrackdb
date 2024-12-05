package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerBinary;

public class ODocumentSchemafullBinarySerializationTest
    extends ODocumentSchemafullSerializationTest {

  public ODocumentSchemafullBinarySerializationTest() {
    super(new ORecordSerializerBinary());
  }
}
