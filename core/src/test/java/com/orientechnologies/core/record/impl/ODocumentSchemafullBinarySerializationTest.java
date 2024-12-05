package com.orientechnologies.core.record.impl;

import com.orientechnologies.core.serialization.serializer.record.binary.ORecordSerializerBinary;

public class ODocumentSchemafullBinarySerializationTest
    extends ODocumentSchemafullSerializationTest {

  public ODocumentSchemafullBinarySerializationTest() {
    super(new ORecordSerializerBinary());
  }
}
