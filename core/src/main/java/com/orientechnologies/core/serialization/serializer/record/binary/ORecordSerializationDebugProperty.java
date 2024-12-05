package com.orientechnologies.core.serialization.serializer.record.binary;

import com.orientechnologies.core.metadata.schema.YTType;

public class ORecordSerializationDebugProperty {

  public String name;
  public int globalId;
  public YTType type;
  public RuntimeException readingException;
  public boolean faildToRead;
  public int failPosition;
  public Object value;
  public int valuePos;
}
