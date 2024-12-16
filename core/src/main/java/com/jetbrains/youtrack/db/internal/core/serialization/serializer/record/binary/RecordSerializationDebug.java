package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import java.util.ArrayList;

public class RecordSerializationDebug {

  public String className;
  public ArrayList<RecordSerializationDebugProperty> properties;
  public boolean readingFailure;
  public RuntimeException readingException;
  public int failPosition;
}
