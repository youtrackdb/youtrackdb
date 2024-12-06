package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readString;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ImmutableSchema;

public class RecordSerializerBinaryDebug extends RecordSerializerBinaryV0 {

  public RecordSerializationDebug deserializeDebug(
      final byte[] iSource, DatabaseSessionInternal db) {
    RecordSerializationDebug debugInfo = new RecordSerializationDebug();
    ImmutableSchema schema = db.getMetadata().getImmutableSchemaSnapshot();
    BytesContainer bytes = new BytesContainer(iSource);
    int version = readByte(bytes);

    if (RecordSerializerBinary.INSTANCE.getSerializer(version).isSerializingClassNameByDefault()) {
      try {
        final String className = readString(bytes);
        debugInfo.className = className;
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
        return debugInfo;
      }
    }

    RecordSerializerBinary.INSTANCE
        .getSerializer(version)
        .deserializeDebug(db, bytes, debugInfo, schema);
    return debugInfo;
  }
}
