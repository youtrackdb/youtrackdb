package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;

public interface RecordSerializerNetwork extends RecordSerializer {

  byte[] serializeValue(DatabaseSessionInternal db, Object value, PropertyType type);

  Object deserializeValue(DatabaseSessionInternal db, byte[] val, PropertyType type);
}
