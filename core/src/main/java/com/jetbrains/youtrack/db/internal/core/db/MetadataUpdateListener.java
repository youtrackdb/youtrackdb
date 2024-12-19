package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;

public interface MetadataUpdateListener {

  void onSchemaUpdate(DatabaseSessionInternal db, String database, SchemaShared schema);

  void onSequenceLibraryUpdate(DatabaseSessionInternal session, String database);

  void onStorageConfigurationUpdate(String database,
      StorageConfiguration update);
}
