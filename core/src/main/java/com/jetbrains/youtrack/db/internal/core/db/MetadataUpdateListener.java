package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;

public interface MetadataUpdateListener {

  void onSchemaUpdate(DatabaseSessionInternal session, String database, SchemaShared schema);

  void onIndexManagerUpdate(DatabaseSessionInternal session, String database,
      IndexManagerAbstract indexManager);

  void onFunctionLibraryUpdate(DatabaseSessionInternal session, String database);

  void onSequenceLibraryUpdate(DatabaseSessionInternal session, String database);

  void onStorageConfigurationUpdate(String database,
      StorageConfiguration update);
}
