package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.config.OStorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.OSchemaShared;

public interface OMetadataUpdateListener {

  void onSchemaUpdate(YTDatabaseSessionInternal session, String database, OSchemaShared schema);

  void onIndexManagerUpdate(YTDatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager);

  void onFunctionLibraryUpdate(YTDatabaseSessionInternal session, String database);

  void onSequenceLibraryUpdate(YTDatabaseSessionInternal session, String database);

  void onStorageConfigurationUpdate(String database,
      OStorageConfiguration update);
}
