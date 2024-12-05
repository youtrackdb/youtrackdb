package com.orientechnologies.core.db;

import com.orientechnologies.core.config.OStorageConfiguration;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.metadata.schema.OSchemaShared;

public interface OMetadataUpdateListener {

  void onSchemaUpdate(YTDatabaseSessionInternal session, String database, OSchemaShared schema);

  void onIndexManagerUpdate(YTDatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager);

  void onFunctionLibraryUpdate(YTDatabaseSessionInternal session, String database);

  void onSequenceLibraryUpdate(YTDatabaseSessionInternal session, String database);

  void onStorageConfigurationUpdate(String database,
      OStorageConfiguration update);
}
