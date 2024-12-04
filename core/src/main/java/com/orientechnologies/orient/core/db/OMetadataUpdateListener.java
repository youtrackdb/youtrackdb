package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;

public interface OMetadataUpdateListener {

  void onSchemaUpdate(YTDatabaseSessionInternal session, String database, OSchemaShared schema);

  void onIndexManagerUpdate(YTDatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager);

  void onFunctionLibraryUpdate(YTDatabaseSessionInternal session, String database);

  void onSequenceLibraryUpdate(YTDatabaseSessionInternal session, String database);

  void onStorageConfigurationUpdate(String database,
      OStorageConfiguration update);
}
