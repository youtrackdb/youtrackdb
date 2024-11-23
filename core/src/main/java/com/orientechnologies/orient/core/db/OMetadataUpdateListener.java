package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;

public interface OMetadataUpdateListener {

  void onSchemaUpdate(ODatabaseSessionInternal session, String database, OSchemaShared schema);

  void onIndexManagerUpdate(ODatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager);

  void onFunctionLibraryUpdate(ODatabaseSessionInternal session, String database);

  void onSequenceLibraryUpdate(ODatabaseSessionInternal session, String database);

  void onStorageConfigurationUpdate(String database,
      OStorageConfiguration update);
}
