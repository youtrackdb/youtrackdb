package com.orientechnologies.orient.server;

import com.orientechnologies.core.config.OStorageConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.OMetadataUpdateListener;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.metadata.schema.OSchemaShared;

class OQueryMetadataUpdateListener implements OMetadataUpdateListener {

  private boolean updated = false;

  @Override
  public void onSchemaUpdate(YTDatabaseSessionInternal session, String database,
      OSchemaShared schema) {
    updated = true;
  }

  @Override
  public void onIndexManagerUpdate(YTDatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager) {
    updated = true;
  }

  @Override
  public void onFunctionLibraryUpdate(YTDatabaseSessionInternal session, String database) {
    updated = true;
  }

  @Override
  public void onSequenceLibraryUpdate(YTDatabaseSessionInternal session, String database) {
    updated = true;
  }

  @Override
  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
    updated = true;
  }

  public boolean isUpdated() {
    return updated;
  }
}
