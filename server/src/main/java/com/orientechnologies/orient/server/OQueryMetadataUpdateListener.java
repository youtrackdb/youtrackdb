package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;

class OQueryMetadataUpdateListener implements OMetadataUpdateListener {

  private boolean updated = false;

  @Override
  public void onSchemaUpdate(ODatabaseSessionInternal session, String database,
      OSchemaShared schema) {
    updated = true;
  }

  @Override
  public void onIndexManagerUpdate(ODatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager) {
    updated = true;
  }

  @Override
  public void onFunctionLibraryUpdate(ODatabaseSessionInternal session, String database) {
    updated = true;
  }

  @Override
  public void onSequenceLibraryUpdate(ODatabaseSessionInternal session, String database) {
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
