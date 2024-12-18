package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.MetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;

class QueryMetadataUpdateListener implements MetadataUpdateListener {

  private boolean updated = false;

  @Override
  public void onSchemaUpdate(DatabaseSessionInternal db, String database,
      SchemaShared schema) {
    updated = true;
  }

  @Override
  public void onFunctionLibraryUpdate(DatabaseSessionInternal session, String database) {
    updated = true;
  }

  @Override
  public void onSequenceLibraryUpdate(DatabaseSessionInternal session, String database) {
    updated = true;
  }

  @Override
  public void onStorageConfigurationUpdate(String database, StorageConfiguration update) {
    updated = true;
  }

  public boolean isUpdated() {
    return updated;
  }
}
