package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import com.jetbrains.youtrack.db.api.record.Entity;
import javax.annotation.Nonnull;

public interface MaterializedEntity {
  @Nonnull
  Entity getDatabaseEntity();
}
