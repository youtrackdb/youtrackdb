package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import javax.annotation.Nonnull;

public interface MaterializedEntity extends Identifiable {
  @Nonnull
  Entity getDatabaseEntity();
}
