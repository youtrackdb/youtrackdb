package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;

public interface SelfReferencedEntity extends MaterializedEntity {

  SelfReferencedEntity getSelfReferencedEntity();
}
