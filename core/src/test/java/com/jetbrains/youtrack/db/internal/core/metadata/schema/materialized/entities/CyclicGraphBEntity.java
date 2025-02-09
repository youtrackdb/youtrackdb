package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;
import java.util.Set;

public interface CyclicGraphBEntity extends MaterializedEntity {

  Set<CyclicGraphCEntity> getCyclicGraphSetCEntity();
}
