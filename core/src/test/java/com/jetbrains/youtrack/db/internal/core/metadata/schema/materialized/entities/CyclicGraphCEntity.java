package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;
import java.util.Map;

public interface CyclicGraphCEntity extends MaterializedEntity {

  Map<String, CyclicGraphAEntity> getCyclicGraphMapAEntity();
}
