package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;
import java.util.List;

public interface CyclicGraphAEntity extends MaterializedEntity {

  List<CyclicGraphBEntity> getCyclicGraphListBEntity();

  CyclicGraphCEntity getCyclicGraphCEntity();
}
