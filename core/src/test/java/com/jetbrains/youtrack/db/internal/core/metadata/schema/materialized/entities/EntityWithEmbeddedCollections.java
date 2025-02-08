package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface EntityWithEmbeddedCollections extends MaterializedEntity {

  List<String> getStringList();

  Set<String> getStringSet();

  Map<String, Integer> getIntegerMap();
}
