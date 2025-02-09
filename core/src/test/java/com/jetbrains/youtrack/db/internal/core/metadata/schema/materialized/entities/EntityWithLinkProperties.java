package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface EntityWithLinkProperties extends MaterializedEntity {

  EntityWithEmbeddedCollections getEntityWithEmbeddedCollections();

  Set<EntityWithSingleValueProperties> getEntityWithPrimitivePropertiesSet();

  List<EmptyEntity> getEmptyEntityList();

  Map<String, EntityWithEmbeddedCollections> getEntityWithEmbeddedCollectionsMap();
}
