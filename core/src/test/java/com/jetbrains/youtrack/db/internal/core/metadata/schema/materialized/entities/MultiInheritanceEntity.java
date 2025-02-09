package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import java.util.Set;

public interface MultiInheritanceEntity extends EntityWithLinkProperties,
    EntityWithEmbeddedCollections {

  Set<EmptyEntity> getEmptyEntitySet();
}
