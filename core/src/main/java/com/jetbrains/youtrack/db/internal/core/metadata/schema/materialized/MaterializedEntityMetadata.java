package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;

public record MaterializedEntityMetadata(
    @Nonnull Class<? extends MaterializedEntity> entityInterface,
    @Nonnull Collection<MaterializedPropertyMetadata> properties,
    @Nonnull Collection<Class<? extends MaterializedEntity>> parents,
    @Nonnull Map<Class<? extends MaterializedEntity>, MaterializedEntityMetadata> requiredDeclarations) {

}
