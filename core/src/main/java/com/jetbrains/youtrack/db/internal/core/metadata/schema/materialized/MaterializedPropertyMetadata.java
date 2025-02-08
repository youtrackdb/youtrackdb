package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import java.lang.reflect.Method;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public record MaterializedPropertyMetadata(@Nonnull String name, @Nonnull Class<?> type,
                                           @Nullable Class<?> linkedType,
                                           @Nonnull Method getter, @Nullable Method setter) {

}
