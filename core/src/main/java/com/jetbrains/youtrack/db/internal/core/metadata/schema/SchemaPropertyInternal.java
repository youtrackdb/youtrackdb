package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import java.lang.reflect.Method;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SchemaPropertyInternal extends SchemaProperty {

  Collection<Index> getAllIndexesInternal(@Nonnull DatabaseSession session);

  void setMaterializedAccessMethods(@Nonnull Method getter, @Nullable Method setter);

  @Nonnull
  Method getMaterializedGetter();

  @Nullable
  Method getMaterializedSetter();
}
