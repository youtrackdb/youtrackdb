package com.jetbrains.youtrack.db.internal.core.index.multivalue;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValuesTransformer;
import java.util.Collection;

public final class MultiValuesTransformer implements IndexEngineValuesTransformer {

  public static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

  @Override
  public Collection<RID> transformFromValue(Object value) {
    //noinspection unchecked
    return (Collection<RID>) value;
  }
}
