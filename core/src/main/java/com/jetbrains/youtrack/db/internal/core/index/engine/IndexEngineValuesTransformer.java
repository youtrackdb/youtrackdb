package com.jetbrains.youtrack.db.internal.core.index.engine;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.util.Collection;

public interface IndexEngineValuesTransformer {

  Collection<YTRID> transformFromValue(Object value);
}
