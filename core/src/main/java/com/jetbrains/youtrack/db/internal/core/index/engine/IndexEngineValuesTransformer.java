package com.jetbrains.youtrack.db.internal.core.index.engine;

import com.jetbrains.youtrack.db.api.record.RID;
import java.util.Collection;

public interface IndexEngineValuesTransformer {

  Collection<RID> transformFromValue(Object value);
}
