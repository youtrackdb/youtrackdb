package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.internal.core.index.engine.BaseIndexEngine;

/**
 * @since 9/4/2015
 */
public interface IndexEngineCallback<T> {

  T callEngine(BaseIndexEngine engine);
}
