package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.internal.core.index.engine.OBaseIndexEngine;

/**
 * @since 9/4/2015
 */
public interface OIndexEngineCallback<T> {

  T callEngine(OBaseIndexEngine engine);
}
