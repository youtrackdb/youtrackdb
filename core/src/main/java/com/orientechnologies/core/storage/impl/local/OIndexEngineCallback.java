package com.orientechnologies.core.storage.impl.local;

import com.orientechnologies.core.index.engine.OBaseIndexEngine;

/**
 * @since 9/4/2015
 */
public interface OIndexEngineCallback<T> {

  T callEngine(OBaseIndexEngine engine);
}
