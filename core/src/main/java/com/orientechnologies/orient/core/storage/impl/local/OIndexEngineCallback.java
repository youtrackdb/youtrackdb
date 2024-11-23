package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;

/**
 * @since 9/4/2015
 */
public interface OIndexEngineCallback<T> {

  T callEngine(OBaseIndexEngine engine);
}
