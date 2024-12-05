package com.orientechnologies.core.storage.cache.local;

/**
 * Listener for exceptions which are thrown during background flush of files in write cache.
 */
public interface OBackgroundExceptionListener {

  void onException(Throwable e);
}
