package com.orientechnologies.orient.core.storage.impl.local;

/**
 * Allows listeners to be notified in case of recovering is started at storage open.
 */
public interface OStorageRecoverListener {

  void onStorageRecover();
}
