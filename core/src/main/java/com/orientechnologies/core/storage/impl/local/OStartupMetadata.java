package com.orientechnologies.core.storage.impl.local;

public final class OStartupMetadata {

  final long lastTxId;
  final byte[] txMetadata;

  public OStartupMetadata(long lastTxId, byte[] txMetadata) {
    this.lastTxId = lastTxId;
    this.txMetadata = txMetadata;
  }
}
