package com.jetbrains.youtrack.db.internal.core.tx;

public interface OTxMetadataHolder {

  byte[] metadata();

  void notifyMetadataRead();

  OTransactionId getId();

  OTransactionSequenceStatus getStatus();
}
