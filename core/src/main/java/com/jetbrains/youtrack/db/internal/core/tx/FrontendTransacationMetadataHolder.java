package com.jetbrains.youtrack.db.internal.core.tx;

public interface FrontendTransacationMetadataHolder {

  byte[] metadata();

  void notifyMetadataRead();

  FrontendTransactionId getId();

  FrontendTransactionSequenceStatus getStatus();
}
