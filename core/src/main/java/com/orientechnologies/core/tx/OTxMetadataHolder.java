package com.orientechnologies.core.tx;

public interface OTxMetadataHolder {

  byte[] metadata();

  void notifyMetadataRead();

  OTransactionId getId();

  OTransactionSequenceStatus getStatus();
}
