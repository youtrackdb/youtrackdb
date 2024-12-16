package com.jetbrains.youtrack.db.internal.core.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class FrontendTransacationMetadataHolderImpl implements FrontendTransacationMetadataHolder {

  private final CountDownLatch request;
  private final FrontendTransactionSequenceStatus status;
  private final FrontendTransactionId id;

  public FrontendTransacationMetadataHolderImpl(
      CountDownLatch request, FrontendTransactionId id, FrontendTransactionSequenceStatus status) {
    this.request = request;
    this.id = id;
    this.status = status;
  }

  @Override
  public byte[] metadata() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(outputStream);
    try {
      id.write(output);
      byte[] status = this.status.store();
      output.writeInt(status.length);
      output.write(status, 0, status.length);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputStream.toByteArray();
  }

  public static FrontendTransacationMetadataHolder read(final byte[] data) {
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    final DataInput input = new DataInputStream(inputStream);
    try {
      final FrontendTransactionId txId = FrontendTransactionId.read(input);
      int size = input.readInt();
      byte[] status = new byte[size];
      input.readFully(status);
      return new FrontendTransacationMetadataHolderImpl(
          new CountDownLatch(0), txId, FrontendTransactionSequenceStatus.read(status));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public void notifyMetadataRead() {
    request.countDown();
  }

  public FrontendTransactionId getId() {
    return id;
  }

  @Override
  public FrontendTransactionSequenceStatus getStatus() {
    return status;
  }
}
