package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.VarIntSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class FrontendTransactionSequenceStatus {

  private final long[] status;

  public FrontendTransactionSequenceStatus(long[] status) {
    this.status = status;
  }

  public byte[] store() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(buffer);
    VarIntSerializer.write(dataOutput, this.status.length);
    for (int i = 0; i < this.status.length; i++) {
      VarIntSerializer.write(dataOutput, this.status[i]);
    }
    return buffer.toByteArray();
  }

  public static FrontendTransactionSequenceStatus read(byte[] data) throws IOException {
    DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
    int len = VarIntSerializer.readAsInt(dataInput);
    long[] newSequential = new long[len];
    for (int i = 0; i < len; i++) {
      newSequential[i] = VarIntSerializer.readAsLong(dataInput);
    }
    return new FrontendTransactionSequenceStatus(newSequential);
  }

  public void writeNetwork(DataOutput dataOutput) throws IOException {
    VarIntSerializer.write(dataOutput, this.status.length);
    for (int i = 0; i < this.status.length; i++) {
      VarIntSerializer.write(dataOutput, this.status[i]);
    }
  }

  public static FrontendTransactionSequenceStatus readNetwork(DataInput dataInput)
      throws IOException {
    int len = VarIntSerializer.readAsInt(dataInput);
    long[] newSequential = new long[len];
    for (int i = 0; i < len; i++) {
      newSequential[i] = VarIntSerializer.readAsLong(dataInput);
    }
    return new FrontendTransactionSequenceStatus(newSequential);
  }

  public long[] getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FrontendTransactionSequenceStatus that = (FrontendTransactionSequenceStatus) o;
    return Arrays.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(status);
  }
}
