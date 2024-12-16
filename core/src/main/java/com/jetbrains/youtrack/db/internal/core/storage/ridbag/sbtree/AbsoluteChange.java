package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;

public class AbsoluteChange implements Change {

  public static final byte TYPE = 1;
  private int value;

  AbsoluteChange(int value) {
    this.value = value;

    checkPositive();
  }

  @Override
  public int getValue() {
    return value;
  }

  @Override
  public void increment() {
    value++;
  }

  @Override
  public void decrement() {
    value--;

    checkPositive();
  }

  @Override
  public int applyTo(Integer value) {
    return this.value;
  }

  @Override
  public boolean isUndefined() {
    return true;
  }

  @Override
  public void applyDiff(int delta) {
    value += delta;

    checkPositive();
  }

  @Override
  public byte getType() {
    return TYPE;
  }

  @Override
  public int serialize(byte[] stream, int offset) {
    ByteSerializer.INSTANCE.serializeLiteral(TYPE, stream, offset);
    IntegerSerializer.INSTANCE.serializeLiteral(value, stream, offset + ByteSerializer.BYTE_SIZE);
    return ByteSerializer.BYTE_SIZE + IntegerSerializer.INT_SIZE;
  }

  private void checkPositive() {
    if (value < 0) {
      value = 0;
    }
  }
}
