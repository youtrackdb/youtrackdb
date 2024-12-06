package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;

public interface Change {

  int SIZE = ByteSerializer.BYTE_SIZE + IntegerSerializer.INT_SIZE;

  void increment();

  void decrement();

  int applyTo(Integer value);

  int getValue();

  byte getType();

  /**
   * Checks if put increment operation can be safely performed.
   *
   * @return true if increment operation can be safely performed.
   */
  boolean isUndefined();

  void applyDiff(int delta);

  int serialize(byte[] stream, int offset);
}
