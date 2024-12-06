package com.jetbrains.youtrack.db.internal.client.remote;

public interface BinaryAsyncRequest<T extends BinaryResponse> extends BinaryRequest<T> {

  void setMode(byte mode);

  byte getMode();
}
