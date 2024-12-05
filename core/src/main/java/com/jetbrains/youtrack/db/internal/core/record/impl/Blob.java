package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public interface Blob extends Record {

  byte RECORD_TYPE = 'b';

  int fromInputStream(final InputStream in) throws IOException;

  int fromInputStream(final InputStream in, final int maxSize) throws IOException;

  void toOutputStream(final OutputStream out) throws IOException;

  byte[] toStream();
}
