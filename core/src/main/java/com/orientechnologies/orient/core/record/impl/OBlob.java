package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.record.ORecord;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public interface OBlob extends ORecord {

  byte RECORD_TYPE = 'b';

  int fromInputStream(final InputStream in) throws IOException;

  int fromInputStream(final InputStream in, final int maxSize) throws IOException;

  void toOutputStream(final OutputStream out) throws IOException;

  byte[] toStream();
}
