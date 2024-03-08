package com.orientechnologies.orient.core.storage.fs;

import com.orientechnologies.common.collection.closabledictionary.OClosableItem;
import com.orientechnologies.common.util.ORawPairLongObject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

public interface OFile extends OClosableItem {
  int HEADER_SIZE = 1024;

  long allocateSpace(int size) throws IOException;

  void shrink(long size) throws IOException;

  long getFileSize();

  void read(long offset, ByteBuffer buffer, boolean throwOnEof) throws IOException;

  void write(long offset, ByteBuffer buffer) throws IOException;

  IOResult write(List<ORawPairLongObject<ByteBuffer>> buffers) throws IOException;

  void synch();

  void create() throws IOException;

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
   */
  void open();

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  void close();

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#delete()
   */
  void delete() throws IOException, InterruptedException;

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isOpen()
   */
  boolean isOpen();

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#exists()
   */
  boolean exists();

  String getName();

  void renameTo(Path newFile) throws IOException, InterruptedException;

  /**
   * Returns the underlying file size. It's the physical size of the file on the file system.
   *
   * @return the size of the file
   * @throws IOException if an I/O error occurs
   */
  long getUnderlyingFileSize() throws IOException;

  void replaceContentWith(Path newContentFile) throws IOException, InterruptedException;

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#toString()
   */
  @Override
  String toString();
}
