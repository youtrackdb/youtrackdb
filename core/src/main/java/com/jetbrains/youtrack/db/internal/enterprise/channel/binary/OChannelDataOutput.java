package com.jetbrains.youtrack.db.internal.enterprise.channel.binary;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public interface OChannelDataOutput {

  OChannelDataOutput writeByte(final byte iContent) throws IOException;

  OChannelDataOutput writeBoolean(final boolean iContent) throws IOException;

  OChannelDataOutput writeInt(final int iContent) throws IOException;

  OChannelDataOutput writeLong(final long iContent) throws IOException;

  OChannelDataOutput writeShort(final short iContent) throws IOException;

  OChannelDataOutput writeString(final String iContent) throws IOException;

  OChannelDataOutput writeBytes(final byte[] iContent) throws IOException;

  OChannelDataOutput writeBytes(final byte[] iContent, final int iLength) throws IOException;

  void writeRID(final YTRID iRID) throws IOException;

  void writeVersion(final int version) throws IOException;

  OutputStream getDataOutput();
}
