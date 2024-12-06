package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class ImportRequest implements BinaryRequest<ImportResponse> {

  private InputStream inputStream;
  private String options;
  private String name;
  private String imporPath;

  public ImportRequest(InputStream inputStream, String options, String name) {
    this.inputStream = inputStream;
    this.options = options;
    this.name = name;
  }

  public ImportRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeString(options);
    network.writeString(name);
    byte[] buffer = new byte[1024];
    int size;
    while ((size = inputStream.read(buffer)) > 0) {
      network.writeBytes(buffer, size);
    }
    network.writeBytes(null);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    options = channel.readString();
    name = channel.readString();
    File file = File.createTempFile("import", name);
    FileOutputStream output = new FileOutputStream(file);
    byte[] bytes;
    while ((bytes = channel.readBytes()) != null) {
      output.write(bytes);
    }
    output.close();

    // This may not be closed, to double check
    imporPath = file.getAbsolutePath();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_DB_IMPORT;
  }

  @Override
  public String getDescription() {
    return "Import Database";
  }

  public String getImporPath() {
    return imporPath;
  }

  public String getName() {
    return name;
  }

  public String getOptions() {
    return options;
  }

  @Override
  public ImportResponse createResponse() {
    return new ImportResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeImport(this);
  }
}
