/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfigurationImpl;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.storage.disk.LocalPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.fs.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Handles the database configuration in one big record.
 */
public class StorageConfigurationSegment extends StorageConfigurationImpl {

  private static final int VERSION_OFFSET = 48;

  // This class uses "double write" pattern.
  // Whenever we want to update configuration, first we write data in backup file and make fsync.
  // Then we write the same data
  // in primary file and make fsync. Then we remove backup file. So does not matter if we have error
  // on any of this stages
  // we always will have consistent storage configuration.
  // Downside of this approach is the double overhead during write of configuration, but it was
  // chosen to keep binary compatibility
  // between versions.

  /**
   * Name of primary file
   */
  private static final String NAME = "database.ocf";

  /**
   * Name of backup file which is used when we update storage configuration using double write
   * pattern
   */
  private static final String BACKUP_NAME = "database.ocf2";

  private static final long ENCODING_FLAG_1 = 128975354756545L;
  private static final long ENCODING_FLAG_2 = 587138568122547L;
  private static final long ENCODING_FLAG_3 = 812587836547249L;

  private static final int CRC_32_OFFSET = 100;
  private static final byte FORMAT_VERSION = (byte) 42;

  private final String storageName;
  private final Path storagePath;

  public StorageConfigurationSegment(final LocalPaginatedStorage storage) {
    super(storage, StandardCharsets.UTF_8);

    this.storageName = storage.getName();
    this.storagePath = storage.getStoragePath();
  }

  @Override
  public void delete() throws IOException {
    lock.writeLock().lock();
    try {
      super.delete();

      clearConfigurationFiles();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove both backup and primary configuration files on delete
   *
   * @see #update()
   */
  private void clearConfigurationFiles() throws IOException {
    final var file = storagePath.resolve(NAME);
    Files.deleteIfExists(file);

    final var backupFile = storagePath.resolve(BACKUP_NAME);
    Files.deleteIfExists(backupFile);
  }

  @Override
  public void create() throws IOException {
    lock.writeLock().lock();
    try {
      clearConfigurationFiles();

      super.create();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public StorageConfigurationImpl load(final ContextConfiguration configuration)
      throws SerializationException {
    lock.writeLock().lock();
    try {
      initConfiguration(configuration);

      final var file = storagePath.resolve(NAME);
      final var backupFile = storagePath.resolve(BACKUP_NAME);

      if (Files.exists(file)) {
        if (readData(file)) {
          return this;
        }

        LogManager.instance()
            .warn(
                this,
                "Main storage configuration file %s is broken in storage %s, try to read from"
                    + " backup file %s",
                file,
                storageName,
                backupFile);

        if (Files.exists(backupFile)) {
          if (readData(backupFile)) {
            return this;
          }

          LogManager.instance().error(this, "Backup configuration file %s is broken too", null);
          throw new StorageException(
              "Invalid format for configuration file " + file + " for storage" + storageName);
        } else {
          LogManager.instance()
              .error(this, "Backup configuration file %s does not exist", null, backupFile);
          throw new StorageException(
              "Invalid format for configuration file " + file + " for storage" + storageName);
        }
      } else if (Files.exists(backupFile)) {
        LogManager.instance()
            .warn(
                this,
                "Seems like previous update to the storage '%s' configuration was finished"
                    + " incorrectly, main configuration file %s is absent, reading from backup",
                backupFile,
                file);

        if (readData(backupFile)) {
          return this;
        }

        LogManager.instance()
            .error(this, "Backup configuration file %s is broken", null, backupFile);
        throw new StorageException(
            "Invalid format for configuration file " + backupFile + " for storage" + storageName);
      } else {
        throw new StorageException("Can not find configuration file for storage " + storageName);
      }
    } catch (IOException e) {
      throw BaseException.wrapException(
          new SerializationException(
              "Cannot load database configuration. The database seems corrupted"),
          e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void update() throws SerializationException {
    lock.writeLock().lock();
    try {
      final var utf8 = StandardCharsets.UTF_8;
      final var buffer = toStream(utf8);

      final var byteBuffer =
          ByteBuffer.allocate(buffer.length + IntegerSerializer.INT_SIZE);
      byteBuffer.putInt(buffer.length);
      byteBuffer.put(buffer);

      try {
        if (!Files.exists(storagePath)) {
          Files.createDirectories(storagePath);
        }

        final var backupFile = storagePath.resolve(BACKUP_NAME);
        Files.deleteIfExists(backupFile);

        try (var channel =
            FileChannel.open(backupFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
          writeConfigFile(buffer, byteBuffer, channel);
        }

        final var file = storagePath.resolve(NAME);
        Files.deleteIfExists(file);

        try (var channel =
            FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
          writeConfigFile(buffer, byteBuffer, channel);
        }

        Files.delete(backupFile);
      } catch (Exception e) {
        throw BaseException.wrapException(
            new SerializationException("Error on update storage configuration"), e);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void writeConfigFile(byte[] buffer, ByteBuffer byteBuffer, FileChannel channel)
      throws IOException {
    final var versionBuffer = ByteBuffer.allocate(1);
    versionBuffer.put(FORMAT_VERSION);

    versionBuffer.position(0);
    IOUtils.writeByteBuffer(versionBuffer, channel, VERSION_OFFSET);

    final var crc32buffer = ByteBuffer.allocate(IntegerSerializer.INT_SIZE);
    final var crc32 = new CRC32();
    crc32.update(buffer);
    crc32buffer.putInt((int) crc32.getValue());

    crc32buffer.position(0);
    IOUtils.writeByteBuffer(crc32buffer, channel, CRC_32_OFFSET);

    channel.force(true);

    byteBuffer.position(0);
    IOUtils.writeByteBuffer(byteBuffer, channel, File.HEADER_SIZE);

    channel.force(true);
  }

  private boolean readData(Path file) throws IOException {
    final ByteBuffer byteBuffer;
    final byte fileVersion;
    final int crc32content;

    try (final var channel = FileChannel.open(file, StandardOpenOption.READ)) {
      // file header + size of content + at least one byte of content
      if (channel.size()
          < File.HEADER_SIZE + IntegerSerializer.INT_SIZE + ByteSerializer.BYTE_SIZE) {
        return false;
      }

      final var versionBuffer = ByteBuffer.allocate(1);
      IOUtils.readByteBuffer(versionBuffer, channel, VERSION_OFFSET, true);
      versionBuffer.position(0);

      fileVersion = versionBuffer.get();
      if (fileVersion >= 42) {
        final var crc32buffer = ByteBuffer.allocate(IntegerSerializer.INT_SIZE);
        IOUtils.readByteBuffer(crc32buffer, channel, CRC_32_OFFSET, true);

        crc32buffer.position(0);
        crc32content = crc32buffer.getInt();
      } else {
        crc32content = 0;
      }

      byteBuffer = ByteBuffer.allocate((int) channel.size() - File.HEADER_SIZE);
      IOUtils.readByteBuffer(byteBuffer, channel, File.HEADER_SIZE, true);
    }

    byteBuffer.position(0);
    final var size = byteBuffer.getInt(); // size of string which contains database configuration
    var buffer = new byte[size];

    byteBuffer.get(buffer);

    if (fileVersion < 42) {
      if (byteBuffer.limit()
          >= size + 2 * IntegerSerializer.INT_SIZE + 3 * LongSerializer.LONG_SIZE) {
        final var encodingFagOne = byteBuffer.getLong();
        final var encodingFagTwo = byteBuffer.getLong();
        final var encodingFagThree = byteBuffer.getLong();

        final Charset streamEncoding;

        // those flags are added to distinguish between old version of configuration file and new
        // one.
        if (encodingFagOne == ENCODING_FLAG_1
            && encodingFagTwo == ENCODING_FLAG_2
            && encodingFagThree == ENCODING_FLAG_3) {
          final var utf8Encoded = "UTF-8".getBytes(StandardCharsets.UTF_8);

          final var encodingNameLength = byteBuffer.getInt();

          if (encodingNameLength == utf8Encoded.length) {
            final var binaryEncodingName = new byte[encodingNameLength];
            byteBuffer.get(binaryEncodingName);

            final var encodingName = new String(binaryEncodingName, StandardCharsets.UTF_8);

            if (encodingName.equals("UTF-8")) {
              streamEncoding = StandardCharsets.UTF_8;
            } else {
              return false;
            }

            try {
              fromStream(buffer, 0, buffer.length, streamEncoding);
            } catch (Exception e) {
              LogManager.instance()
                  .error(
                      this,
                      "Error during reading of configuration %s of storage %s",
                      e,
                      file,
                      storageName);
              return false;
            }

          } else {
            return false;
          }
        } else {
          try {
            fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
          } catch (Exception e) {
            LogManager.instance()
                .error(
                    this,
                    "Error during reading of configuration %s of storage %s",
                    e,
                    file,
                    storageName);
            return false;
          }
        }
      } else {
        try {
          fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
        } catch (Exception e) {
          LogManager.instance()
              .error(
                  this,
                  "Error during reading of configuration %s of storage %s",
                  e,
                  file,
                  storageName);
          return false;
        }
      }
    } else {
      var crc32 = new CRC32();
      crc32.update(buffer);

      if (crc32content != (int) crc32.getValue()) {
        return false;
      }

      try {
        fromStream(buffer, 0, buffer.length, StandardCharsets.UTF_8);
      } catch (Exception e) {
        LogManager.instance()
            .error(
                this,
                "Error during reading of configuration %s of storage %s",
                e,
                file,
                storageName);
        return false;
      }
    }

    return true;
  }
}
