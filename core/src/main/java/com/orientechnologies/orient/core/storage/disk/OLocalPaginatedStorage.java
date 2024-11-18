/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.disk;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog.MASTER_RECORD_EXTENSION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog.WAL_SEGMENT_EXTENSION;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OBackupInProgressException;
import com.orientechnologies.orient.core.exception.OInvalidInstanceIdException;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeMultiValueIndexEngine;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLog;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLogGL;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLogNoOP;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.v2.FreeSpaceMap;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStartupMetadata;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseStorageOperationListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.StorageStartupMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.MetaDataRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.storage.index.versionmap.OVersionPositionMap;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 28.03.13
 */
public class OLocalPaginatedStorage extends OAbstractPaginatedStorage {

  private static final String INCREMENTAL_BACKUP_LOCK = "backup.ibl";

  private static final String ALGORITHM_NAME = "AES";
  private static final String TRANSFORMATION = "AES/CTR/NoPadding";

  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(OLocalPaginatedStorage::getCipherInstance);

  private static final String IBU_EXTENSION_V3 = ".ibu3";
  private static final int INCREMENTAL_BACKUP_VERSION = 423;
  private static final String CONF_ENTRY_NAME = "database.ocf";
  private static final String INCREMENTAL_BACKUP_DATEFORMAT = "yyyy-MM-dd-HH-mm-ss";
  private static final String CONF_UTF_8_ENTRY_NAME = "database_utf8.ocf";

  private static final String ENCRYPTION_IV = "encryption.iv";

  private final List<OEnterpriseStorageOperationListener> listeners = new CopyOnWriteArrayList<>();

  @SuppressWarnings("WeakerAccess")
  protected static final long IV_SEED = 234120934;

  private static final String IV_EXT = ".iv";

  @SuppressWarnings("WeakerAccess")
  protected static final String IV_NAME = "data" + IV_EXT;

  private static final String[] ALL_FILE_EXTENSIONS = {
    ".cm",
    ".ocf",
    ".pls",
    ".pcl",
    ".oda",
    ".odh",
    ".otx",
    ".ocs",
    ".oef",
    ".oem",
    ".oet",
    ".fl",
    ".flb",
    IV_EXT,
    CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION,
    CASDiskWriteAheadLog.MASTER_RECORD_EXTENSION,
    OHashTableIndexEngine.BUCKET_FILE_EXTENSION,
    OHashTableIndexEngine.METADATA_FILE_EXTENSION,
    OHashTableIndexEngine.TREE_FILE_EXTENSION,
    OHashTableIndexEngine.NULL_BUCKET_FILE_EXTENSION,
    OClusterPositionMap.DEF_EXTENSION,
    OSBTreeIndexEngine.DATA_FILE_EXTENSION,
    OIndexRIDContainer.INDEX_FILE_EXTENSION,
    OSBTreeCollectionManagerShared.FILE_EXTENSION,
    OSBTreeIndexEngine.NULL_BUCKET_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.MAP_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.DATA_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.TREE_DATA_FILE_EXTENSION,
    OClusterBasedStorageConfiguration.TREE_NULL_FILE_EXTENSION,
    OCellBTreeMultiValueIndexEngine.DATA_FILE_EXTENSION,
    OCellBTreeMultiValueIndexEngine.M_CONTAINER_EXTENSION,
    DoubleWriteLogGL.EXTENSION,
    FreeSpaceMap.DEF_EXTENSION,
    OVersionPositionMap.DEF_EXTENSION
  };

  private static final int ONE_KB = 1024;

  private final int deleteMaxRetries;
  private final int deleteWaitTime;

  private final StorageStartupMetadata startupMetadata;

  private final Path storagePath;
  private final OClosableLinkedContainer<Long, OFile> files;

  private Future<?> fuzzyCheckpointTask;

  private final long walMaxSegSize;
  private final long doubleWriteLogMaxSegSize;

  protected volatile byte[] iv;

  public OLocalPaginatedStorage(
      final String name,
      final String filePath,
      final int id,
      final OReadCache readCache,
      final OClosableLinkedContainer<Long, OFile> files,
      final long walMaxSegSize,
      long doubleWriteLogMaxSegSize,
      OrientDBInternal context) {
    super(name, filePath, id, context);

    this.walMaxSegSize = walMaxSegSize;
    this.files = files;
    this.doubleWriteLogMaxSegSize = doubleWriteLogMaxSegSize;
    this.readCache = readCache;

    final String sp =
        OSystemVariableResolver.resolveSystemVariables(
            OFileUtils.getPath(new java.io.File(url).getPath()));

    storagePath = Paths.get(OIOUtils.getPathFromDatabaseName(sp)).normalize().toAbsolutePath();

    deleteMaxRetries = OGlobalConfiguration.FILE_DELETE_RETRY.getValueAsInteger();
    deleteWaitTime = OGlobalConfiguration.FILE_DELETE_DELAY.getValueAsInteger();

    startupMetadata =
        new StorageStartupMetadata(
            storagePath.resolve("dirty.fl"), storagePath.resolve("dirty.flb"));
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public void create(final OContextConfiguration contextConfiguration) {
    try {
      stateLock.writeLock().lock();
      try {
        doCreate(contextConfiguration);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    boolean fsyncAfterCreate =
        contextConfiguration.getValueAsBoolean(
            OGlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE);
    if (fsyncAfterCreate) {
      synch();
    }

    final Object[] additionalArgs = new Object[] {getURL(), OConstants.getVersion()};
    OLogManager.instance()
        .info(this, "Storage '%s' is created under OrientDB distribution : %s", additionalArgs);
  }

  protected void doCreate(OContextConfiguration contextConfiguration)
      throws IOException, InterruptedException {
    final Path storageFolder = storagePath;
    if (!Files.exists(storageFolder)) {
      Files.createDirectories(storageFolder);
    }

    super.doCreate(contextConfiguration);
  }

  @Override
  public final boolean exists() {
    try {
      if (status == STATUS.OPEN || isInError() || status == STATUS.MIGRATION) {
        return true;
      }

      return exists(storagePath);
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public String getURL() {
    return OEngineLocalPaginated.NAME + ":" + url;
  }

  public final Path getStoragePath() {
    return storagePath;
  }

  @Override
  public String getType() {
    return OEngineLocalPaginated.NAME;
  }

  @Override
  public final List<String> backup(
      final OutputStream out,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iOutput,
      final int compressionLevel,
      final int bufferSize) {
    stateLock.readLock().lock();
    try {
      if (out == null) {
        throw new IllegalArgumentException("Backup output is null");
      }

      freeze(false);
      try {
        if (callable != null) {
          try {
            callable.call();
          } catch (final Exception e) {
            OLogManager.instance().error(this, "Error on callback invocation during backup", e);
          }
        }
        OLogSequenceNumber freezeLSN = null;
        if (writeAheadLog != null) {
          freezeLSN = writeAheadLog.begin();
          writeAheadLog.addCutTillLimit(freezeLSN);
        }

        startupMetadata.setTxMetadata(getLastMetadata().orElse(null));
        try {
          final OutputStream bo = bufferSize > 0 ? new BufferedOutputStream(out, bufferSize) : out;
          try {
            try (final ZipOutputStream zos = new ZipOutputStream(bo)) {
              zos.setComment("OrientDB Backup executed on " + new Date());
              zos.setLevel(compressionLevel);

              final List<String> names =
                  OZIPCompressionUtil.compressDirectory(
                      storagePath.toString(),
                      zos,
                      new String[] {".fl", ".lock", DoubleWriteLogGL.EXTENSION},
                      iOutput);
              startupMetadata.addFileToArchive(zos, "dirty.fl");
              names.add("dirty.fl");
              return names;
            }
          } finally {
            if (bufferSize > 0) {
              bo.flush();
              bo.close();
            }
          }
        } finally {
          if (freezeLSN != null) {
            writeAheadLog.removeCutTillLimit(freezeLSN);
          }
        }

      } finally {
        release();
      }
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void restore(
      final InputStream in,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iListener) {
    try {
      stateLock.writeLock().lock();
      try {
        if (!isClosedInternal()) {
          doShutdown();
        }

        final java.io.File dbDir =
            new java.io.File(
                OIOUtils.getPathFromDatabaseName(
                    OSystemVariableResolver.resolveSystemVariables(url)));
        final java.io.File[] storageFiles = dbDir.listFiles();
        if (storageFiles != null) {
          // TRY TO DELETE ALL THE FILES
          for (final java.io.File f : storageFiles) {
            // DELETE ONLY THE SUPPORTED FILES
            for (final String ext : ALL_FILE_EXTENSIONS) {
              if (f.getPath().endsWith(ext)) {
                //noinspection ResultOfMethodCallIgnored
                f.delete();
                break;
              }
            }
          }
        }
        Files.createDirectories(Paths.get(storagePath.toString()));
        OZIPCompressionUtil.uncompressDirectory(in, storagePath.toString(), iListener);

        final java.io.File[] newStorageFiles = dbDir.listFiles();
        if (newStorageFiles != null) {
          // TRY TO DELETE ALL THE FILES
          for (final java.io.File f : newStorageFiles) {
            if (f.getPath().endsWith(MASTER_RECORD_EXTENSION)) {
              final boolean renamed =
                  f.renameTo(new File(f.getParent(), getName() + MASTER_RECORD_EXTENSION));
              assert renamed;
            }
            if (f.getPath().endsWith(WAL_SEGMENT_EXTENSION)) {
              String walName = f.getName();
              final int segmentIndex =
                  walName.lastIndexOf(".", walName.length() - WAL_SEGMENT_EXTENSION.length() - 1);
              String ending = walName.substring(segmentIndex);
              final boolean renamed = f.renameTo(new File(f.getParent(), getName() + ending));
              assert renamed;
            }
          }
        }

        if (callable != null) {
          try {
            callable.call();
          } catch (final Exception e) {
            OLogManager.instance().error(this, "Error on calling callback on database restore", e);
          }
        }
      } finally {
        stateLock.writeLock().unlock();
      }

      open(new OContextConfiguration());
      atomicOperationsManager.executeInsideAtomicOperation(null, this::generateDatabaseInstanceId);
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  protected OLogSequenceNumber copyWALToIncrementalBackup(
      final ZipOutputStream zipOutputStream, final long startSegment) throws IOException {

    java.io.File[] nonActiveSegments;

    OLogSequenceNumber lastLSN;
    final long freezeId = getAtomicOperationsManager().freezeAtomicOperations(null, null);
    try {
      lastLSN = writeAheadLog.end();
      writeAheadLog.flush();
      writeAheadLog.appendNewSegment();
      nonActiveSegments = writeAheadLog.nonActiveSegments(startSegment);
    } finally {
      getAtomicOperationsManager().releaseAtomicOperations(freezeId);
    }

    for (final java.io.File nonActiveSegment : nonActiveSegments) {
      try (final FileInputStream fileInputStream = new FileInputStream(nonActiveSegment)) {
        try (final BufferedInputStream bufferedInputStream =
            new BufferedInputStream(fileInputStream)) {
          final ZipEntry entry = new ZipEntry(nonActiveSegment.getName());
          zipOutputStream.putNextEntry(entry);
          try {
            final byte[] buffer = new byte[4096];

            int br;

            while ((br = bufferedInputStream.read(buffer)) >= 0) {
              zipOutputStream.write(buffer, 0, br);
            }
          } finally {
            zipOutputStream.closeEntry();
          }
        }
      }
    }

    return lastLSN;
  }

  @Override
  protected java.io.File createWalTempDirectory() {
    final java.io.File walDirectory =
        new java.io.File(storagePath.toFile(), "walIncrementalBackupRestoreDirectory");

    if (walDirectory.exists()) {
      OFileUtils.deleteRecursively(walDirectory);
    }

    if (!walDirectory.mkdirs()) {
      throw new OStorageException(
          "Can not create temporary directory to store files created during incremental backup");
    }

    return walDirectory;
  }

  private void addFileToDirectory(final String name, final InputStream stream, final File directory)
      throws IOException {
    final byte[] buffer = new byte[4096];

    int rb = -1;
    int bl = 0;

    final File walBackupFile = new File(directory, name);
    if (!walBackupFile.toPath().normalize().startsWith(directory.toPath().normalize())) {
      throw new IllegalStateException("Bad zip entry " + name);
    }

    try (final FileOutputStream outputStream = new FileOutputStream(walBackupFile)) {
      try (final BufferedOutputStream bufferedOutputStream =
          new BufferedOutputStream(outputStream)) {
        do {
          while (bl < buffer.length && (rb = stream.read(buffer, bl, buffer.length - bl)) > -1) {
            bl += rb;
          }

          bufferedOutputStream.write(buffer, 0, bl);
          bl = 0;

        } while (rb >= 0);
      }
    }
  }

  @Override
  protected OWriteAheadLog createWalFromIBUFiles(
      final java.io.File directory,
      final OContextConfiguration contextConfiguration,
      final Locale locale,
      byte[] iv)
      throws IOException {
    final String aesKeyEncoded =
        contextConfiguration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    final byte[] aesKey =
        Optional.ofNullable(aesKeyEncoded)
            .map(keyEncoded -> Base64.getDecoder().decode(keyEncoded))
            .orElse(null);

    return new CASDiskWriteAheadLog(
        name,
        storagePath,
        directory.toPath(),
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_CACHE_SIZE),
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_BUFFER_SIZE),
        aesKey,
        iv,
        contextConfiguration.getValueAsLong(OGlobalConfiguration.WAL_SEGMENTS_INTERVAL)
            * 60
            * 1_000_000_000L,
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE)
            * 1024
            * 1024L,
        10,
        true,
        locale,
        OGlobalConfiguration.WAL_MAX_SIZE.getValueAsLong() * 1024 * 1024,
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_COMMIT_TIMEOUT),
        contextConfiguration.getValueAsBoolean(OGlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT),
        contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC),
        contextConfiguration.getValueAsBoolean(
            OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS),
        contextConfiguration.getValueAsInteger(
            OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL));
  }

  @Override
  protected OStartupMetadata checkIfStorageDirty() throws IOException {
    if (startupMetadata.exists()) {
      startupMetadata.open(OConstants.getRawVersion());
    } else {
      startupMetadata.create(OConstants.getRawVersion());
      startupMetadata.makeDirty(OConstants.getRawVersion());
    }

    return new OStartupMetadata(startupMetadata.getLastTxId(), startupMetadata.getTxMetadata());
  }

  @Override
  protected void initConfiguration(
      OAtomicOperation atomicOperation, final OContextConfiguration contextConfiguration)
      throws IOException {
    if (!OClusterBasedStorageConfiguration.exists(writeCache)
        && Files.exists(storagePath.resolve("database.ocf"))) {
      final OStorageConfigurationSegment oldConfig = new OStorageConfigurationSegment(this);
      oldConfig.load(contextConfiguration);

      final OClusterBasedStorageConfiguration atomicConfiguration =
          new OClusterBasedStorageConfiguration(this);
      atomicConfiguration.create(atomicOperation, contextConfiguration, oldConfig);
      configuration = atomicConfiguration;

      oldConfig.close();
      Files.deleteIfExists(storagePath.resolve("database.ocf"));
    }

    if (configuration == null) {
      configuration = new OClusterBasedStorageConfiguration(this);
      ((OClusterBasedStorageConfiguration) configuration)
          .load(contextConfiguration, atomicOperation);
    }
  }

  @Override
  protected Map<String, Object> preCloseSteps() {
    final Map<String, Object> params = super.preCloseSteps();

    if (fuzzyCheckpointTask != null) {
      fuzzyCheckpointTask.cancel(false);
    }

    return params;
  }

  @Override
  protected void preCreateSteps() throws IOException {
    startupMetadata.create(OConstants.getRawVersion());
  }

  @Override
  protected void postCloseSteps(
      final boolean onDelete, final boolean internalError, final long lastTxId) throws IOException {
    if (onDelete) {
      startupMetadata.delete();
    } else {
      if (!internalError) {
        startupMetadata.setLastTxId(lastTxId);
        startupMetadata.setTxMetadata(getLastMetadata().orElse(null));

        startupMetadata.clearDirty();
      }
      startupMetadata.close();
    }
  }

  @Override
  protected void postDeleteSteps() {
    String databasePath =
        OIOUtils.getPathFromDatabaseName(OSystemVariableResolver.resolveSystemVariables(url));
    deleteFilesFromDisc(name, deleteMaxRetries, deleteWaitTime, databasePath);
  }

  public static void deleteFilesFromDisc(
      final String name, final int maxRetries, final int waitTime, final String databaseDirectory) {
    File dbDir = new java.io.File(databaseDirectory);
    if (!dbDir.exists() || !dbDir.isDirectory()) {
      dbDir = dbDir.getParentFile();
    }

    // RETRIES
    for (int i = 0; i < maxRetries; ++i) {
      if (dbDir != null && dbDir.exists() && dbDir.isDirectory()) {
        int notDeletedFiles = 0;

        final File[] storageFiles = dbDir.listFiles();
        if (storageFiles == null) {
          continue;
        }

        // TRY TO DELETE ALL THE FILES
        for (final File f : storageFiles) {
          // DELETE ONLY THE SUPPORTED FILES
          for (final String ext : ALL_FILE_EXTENSIONS) {
            if (f.getPath().endsWith(ext)) {
              if (!f.delete()) {
                notDeletedFiles++;
              }
              break;
            }
          }
        }

        if (notDeletedFiles == 0) {
          // TRY TO DELETE ALSO THE DIRECTORY IF IT'S EMPTY
          if (!dbDir.delete()) {
            OLogManager.instance()
                .error(
                    OLocalPaginatedStorage.class,
                    "Cannot delete storage directory with path "
                        + dbDir.getAbsolutePath()
                        + " because directory is not empty. Files: "
                        + Arrays.toString(dbDir.listFiles()),
                    null);
          }
          return;
        }
      } else {
        return;
      }
      OLogManager.instance()
          .debug(
              OLocalPaginatedStorage.class,
              "Cannot delete database files because they are still locked by the OrientDB process:"
                  + " waiting %d ms and retrying %d/%d...",
              waitTime,
              i,
              maxRetries);
    }

    throw new OStorageException(
        "Cannot delete database '"
            + name
            + "' located in: "
            + dbDir
            + ". Database files seem locked");
  }

  @Override
  protected void makeStorageDirty() throws IOException {
    startupMetadata.makeDirty(OConstants.getRawVersion());
  }

  @Override
  protected void clearStorageDirty() throws IOException {
    if (!isInError()) {
      startupMetadata.clearDirty();
    }
  }

  @Override
  protected boolean isDirty() {
    return startupMetadata.isDirty();
  }

  protected String getOpenedAtVersion() {
    return startupMetadata.getOpenedAtVersion();
  }

  @Override
  protected boolean isWriteAllowedDuringIncrementalBackup() {
    return true;
  }

  @Override
  protected void initIv() throws IOException {
    try (final RandomAccessFile ivFile =
        new RandomAccessFile(storagePath.resolve(IV_NAME).toAbsolutePath().toFile(), "rw")) {
      final byte[] iv = new byte[16];

      final SecureRandom random = new SecureRandom();
      random.nextBytes(iv);

      final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
      final XXHash64 hash64 = hashFactory.hash64();

      final long hash = hash64.hash(iv, 0, iv.length, IV_SEED);
      ivFile.write(iv);
      ivFile.writeLong(hash);
      ivFile.getFD().sync();

      this.iv = iv;
    }
  }

  @Override
  protected void readIv() throws IOException {
    final Path ivPath = storagePath.resolve(IV_NAME).toAbsolutePath();
    if (!Files.exists(ivPath)) {
      OLogManager.instance().info(this, "IV file is absent, will create new one.");
      initIv();
      return;
    }

    try (final RandomAccessFile ivFile = new RandomAccessFile(ivPath.toFile(), "r")) {
      final byte[] iv = new byte[16];
      ivFile.readFully(iv);

      final long storedHash = ivFile.readLong();

      final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
      final XXHash64 hash64 = hashFactory.hash64();

      final long expectedHash = hash64.hash(iv, 0, iv.length, IV_SEED);
      if (storedHash != expectedHash) {
        throw new OStorageException("iv data are broken");
      }

      this.iv = iv;
    }
  }

  @Override
  protected byte[] getIv() {
    return iv;
  }

  @Override
  protected void initWalAndDiskCache(final OContextConfiguration contextConfiguration)
      throws IOException, InterruptedException {
    final String aesKeyEncoded =
        contextConfiguration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    final byte[] aesKey =
        Optional.ofNullable(aesKeyEncoded)
            .map(keyEncoded -> Base64.getDecoder().decode(keyEncoded))
            .orElse(null);

    fuzzyCheckpointTask =
        fuzzyCheckpointExecutor.scheduleWithFixedDelay(
            new OPeriodicFuzzyCheckpoint(this),
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL),
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL),
            TimeUnit.SECONDS);

    final String configWalPath =
        contextConfiguration.getValueAsString(OGlobalConfiguration.WAL_LOCATION);
    final Path walPath;
    if (configWalPath == null) {
      walPath = null;
    } else {
      walPath = Paths.get(configWalPath);
    }

    writeAheadLog =
        new CASDiskWriteAheadLog(
            name,
            storagePath,
            walPath,
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_CACHE_SIZE),
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_BUFFER_SIZE),
            aesKey,
            iv,
            contextConfiguration.getValueAsLong(OGlobalConfiguration.WAL_SEGMENTS_INTERVAL)
                * 60
                * 1_000_000_000L,
            walMaxSegSize,
            10,
            true,
            Locale.getDefault(),
            contextConfiguration.getValueAsLong(OGlobalConfiguration.WAL_MAX_SIZE) * 1024 * 1024,
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_COMMIT_TIMEOUT),
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT),
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC),
            contextConfiguration.getValueAsBoolean(
                OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS),
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL));
    writeAheadLog.addCheckpointListener(this);

    final int pageSize =
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE) * ONE_KB;
    final long diskCacheSize =
        contextConfiguration.getValueAsLong(OGlobalConfiguration.DISK_CACHE_SIZE) * 1024 * 1024;
    final long writeCacheSize =
        (long)
            (contextConfiguration.getValueAsInteger(OGlobalConfiguration.DISK_WRITE_CACHE_PART)
                / 100.0
                * diskCacheSize);

    final DoubleWriteLog doubleWriteLog;
    if (contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_USE_DOUBLE_WRITE_LOG)) {
      doubleWriteLog = new DoubleWriteLogGL(doubleWriteLogMaxSegSize);
    } else {
      doubleWriteLog = new DoubleWriteLogNoOP();
    }

    final OWOWCache wowCache =
        new OWOWCache(
            pageSize,
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.FILE_LOG_DELETION),
            OByteBufferPool.instance(null),
            writeAheadLog,
            doubleWriteLog,
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL),
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT),
            writeCacheSize,
            storagePath,
            getName(),
            OStringSerializer.INSTANCE,
            files,
            getId(),
            contextConfiguration.getValueAsEnum(
                OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.class),
            iv,
            aesKey,
            contextConfiguration.getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC),
            ((OrientDBEmbedded) context).getIoExecutor());

    wowCache.loadRegisteredFiles();
    wowCache.addBackgroundExceptionListener(this);
    wowCache.addPageIsBrokenListener(this);

    writeCache = wowCache;
  }

  public static boolean exists(final Path path) {
    try {
      final boolean[] exists = new boolean[1];
      if (Files.exists(path.normalize().toAbsolutePath())) {
        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
          stream.forEach(
              (p) -> {
                final String fileName = p.getFileName().toString();
                if (fileName.equals("database.ocf")
                    || (fileName.startsWith("config") && fileName.endsWith(".bd"))
                    || fileName.startsWith("dirty.fl")
                    || fileName.startsWith("dirty.flb")) {
                  exists[0] = true;
                }
              });
        }
        return exists[0];
      }

      return false;
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during fetching list of files"), e);
    }
  }

  @Override
  public String incrementalBackup(final String backupDirectory, OCallable<Void, Void> started) {
    return incrementalBackup(new File(backupDirectory), started);
  }

  @Override
  public boolean supportIncremental() {
    return true;
  }

  @Override
  public void fullIncrementalBackup(final OutputStream stream) {
    try {
      incrementalBackup(stream, null, false);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    }
  }

  @SuppressWarnings("unused")
  public boolean isLastBackupCompatibleWithUUID(final File backupDirectory) throws IOException {
    if (!backupDirectory.exists()) {
      return true;
    }

    final Path fileLockPath = backupDirectory.toPath().resolve(INCREMENTAL_BACKUP_LOCK);
    try (FileChannel lockChannel =
        FileChannel.open(fileLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      try (final FileLock ignored = lockChannel.lock()) {
        final String[] files = fetchIBUFiles(backupDirectory);
        if (files.length > 0) {
          UUID backupUUID =
              extractDbInstanceUUID(backupDirectory, files[0], configuration.getCharset());
          try {
            checkDatabaseInstanceId(backupUUID);
          } catch (OInvalidInstanceIdException ex) {
            return false;
          }
        }
      } catch (final OverlappingFileLockException e) {
        OLogManager.instance()
            .error(
                this,
                "Another incremental backup process is in progress, please wait till it will be"
                    + " finished",
                null);
      } catch (final IOException e) {
        throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
      }

      try {
        Files.deleteIfExists(fileLockPath);
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
      }
    }
    return true;
  }

  private String incrementalBackup(
      final File backupDirectory, final OCallable<Void, Void> started) {
    String fileName = "";

    if (!backupDirectory.exists()) {
      if (!backupDirectory.mkdirs()) {
        throw new OStorageException(
            "Backup directory "
                + backupDirectory.getAbsolutePath()
                + " does not exist and can not be created");
      }
    }
    checkNoBackupInStorageDir(backupDirectory);

    final Path fileLockPath = backupDirectory.toPath().resolve(INCREMENTAL_BACKUP_LOCK);
    try (final FileChannel lockChannel =
        FileChannel.open(fileLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      try (@SuppressWarnings("unused")
          final FileLock fileLock = lockChannel.lock()) {
        RandomAccessFile rndIBUFile = null;
        try {
          final String[] files = fetchIBUFiles(backupDirectory);

          final OLogSequenceNumber lastLsn;
          long nextIndex;
          final UUID backupUUID;

          if (files.length == 0) {
            lastLsn = null;
            nextIndex = 0;
          } else {
            lastLsn = extractIBULsn(backupDirectory, files[files.length - 1]);
            nextIndex = extractIndexFromIBUFile(backupDirectory, files[files.length - 1]) + 1;
            backupUUID =
                extractDbInstanceUUID(backupDirectory, files[0], configuration.getCharset());
            checkDatabaseInstanceId(backupUUID);
          }

          final SimpleDateFormat dateFormat = new SimpleDateFormat(INCREMENTAL_BACKUP_DATEFORMAT);
          if (lastLsn != null) {
            fileName =
                getName()
                    + "_"
                    + dateFormat.format(new Date())
                    + "_"
                    + nextIndex
                    + IBU_EXTENSION_V3;
          } else {
            fileName =
                getName()
                    + "_"
                    + dateFormat.format(new Date())
                    + "_"
                    + nextIndex
                    + "_full"
                    + IBU_EXTENSION_V3;
          }

          final File ibuFile = new File(backupDirectory, fileName);

          if (started != null) {
            started.call(null);
          }
          rndIBUFile = new RandomAccessFile(ibuFile, "rw");
          try {
            final FileChannel ibuChannel = rndIBUFile.getChannel();

            final ByteBuffer versionBuffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
            versionBuffer.putInt(INCREMENTAL_BACKUP_VERSION);
            versionBuffer.rewind();

            OIOUtils.writeByteBuffer(versionBuffer, ibuChannel, 0);

            ibuChannel.position(
                2 * OIntegerSerializer.INT_SIZE
                    + 2 * OLongSerializer.LONG_SIZE
                    + OByteSerializer.BYTE_SIZE);

            OLogSequenceNumber maxLsn;
            try (OutputStream stream = Channels.newOutputStream(ibuChannel)) {
              maxLsn = incrementalBackup(stream, lastLsn, true);
              final ByteBuffer dataBuffer =
                  ByteBuffer.allocate(
                      OIntegerSerializer.INT_SIZE
                          + 2 * OLongSerializer.LONG_SIZE
                          + OByteSerializer.BYTE_SIZE);

              dataBuffer.putLong(nextIndex);
              dataBuffer.putLong(maxLsn.getSegment());
              dataBuffer.putInt(maxLsn.getPosition());

              if (lastLsn == null) {
                dataBuffer.put((byte) 1);
              } else {
                dataBuffer.put((byte) 0);
              }

              dataBuffer.rewind();

              ibuChannel.write(dataBuffer);
              OIOUtils.writeByteBuffer(dataBuffer, ibuChannel, OIntegerSerializer.INT_SIZE);
            }
          } catch (RuntimeException e) {
            rndIBUFile.close();

            if (!ibuFile.delete()) {
              OLogManager.instance()
                  .error(
                      this, ibuFile.getAbsolutePath() + " is closed but can not be deleted", null);
            }

            throw e;
          }
        } catch (IOException e) {
          throw OException.wrapException(
              new OStorageException("Error during incremental backup"), e);
        } finally {
          try {
            if (rndIBUFile != null) {
              rndIBUFile.close();
            }
          } catch (IOException e) {
            OLogManager.instance().error(this, "Can not close %s file", e, fileName);
          }
        }
      }
    } catch (final OverlappingFileLockException e) {
      OLogManager.instance()
          .error(
              this,
              "Another incremental backup process is in progress, please wait till it will be"
                  + " finished",
              null);
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    }

    try {
      Files.deleteIfExists(fileLockPath);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during incremental backup"), e);
    }

    return fileName;
  }

  private UUID extractDbInstanceUUID(File backupDirectory, String file, String charset)
      throws IOException {
    final File ibuFile = new File(backupDirectory, file);
    final RandomAccessFile rndIBUFile;
    try {
      rndIBUFile = new RandomAccessFile(ibuFile, "r");
    } catch (FileNotFoundException e) {
      throw OException.wrapException(new OStorageException("Backup file was not found"), e);
    }

    try {
      final FileChannel ibuChannel = rndIBUFile.getChannel();
      ibuChannel.position(3 * OLongSerializer.LONG_SIZE + 1);

      final InputStream inputStream = Channels.newInputStream(ibuChannel);
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      final ZipInputStream zipInputStream =
          new ZipInputStream(bufferedInputStream, Charset.forName(charset));

      ZipEntry zipEntry;
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        if (zipEntry.getName().equals("database_instance.uuid")) {
          DataInputStream dis = new DataInputStream(zipInputStream);
          return UUID.fromString(dis.readUTF());
        }
      }
    } finally {
      rndIBUFile.close();
    }
    return null;
  }

  private void checkNoBackupInStorageDir(final File backupDirectory) {
    if (getStoragePath() == null || backupDirectory == null) {
      return;
    }

    boolean invalid = false;
    final File storageDir = getStoragePath().toFile();
    if (backupDirectory.equals(storageDir)) {
      invalid = true;
    }

    if (invalid) {
      throw new OStorageException("Backup cannot be performed in the storage path");
    }
  }

  @SuppressWarnings("unused")
  public void registerStorageListener(OEnterpriseStorageOperationListener listener) {
    this.listeners.add(listener);
  }

  @SuppressWarnings("unused")
  public void unRegisterStorageListener(OEnterpriseStorageOperationListener listener) {
    this.listeners.remove(listener);
  }

  private String[] fetchIBUFiles(final File backupDirectory) throws IOException {
    final String[] files =
        backupDirectory.list(
            (dir, name) ->
                new File(dir, name).length() > 0 && name.toLowerCase().endsWith(IBU_EXTENSION_V3));

    if (files == null) {
      throw new OStorageException(
          "Can not read list of backup files from directory " + backupDirectory.getAbsolutePath());
    }

    final List<OPair<Long, String>> indexedFiles = new ArrayList<>(files.length);

    for (String file : files) {
      final long fileIndex = extractIndexFromIBUFile(backupDirectory, file);
      indexedFiles.add(new OPair<>(fileIndex, file));
    }

    Collections.sort(indexedFiles);

    final String[] sortedFiles = new String[files.length];

    int index = 0;
    for (OPair<Long, String> indexedFile : indexedFiles) {
      sortedFiles[index] = indexedFile.getValue();
      index++;
    }

    return sortedFiles;
  }

  private OLogSequenceNumber extractIBULsn(File backupDirectory, String file) {
    final File ibuFile = new File(backupDirectory, file);
    final RandomAccessFile rndIBUFile;
    try {
      rndIBUFile = new RandomAccessFile(ibuFile, "r");
    } catch (FileNotFoundException e) {
      throw OException.wrapException(new OStorageException("Backup file was not found"), e);
    }

    try {
      try {
        final FileChannel ibuChannel = rndIBUFile.getChannel();
        ibuChannel.position(OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

        ByteBuffer lsnData =
            ByteBuffer.allocate(OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
        ibuChannel.read(lsnData);
        lsnData.rewind();

        final long segment = lsnData.getLong();
        final int position = lsnData.getInt();

        return new OLogSequenceNumber(segment, position);
      } finally {
        rndIBUFile.close();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of backup file"), e);
    } finally {
      try {
        rndIBUFile.close();
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during read of backup file", e);
      }
    }
  }

  private long extractIndexFromIBUFile(final File backupDirectory, final String fileName)
      throws IOException {
    final File file = new File(backupDirectory, fileName);

    try (final RandomAccessFile rndFile = new RandomAccessFile(file, "r")) {
      rndFile.seek(OIntegerSerializer.INT_SIZE);
      return validateLongIndex(rndFile.readLong());
    }
  }

  private long validateLongIndex(final long index) {
    return index < 0 ? 0 : Math.abs(index);
  }

  private OLogSequenceNumber incrementalBackup(
      final OutputStream stream, final OLogSequenceNumber fromLsn, final boolean singleThread)
      throws IOException {
    OLogSequenceNumber lastLsn;

    checkOpennessAndMigration();

    if (singleThread && isIcrementalBackupRunning()) {
      throw new OBackupInProgressException(
          "You are trying to start incremental backup but it is in progress now, please wait till"
              + " it will be finished",
          getName(),
          OErrorCode.BACKUP_IN_PROGRESS);
    }
    startBackup();
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();
      final long freezeId;

      if (!isWriteAllowedDuringIncrementalBackup()) {
        freezeId =
            atomicOperationsManager.freezeAtomicOperations(
                OModificationOperationProhibitedException.class, "Incremental backup in progress");
      } else {
        freezeId = -1;
      }

      try {
        final ZipOutputStream zipOutputStream =
            new ZipOutputStream(
                new BufferedOutputStream(stream), Charset.forName(configuration.getCharset()));
        try {
          final long startSegment;
          final OLogSequenceNumber freezeLsn;

          if (fromLsn == null) {
            UUID databaseInstanceUUID = super.readDatabaseInstanceId();
            if (databaseInstanceUUID == null) {
              atomicOperationsManager.executeInsideAtomicOperation(
                  null, this::generateDatabaseInstanceId);
              databaseInstanceUUID = super.readDatabaseInstanceId();
            }
            final ZipEntry zipEntry = new ZipEntry("database_instance.uuid");

            zipOutputStream.putNextEntry(zipEntry);
            DataOutputStream dos = new DataOutputStream(zipOutputStream);
            dos.writeUTF(databaseInstanceUUID.toString());
            dos.flush();
          }

          final long newSegmentFreezeId =
              atomicOperationsManager.freezeAtomicOperations(null, null);
          try {
            final OLogSequenceNumber startLsn = writeAheadLog.end();

            if (startLsn != null) {
              freezeLsn = startLsn;
            } else {
              freezeLsn = new OLogSequenceNumber(0, 0);
            }

            writeAheadLog.addCutTillLimit(freezeLsn);

            writeAheadLog.appendNewSegment();
            startSegment = writeAheadLog.activeSegment();

            getLastMetadata()
                .ifPresent(
                    metadata -> {
                      try {
                        writeAheadLog.log(new MetaDataRecord(metadata));
                      } catch (final IOException e) {
                        throw new IllegalStateException("Error during write of metadata", e);
                      }
                    });
          } finally {
            atomicOperationsManager.releaseAtomicOperations(newSegmentFreezeId);
          }

          try {
            backupIv(zipOutputStream);

            final byte[] encryptionIv = new byte[16];
            final SecureRandom secureRandom = new SecureRandom();
            secureRandom.nextBytes(encryptionIv);

            backupEncryptedIv(zipOutputStream, encryptionIv);

            final String aesKeyEncoded =
                getConfiguration()
                    .getContextConfiguration()
                    .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
            final byte[] aesKey =
                aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

            if (aesKey != null
                && aesKey.length != 16
                && aesKey.length != 24
                && aesKey.length != 32) {
              throw new OInvalidStorageEncryptionKeyException(
                  "Invalid length of the encryption key, provided size is " + aesKey.length);
            }

            lastLsn = backupPagesWithChanges(fromLsn, zipOutputStream, encryptionIv, aesKey);
            final OLogSequenceNumber lastWALLsn =
                copyWALToIncrementalBackup(zipOutputStream, startSegment);

            if (lastWALLsn != null && (lastLsn == null || lastWALLsn.compareTo(lastLsn) > 0)) {
              lastLsn = lastWALLsn;
            }
          } finally {
            writeAheadLog.removeCutTillLimit(freezeLsn);
          }
        } finally {
          try {
            zipOutputStream.finish();
            zipOutputStream.flush();
          } catch (IOException e) {
            OLogManager.instance().warn(this, "Failed to flush resource " + zipOutputStream);
          }
        }
      } finally {
        if (!isWriteAllowedDuringIncrementalBackup()) {
          atomicOperationsManager.releaseAtomicOperations(freezeId);
        }
      }
    } finally {
      stateLock.readLock().unlock();
      endBackup();
    }

    return lastLsn;
  }

  private static void doEncryptionDecryption(
      final int mode,
      final byte[] aesKey,
      final long pageIndex,
      final long fileId,
      final byte[] backUpPage,
      final byte[] encryptionIv) {
    try {
      final Cipher cipher = CIPHER.get();
      final SecretKey secretKey = new SecretKeySpec(aesKey, ALGORITHM_NAME);

      final byte[] updatedIv = new byte[16];
      for (int i = 0; i < OLongSerializer.LONG_SIZE; i++) {
        updatedIv[i] = (byte) (encryptionIv[i] ^ ((pageIndex >>> i) & 0xFF));
      }

      for (int i = 0; i < OLongSerializer.LONG_SIZE; i++) {
        updatedIv[i + OLongSerializer.LONG_SIZE] =
            (byte) (encryptionIv[i + OLongSerializer.LONG_SIZE] ^ ((fileId >>> i) & 0xFF));
      }

      cipher.init(mode, secretKey, new IvParameterSpec(updatedIv));

      final byte[] data =
          cipher.doFinal(
              backUpPage, OLongSerializer.LONG_SIZE, backUpPage.length - OLongSerializer.LONG_SIZE);
      System.arraycopy(
          data,
          0,
          backUpPage,
          OLongSerializer.LONG_SIZE,
          backUpPage.length - OLongSerializer.LONG_SIZE);
    } catch (InvalidKeyException e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(e.getMessage()), e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("Invalid IV.", e);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new IllegalStateException("Unexpected exception during CRT encryption.", e);
    }
  }

  private void backupEncryptedIv(final ZipOutputStream zipOutputStream, final byte[] encryptionIv)
      throws IOException {
    final ZipEntry zipEntry = new ZipEntry(ENCRYPTION_IV);
    zipOutputStream.putNextEntry(zipEntry);

    zipOutputStream.write(encryptionIv);
    zipOutputStream.closeEntry();
  }

  private void backupIv(final ZipOutputStream zipOutputStream) throws IOException {
    final ZipEntry zipEntry = new ZipEntry(IV_NAME);
    zipOutputStream.putNextEntry(zipEntry);

    zipOutputStream.write(this.iv);
    zipOutputStream.closeEntry();
  }

  private byte[] restoreIv(final ZipInputStream zipInputStream) throws IOException {
    final byte[] iv = new byte[16];
    OIOUtils.readFully(zipInputStream, iv, 0, iv.length);

    return iv;
  }

  private OLogSequenceNumber backupPagesWithChanges(
      final OLogSequenceNumber changeLsn,
      final ZipOutputStream stream,
      final byte[] encryptionIv,
      final byte[] aesKey)
      throws IOException {
    OLogSequenceNumber lastLsn = changeLsn;

    final Map<String, Long> files = writeCache.files();
    final int pageSize = writeCache.pageSize();

    for (Map.Entry<String, Long> entry : files.entrySet()) {
      final String fileName = entry.getKey();

      long fileId = entry.getValue();
      fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

      final long filledUpTo = writeCache.getFilledUpTo(fileId);
      final ZipEntry zipEntry = new ZipEntry(fileName);

      stream.putNextEntry(zipEntry);

      final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serialize(fileId, binaryFileId, 0);
      stream.write(binaryFileId, 0, binaryFileId.length);

      for (int pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry =
            readCache.silentLoadForRead(fileId, pageIndex, writeCache, true);
        cacheEntry.acquireSharedLock();
        try {
          var cachePointer = cacheEntry.getCachePointer();
          assert cachePointer != null;

          var cachePointerBuffer = cachePointer.getBuffer();
          assert cachePointerBuffer != null;

          final OLogSequenceNumber pageLsn =
              ODurablePage.getLogSequenceNumberFromPage(cachePointerBuffer);

          if (changeLsn == null || pageLsn.compareTo(changeLsn) > 0) {

            final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];
            OLongSerializer.INSTANCE.serializeNative(pageIndex, data, 0);
            ODurablePage.getPageData(cachePointerBuffer, data, OLongSerializer.LONG_SIZE, pageSize);

            if (aesKey != null) {
              doEncryptionDecryption(
                  Cipher.ENCRYPT_MODE, aesKey, fileId, pageIndex, data, encryptionIv);
            }

            stream.write(data);

            if (lastLsn == null || pageLsn.compareTo(lastLsn) > 0) {
              lastLsn = pageLsn;
            }
          }
        } finally {
          cacheEntry.releaseSharedLock();
          readCache.releaseFromRead(cacheEntry);
        }
      }

      stream.closeEntry();
    }

    return lastLsn;
  }

  public void restoreFromIncrementalBackup(final String filePath) {
    restoreFromIncrementalBackup(new File(filePath));
  }

  @Override
  public void restoreFullIncrementalBackup(final InputStream stream)
      throws UnsupportedOperationException {
    stateLock.writeLock().lock();
    try {
      final String aesKeyEncoded =
          getConfiguration()
              .getContextConfiguration()
              .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
      final byte[] aesKey =
          aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

      if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
        throw new OInvalidStorageEncryptionKeyException(
            "Invalid length of the encryption key, provided size is " + aesKey.length);
      }

      var result = preprocessingIncrementalRestore();
      restoreFromIncrementalBackup(
          result.charset,
          result.serverLocale,
          result.locale,
          result.contextConfiguration,
          aesKey,
          stream,
          true);

      postProcessIncrementalRestore(result.contextConfiguration);
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during restore from incremental backup"), e);
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private IncrementalRestorePreprocessingResult preprocessingIncrementalRestore()
      throws IOException {
    final Locale serverLocale = configuration.getLocaleInstance();
    final OContextConfiguration contextConfiguration = configuration.getContextConfiguration();
    final String charset = configuration.getCharset();
    final Locale locale = configuration.getLocaleInstance();

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation -> {
          closeClusters();
          closeIndexes(atomicOperation);
          ((OClusterBasedStorageConfiguration) configuration).close(atomicOperation);
        });

    configuration = null;

    return new IncrementalRestorePreprocessingResult(
        serverLocale, contextConfiguration, charset, locale);
  }

  private void restoreFromIncrementalBackup(final File backupDirectory) {
    if (!backupDirectory.exists()) {
      throw new OStorageException(
          "Directory which should contain incremental backup files (files with extension '"
              + IBU_EXTENSION_V3
              + "') is absent. It should be located at '"
              + backupDirectory.getAbsolutePath()
              + "'");
    }

    try {
      final String[] files = fetchIBUFiles(backupDirectory);
      if (files.length == 0) {
        throw new OStorageException(
            "Cannot find incremental backup files (files with extension '"
                + IBU_EXTENSION_V3
                + "') in directory '"
                + backupDirectory.getAbsolutePath()
                + "'");
      }

      stateLock.writeLock().lock();
      try {

        final String aesKeyEncoded =
            getConfiguration()
                .getContextConfiguration()
                .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
        final byte[] aesKey =
            aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

        if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
          throw new OInvalidStorageEncryptionKeyException(
              "Invalid length of the encryption key, provided size is " + aesKey.length);
        }

        var result = preprocessingIncrementalRestore();
        UUID restoreUUID = extractDbInstanceUUID(backupDirectory, files[0], result.charset);

        for (String file : files) {
          UUID fileUUID = extractDbInstanceUUID(backupDirectory, files[0], result.charset);
          if ((restoreUUID == null && fileUUID == null)
              || (restoreUUID != null && restoreUUID.equals(fileUUID))) {
            final File ibuFile = new File(backupDirectory, file);

            RandomAccessFile rndIBUFile = new RandomAccessFile(ibuFile, "rw");
            try {
              final FileChannel ibuChannel = rndIBUFile.getChannel();
              final ByteBuffer versionBuffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
              OIOUtils.readByteBuffer(versionBuffer, ibuChannel);
              versionBuffer.rewind();

              final int backupVersion = versionBuffer.getInt();
              if (backupVersion != INCREMENTAL_BACKUP_VERSION) {
                throw new OStorageException(
                    "Invalid version of incremental backup version was provided. Expected "
                        + INCREMENTAL_BACKUP_VERSION
                        + " , provided "
                        + backupVersion);
              }

              ibuChannel.position(2 * OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE);
              final ByteBuffer buffer = ByteBuffer.allocate(1);
              ibuChannel.read(buffer);
              buffer.rewind();

              final boolean fullBackup = buffer.get() == 1;

              try (final InputStream inputStream = Channels.newInputStream(ibuChannel)) {
                restoreFromIncrementalBackup(
                    result.charset,
                    result.serverLocale,
                    result.locale,
                    result.contextConfiguration,
                    aesKey,
                    inputStream,
                    fullBackup);
              }
            } finally {
              try {
                rndIBUFile.close();
              } catch (IOException e) {
                OLogManager.instance().warn(this, "Failed to close resource " + rndIBUFile);
              }
            }
          } else {
            OLogManager.instance()
                .warn(
                    this,
                    "Skipped file '"
                        + file
                        + "' is not a backup of the same database of previous backups");
          }

          postProcessIncrementalRestore(result.contextConfiguration);
        }
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during restore from incremental backup"), e);
    }
  }

  private void postProcessIncrementalRestore(OContextConfiguration contextConfiguration)
      throws IOException {
    if (OClusterBasedStorageConfiguration.exists(writeCache)) {
      configuration = new OClusterBasedStorageConfiguration(this);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              ((OClusterBasedStorageConfiguration) configuration)
                  .load(contextConfiguration, atomicOperation));
    } else {
      if (Files.exists(getStoragePath().resolve("database.ocf"))) {
        final OStorageConfigurationSegment oldConfig = new OStorageConfigurationSegment(this);
        oldConfig.load(contextConfiguration);

        final OClusterBasedStorageConfiguration atomicConfiguration =
            new OClusterBasedStorageConfiguration(this);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                atomicConfiguration.create(atomicOperation, contextConfiguration, oldConfig));
        configuration = atomicConfiguration;

        oldConfig.close();
        Files.deleteIfExists(getStoragePath().resolve("database.ocf"));
      }

      if (configuration == null) {
        configuration = new OClusterBasedStorageConfiguration(this);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                ((OClusterBasedStorageConfiguration) configuration)
                    .load(contextConfiguration, atomicOperation));
      }
    }

    atomicOperationsManager.executeInsideAtomicOperation(null, this::openClusters);
    sbTreeCollectionManager.close();
    sbTreeCollectionManager.load();
    openIndexes();

    flushAllData();

    atomicOperationsManager.executeInsideAtomicOperation(null, this::generateDatabaseInstanceId);
  }

  private void restoreFromIncrementalBackup(
      final String charset,
      final Locale serverLocale,
      final Locale locale,
      final OContextConfiguration contextConfiguration,
      final byte[] aesKey,
      final InputStream inputStream,
      final boolean isFull)
      throws IOException {
    final List<String> currentFiles = new ArrayList<>(writeCache.files().keySet());

    final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    final ZipInputStream zipInputStream =
        new ZipInputStream(bufferedInputStream, Charset.forName(charset));
    final int pageSize = writeCache.pageSize();

    ZipEntry zipEntry;
    OLogSequenceNumber maxLsn = null;

    List<String> processedFiles = new ArrayList<>();

    if (isFull) {
      final Map<String, Long> files = writeCache.files();
      for (Map.Entry<String, Long> entry : files.entrySet()) {
        final long fileId = writeCache.fileIdByName(entry.getKey());

        assert entry.getValue().equals(fileId);
        readCache.deleteFile(fileId, writeCache);
      }
    }

    final File walTempDir = createWalTempDirectory();

    byte[] encryptionIv = null;
    byte[] walIv = null;

    entryLoop:
    while ((zipEntry = zipInputStream.getNextEntry()) != null) {
      switch (zipEntry.getName()) {
        case IV_NAME -> {
          walIv = restoreIv(zipInputStream);
          continue;
        }
        case ENCRYPTION_IV -> {
          encryptionIv = restoreEncryptionIv(zipInputStream);
          continue;
        }
        case CONF_ENTRY_NAME -> {
          replaceConfiguration(zipInputStream);

          continue;
        }
      }

      if (zipEntry.getName().equalsIgnoreCase("database_instance.uuid")) {
        continue;
      }

      if (zipEntry.getName().equals(CONF_UTF_8_ENTRY_NAME)) {
        replaceConfiguration(zipInputStream);

        continue;
      }

      if (zipEntry
          .getName()
          .toLowerCase(serverLocale)
          .endsWith(CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION)) {
        final String walName = zipEntry.getName();
        final int segmentIndex =
            walName.lastIndexOf(
                ".", walName.length() - CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION.length() - 1);
        final String storageName = getName();

        if (segmentIndex < 0) {
          throw new IllegalStateException("Can not find index of WAL segment");
        }

        addFileToDirectory(
            storageName + walName.substring(segmentIndex), zipInputStream, walTempDir);
        continue;
      }

      if (aesKey != null && encryptionIv == null) {
        throw new OSecurityException("IV can not be null if encryption key is provided");
      }

      final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
      OIOUtils.readFully(zipInputStream, binaryFileId, 0, binaryFileId.length);

      final long expectedFileId = OLongSerializer.INSTANCE.deserialize(binaryFileId, 0);
      long fileId;

      var rootDirectory = getStoragePath();
      var zipEntryPath = rootDirectory.resolve(zipEntry.getName()).normalize();

      if (!zipEntryPath.startsWith(rootDirectory)) {
        throw new IllegalStateException("Bad zip entry " + zipEntry.getName());
      }
      if (!zipEntryPath.getParent().equals(rootDirectory)) {
        throw new IllegalStateException("Bad zip entry " + zipEntry.getName());
      }

      var fileName = zipEntryPath.getFileName().toString();
      if (!writeCache.exists(fileName)) {
        fileId = readCache.addFile(fileName, expectedFileId, writeCache);
      } else {
        fileId = writeCache.fileIdByName(fileName);
      }

      if (!writeCache.fileIdsAreEqual(expectedFileId, fileId)) {
        throw new OStorageException(
            "Can not restore database from backup because expected and actual file ids are not the"
                + " same");
      }

      while (true) {
        final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];

        int rb = 0;

        while (rb < data.length) {
          final int b = zipInputStream.read(data, rb, data.length - rb);

          if (b == -1) {
            if (rb > 0) {
              throw new OStorageException("Can not read data from file " + fileName);
            } else {
              processedFiles.add(fileName);
              continue entryLoop;
            }
          }

          rb += b;
        }

        final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(data, 0);

        if (aesKey != null) {
          doEncryptionDecryption(
              Cipher.DECRYPT_MODE, aesKey, expectedFileId, pageIndex, data, encryptionIv);
        }

        OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, writeCache, true, null);

        if (cacheEntry == null) {
          do {
            if (cacheEntry != null) {
              readCache.releaseFromWrite(cacheEntry, writeCache, true);
            }

            cacheEntry = readCache.allocateNewPage(fileId, writeCache, null);
          } while (cacheEntry.getPageIndex() != pageIndex);
        }

        try {
          final ByteBuffer buffer = cacheEntry.getCachePointer().getBuffer();
          assert buffer != null;
          final OLogSequenceNumber backedUpPageLsn =
              ODurablePage.getLogSequenceNumber(OLongSerializer.LONG_SIZE, data);
          if (isFull) {
            buffer.put(0, data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

            if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
              maxLsn = backedUpPageLsn;
            }
          } else {
            final OLogSequenceNumber currentPageLsn =
                ODurablePage.getLogSequenceNumberFromPage(buffer);
            if (backedUpPageLsn.compareTo(currentPageLsn) > 0) {
              buffer.put(
                  0, data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

              if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                maxLsn = backedUpPageLsn;
              }
            }
          }

        } finally {
          readCache.releaseFromWrite(cacheEntry, writeCache, true);
        }
      }
    }

    currentFiles.removeAll(processedFiles);

    for (String file : currentFiles) {
      if (writeCache.exists(file)) {
        final long fileId = writeCache.fileIdByName(file);
        readCache.deleteFile(fileId, writeCache);
      }
    }

    try (final OWriteAheadLog restoreLog =
        createWalFromIBUFiles(walTempDir, contextConfiguration, locale, walIv)) {
      if (restoreLog != null) {
        final OLogSequenceNumber beginLsn = restoreLog.begin();
        restoreFrom(restoreLog, beginLsn);
      }
    }

    if (maxLsn != null && writeAheadLog != null) {
      writeAheadLog.moveLsnAfter(maxLsn);
    }

    OFileUtils.deleteRecursively(walTempDir);
  }

  private byte[] restoreEncryptionIv(final ZipInputStream zipInputStream) throws IOException {
    final byte[] iv = new byte[16];
    int read = 0;
    while (read < iv.length) {
      final int localRead = zipInputStream.read(iv, read, iv.length - read);

      if (localRead < 0) {
        throw new OStorageException(
            "End of stream is reached but IV data were not completely read");
      }

      read += localRead;
    }

    return iv;
  }

  @Override
  public ORawBuffer readRecord(
      ORecordId iRid,
      boolean iIgnoreCache,
      boolean prefetchRecords,
      ORecordCallback<ORawBuffer> iCallback) {

    try {
      return super.readRecord(iRid, iIgnoreCache, prefetchRecords, iCallback);
    } finally {
      listeners.forEach(OEnterpriseStorageOperationListener::onRead);
    }
  }

  @Override
  public List<ORecordOperation> commit(OTransactionOptimistic clientTx, boolean allocated) {
    List<ORecordOperation> operations = super.commit(clientTx, allocated);
    listeners.forEach((l) -> l.onCommit(operations));
    return operations;
  }

  private void replaceConfiguration(ZipInputStream zipInputStream) throws IOException {
    byte[] buffer = new byte[1024];

    int rb = 0;
    while (true) {
      final int b = zipInputStream.read(buffer, rb, buffer.length - rb);

      if (b == -1) {
        break;
      }

      rb += b;

      if (rb == buffer.length) {
        byte[] oldBuffer = buffer;

        buffer = new byte[buffer.length << 1];
        System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length);
      }
    }
  }

  private static Cipher getCipherInstance() {
    try {
      return Cipher.getInstance(TRANSFORMATION);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw OException.wrapException(
          new OSecurityException("Implementation of encryption " + TRANSFORMATION + " is absent"),
          e);
    }
  }

  private void waitBackup() {
    backupLock.lock();
    try {
      while (isIcrementalBackupRunning()) {
        try {
          backupIsDone.await();
        } catch (InterruptedException e) {
          throw OException.wrapException(
              new OInterruptedException("Interrupted wait for backup to finish"), e);
        }
      }
    } finally {
      backupLock.unlock();
    }
  }

  @Override
  protected void checkBackupRunning() {
    waitBackup();
  }

  private record IncrementalRestorePreprocessingResult(
      Locale serverLocale,
      OContextConfiguration contextConfiguration,
      String charset,
      Locale locale) {}
}
