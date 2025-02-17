package com.jetbrains.youtrack.db.internal.core.db;

import static com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTxInternal.closeAllOnShutdown;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.exception.CommandScriptException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.TransactionException;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.RecordHook;
import com.jetbrains.youtrack.db.api.record.StatefulEdge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.api.session.SessionListener;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.cache.LocalRecordCache;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.conflict.RecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.db.record.CurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorClass;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Token;
import com.jetbrains.youtrack.db.internal.core.query.Query;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.shutdown.ShutdownHandler;
import com.jetbrains.youtrack.db.internal.core.storage.RecordMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.StorageInfo;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionNoTx.NonTxReadMode;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionOptimistic;
import com.jetbrains.youtrack.db.internal.core.util.URLHelper;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 *
 */
@Deprecated
public class DatabaseDocumentTx implements DatabaseSessionInternal {

  protected static ConcurrentMap<String, YouTrackDBInternal> embedded = new ConcurrentHashMap<>();
  protected static ConcurrentMap<String, YouTrackDBInternal> remote = new ConcurrentHashMap<>();

  protected static final Lock embeddedLock = new ReentrantLock();
  protected static final Lock remoteLock = new ReentrantLock();

  protected DatabaseSessionInternal internal;
  private final String url;
  private YouTrackDBInternal factory;
  private final String dbName;
  private final String baseUrl;
  private final Map<String, Object> preopenProperties = new HashMap<>();
  private final Map<ATTRIBUTES, Object> preopenAttributes = new HashMap<>();
  // TODO review for the case of browseListener before open.
  private final Set<SessionListener> preopenListener = new HashSet<>();
  private DatabaseSessionInternal databaseOwner;
  private Storage delegateStorage;
  private RecordConflictStrategy conflictStrategy;
  private RecordSerializer serializer;
  protected final AtomicReference<Thread> owner = new AtomicReference<Thread>();
  private final boolean ownerProtection;

  private static final ShutdownHandler shutdownHandler =
      new ShutdownHandler() {
        @Override
        public void shutdown() throws Exception {
          closeAllOnShutdown();
        }

        @Override
        public int getPriority() {
          return 1000;
        }
      };

  static {
    YouTrackDBEnginesManager.instance()
        .registerYouTrackDBStartupListener(
            () -> YouTrackDBEnginesManager.instance().addShutdownHandler(shutdownHandler));
    YouTrackDBEnginesManager.instance().addShutdownHandler(shutdownHandler);
  }

  public static void closeAll() {
    embeddedLock.lock();
    try {
      for (var factory : embedded.values()) {
        factory.close();
      }
      embedded.clear();
    } finally {
      embeddedLock.unlock();
    }

    remoteLock.lock();
    try {
      for (var factory : remote.values()) {
        factory.close();
      }
      remote.clear();
    } finally {
      remoteLock.unlock();
    }
  }

  protected static YouTrackDBInternal getOrCreateRemoteFactory(String baseUrl) {
    YouTrackDBInternal factory;

    remoteLock.lock();
    try {
      factory = remote.get(baseUrl);
      if (factory == null || !factory.isOpen()) {
        factory = YouTrackDBInternal.fromUrl("remote:" + baseUrl, null);
        remote.put(baseUrl, factory);
      }
    } finally {
      remoteLock.unlock();
    }
    return factory;
  }

  @Override
  public boolean exists(RID rid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean executeExists(RID rid) {
    return false;
  }

  protected static YouTrackDBInternal getOrCreateEmbeddedFactory(
      String baseUrl, YouTrackDBConfig config) {
    if (!baseUrl.endsWith("/")) {
      baseUrl += "/";
    }
    YouTrackDBInternal factory;
    embeddedLock.lock();
    try {
      factory = embedded.get(baseUrl);
      if (factory == null || !factory.isOpen()) {
        factory = YouTrackDBInternal.embedded(baseUrl, config);
        embedded.put(baseUrl, factory);
      }
    } finally {
      embeddedLock.unlock();
    }

    return factory;
  }

  /**
   * @Deprecated use {{@link YouTrackDB}} instead.
   */
  @Deprecated
  public DatabaseDocumentTx(String url) {
    this(url, true);
  }

  protected DatabaseDocumentTx(String url, boolean ownerProtection) {

    var connection = URLHelper.parse(url);
    this.url = connection.getUrl();
    baseUrl = connection.getPath();
    dbName = connection.getDbName();
    this.ownerProtection = ownerProtection;
  }

  public DatabaseDocumentTx(DatabaseSessionInternal ref, String baseUrl) {
    url = ref.getURL();
    this.baseUrl = baseUrl;
    dbName = ref.getDatabaseName();
    internal = ref;
    this.ownerProtection = true;
  }

  @Override
  public CurrentStorageComponentsFactory getStorageVersions() {
    if (internal == null) {
      return null;
    }
    return internal.getStorageVersions();
  }

  @Override
  public Entity newEmbededEntity(String className) {
    return internal.newEmbededEntity(className);
  }

  @Override
  public Entity newEmbededEntity(SchemaClass schemaClass) {
    return internal.newEmbededEntity(schemaClass);
  }

  @Override
  public Entity newEmbededEntity() {
    return internal.newEmbededEntity();
  }

  @Override
  public BTreeCollectionManager getSbTreeCollectionManager() {
    if (internal == null) {
      return null;
    }
    return internal.getSbTreeCollectionManager();
  }

  @Override
  public BinarySerializerFactory getSerializerFactory() {
    checkOpenness();
    return internal.getSerializerFactory();
  }

  @Override
  public RecordSerializer getSerializer() {
    if (internal == null) {
      if (serializer != null) {
        return serializer;
      }
      return RecordSerializerFactory.instance().getDefaultRecordSerializer();
    }
    return internal.getSerializer();
  }

  @Override
  public int begin(FrontendTransactionOptimistic tx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int assignAndCheckCluster(DBRecord record, String clusterName) {
    return internal.assignAndCheckCluster(record, clusterName);
  }

  @Override
  public void reloadUser() {
    checkOpenness();
    internal.reloadUser();
  }

  @Override
  public RecordHook.RESULT callbackHooks(RecordHook.TYPE type, Identifiable id) {
    checkOpenness();
    return internal.callbackHooks(type, id);
  }

  @Nonnull
  @Override
  public <RET extends RecordAbstract> RET executeReadRecord(RecordId rid) {
    checkOpenness();

    return internal.executeReadRecord(rid);
  }

  @Override
  public void setDefaultTransactionMode() {
    checkOpenness();
    internal.setDefaultTransactionMode();
  }

  @Override
  public MetadataInternal getMetadata() {
    checkOpenness();
    return internal.getMetadata();
  }

  @Override
  public void afterCommitOperations() {
    checkOpenness();
    internal.afterCommitOperations();
  }

  @Override
  public Object get(ATTRIBUTES_INTERNAL attribute) {
    return internal.get(attribute);
  }

  @Override
  public void set(ATTRIBUTES_INTERNAL attribute, Object value) {
    internal.set(attribute, value);
  }

  @Override
  public void setValidationEnabled(boolean value) {
    checkOpenness();
    internal.setValidationEnabled(value);
  }

  @Override
  public void registerHook(RecordHook iHookImpl) {
    checkOpenness();
    internal.registerHook(iHookImpl);
  }

  @Override
  public void registerHook(RecordHook iHookImpl, RecordHook.HOOK_POSITION iPosition) {
    checkOpenness();
    internal.registerHook(iHookImpl, iPosition);
  }

  @Override
  public Map<RecordHook, RecordHook.HOOK_POSITION> getHooks() {
    checkOpenness();
    return internal.getHooks();
  }

  @Override
  public void unregisterHook(RecordHook iHookImpl) {
    checkOpenness();
    internal.unregisterHook(iHookImpl);
  }

  @Override
  public boolean isMVCC() {
    return false;
  }

  @Override
  public Iterable<SessionListener> getListeners() {
    return internal.getListeners();
  }

  @Override
  public DatabaseSession setMVCC(boolean iValue) {
    return null;
  }

  @Override
  public RecordConflictStrategy getConflictStrategy() {
    return internal.getConflictStrategy();
  }

  @Override
  public DatabaseSession setConflictStrategy(String iStrategyName) {
    if (internal != null) {
      internal.setConflictStrategy(iStrategyName);
    } else {
      conflictStrategy = YouTrackDBEnginesManager.instance().getRecordConflictStrategy()
          .getStrategy(iStrategyName);
    }
    return this;
  }

  @Override
  public DatabaseSession setConflictStrategy(RecordConflictStrategy iResolver) {
    if (internal != null) {
      internal.setConflictStrategy(iResolver);
    } else {
      conflictStrategy = iResolver;
    }
    return this;
  }

  @Override
  public String incrementalBackup(Path path) {
    checkOpenness();
    return internal.incrementalBackup(path);
  }

  @Override
  public DatabaseDocumentTx copy() {
    checkOpenness();
    return new DatabaseDocumentTx(this.internal.copy(), this.baseUrl);
  }

  @Override
  public boolean assertIfNotActive() {
    return internal.assertIfNotActive();
  }

  protected void checkOpenness() {
    if (internal == null) {
      throw new DatabaseException(url, "Database '" + url + "' is closed");
    }
  }

  @Override
  public void callOnOpenListeners() {
    checkOpenness();
    internal.callOnOpenListeners();
  }

  @Override
  public void callOnCloseListeners() {
    checkOpenness();
    internal.callOnCloseListeners();
  }

  @Override
  @Deprecated
  public Storage getStorage() {
    if (internal == null) {
      return delegateStorage;
    }
    return internal.getStorage();
  }

  @Override
  public void setUser(SecurityUser user) {
    internal.setUser(user);
  }


  @Override
  public void resetInitialization() {
    if (internal != null) {
      internal.resetInitialization();
    }
  }

  @Override
  public DatabaseSessionInternal getDatabaseOwner() {
    var current = databaseOwner;

    while (current != null && current != this && current.getDatabaseOwner() != current) {
      current = current.getDatabaseOwner();
    }
    if (current == null) {
      return this;
    }
    return current;
  }

  @Override
  public DatabaseSessionInternal setDatabaseOwner(DatabaseSessionInternal iOwner) {
    databaseOwner = iOwner;
    if (internal != null) {
      internal.setDatabaseOwner(iOwner);
    }
    return this;
  }

  @Override
  public DatabaseSession getUnderlying() {
    return internal.getUnderlying();
  }

  @Override
  public void setInternal(ATTRIBUTES attribute, Object iValue) {
    checkOpenness();
    internal.setInternal(attribute, iValue);
  }

  @Override
  public DatabaseSession open(Token iToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SharedContext getSharedContext() {
    if (internal == null) {
      return null;
    }
    return internal.getSharedContext();
  }

  @Override
  public RecordIteratorClass<EntityImpl> browseClass(@Nonnull String className) {
    checkOpenness();
    return internal.browseClass(className);
  }

  @Override
  public RecordIteratorClass<EntityImpl> browseClass(@Nonnull String className,
      boolean iPolymorphic) {
    checkOpenness();
    return internal.browseClass(className, iPolymorphic);
  }

  @Override
  public void freeze() {
    checkOpenness();
    internal.freeze();
  }

  @Override
  public void release() {
    checkOpenness();
    internal.release();
  }

  @Override
  public void freeze(boolean throwException) {
    checkOpenness();
    internal.freeze(throwException);
  }

  public Vertex newVertex(final String iClassName) {
    checkOpenness();
    return internal.newVertex(iClassName);
  }

  @Override
  public Vertex newVertex(SchemaClass type) {
    checkOpenness();
    return internal.newVertex(type);
  }

  @Override
  public StatefulEdge newStatefulEdge(Vertex from, Vertex to, String type) {
    checkOpenness();
    return internal.newStatefulEdge(from, to, type);
  }

  @Override
  public Edge newLightweightEdge(Vertex from, Vertex to, @Nonnull String type) {
    checkOpenness();
    return internal.newLightweightEdge(from, to, type);
  }

  @Override
  public StatefulEdge newStatefulEdge(Vertex from, Vertex to, SchemaClass type) {
    checkOpenness();
    return internal.newStatefulEdge(from, to, type);
  }

  @Override
  public Edge newLightweightEdge(Vertex from, Vertex to, @Nonnull SchemaClass type) {
    checkOpenness();
    return internal.newLightweightEdge(from, to, type);
  }

  @Override
  public EntityImpl newEntity() {
    checkOpenness();
    return internal.newInstance();
  }

  @Override
  public EntityImpl newInternalInstance() {
    checkOpenness();
    return internal.newInternalInstance();
  }

  @Override
  public <T extends DBRecord> T fromJson(String json) {
    checkOpenness();
    return internal.fromJson(json);
  }

  @Override
  public Entity entityFromJson(String json) {
    checkOpenness();
    return internal.entityFromJson(json);
  }

  @Override
  public Entity newEntity(String className) {
    checkOpenness();
    return internal.newEntity(className);
  }

  @Override
  public Entity newEntity(SchemaClass cls) {
    return null;
  }

  @Override
  public EntityImpl newInstance() {
    checkOpenness();
    return internal.newInstance();
  }

  @Override
  public SecurityUser geCurrentUser() {
    if (internal != null) {
      return internal.geCurrentUser();
    }
    return null;
  }


  @Nonnull
  @Override
  public <RET extends DBRecord> RET load(RID recordId) {
    checkOpenness();
    return internal.load(recordId);
  }

  @Override
  public <RET extends DBRecord> RET save(DBRecord record) {
    checkOpenness();
    return internal.save(record);
  }

  @Override
  public <RET extends DBRecord> RET save(DBRecord iObject, String iClusterName) {
    checkOpenness();
    return internal.save(iObject, iClusterName);
  }

  @Override
  public void delete(@Nonnull DBRecord record) {
    checkOpenness();
    internal.delete(record);
  }

  @Override
  public DatabaseSessionInternal cleanOutRecord(RID rid, int version) {
    checkOpenness();
    internal.cleanOutRecord(rid, version);
    return this;
  }

  @Override
  public void startExclusiveMetadataChange() {
    checkOpenness();
    internal.startExclusiveMetadataChange();
  }

  @Override
  public void endExclusiveMetadataChange() {
    checkOpenness();
    internal.endExclusiveMetadataChange();
  }

  @Override
  public FrontendTransaction getTransaction() {
    checkOpenness();
    return internal.getTransaction();
  }

  @Override
  public int begin() {
    checkOpenness();
    return internal.begin();
  }

  @Override
  public boolean commit() throws TransactionException {
    checkOpenness();
    return internal.commit();
  }

  @Override
  public void rollback() throws TransactionException {
    checkOpenness();
    internal.rollback();
  }

  @Override
  public void rollback(boolean force) throws TransactionException {
    checkOpenness();
    internal.rollback(force);
  }

  @Override
  public <RET extends List<?>> RET query(Query<?> iCommand, Object... iArgs) {
    checkOpenness();
    return internal.query(iCommand, iArgs);
  }

  @Override
  public <RET extends CommandRequest> RET command(CommandRequest iCommand) {
    checkOpenness();
    return internal.command(iCommand);
  }

  @Override
  public RecordIteratorCluster<EntityImpl> browseCluster(String iClusterName) {
    checkOpenness();
    return internal.browseCluster(iClusterName);
  }

  @Override
  public RecordIteratorCluster<EntityImpl> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones) {
    checkOpenness();
    return internal.browseCluster(
        iClusterName, startClusterPosition, endClusterPosition, loadTombstones);
  }

  @Override
  public <REC extends DBRecord> RecordIteratorCluster<REC> browseCluster(
      String iClusterName, Class<REC> iRecordClass) {
    checkOpenness();
    return internal.browseCluster(iClusterName, iRecordClass);
  }

  @Override
  public <REC extends DBRecord> RecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition) {
    checkOpenness();
    return internal.browseCluster(
        iClusterName, iRecordClass, startClusterPosition, endClusterPosition);
  }

  @Override
  public <REC extends DBRecord> RecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones) {
    checkOpenness();
    return internal.browseCluster(
        iClusterName, iRecordClass, startClusterPosition, endClusterPosition, loadTombstones);
  }

  @Override
  public byte getRecordType() {
    checkOpenness();
    return internal.getRecordType();
  }

  @Override
  public boolean isRetainRecords() {
    checkOpenness();
    return internal.isRetainRecords();
  }

  @Override
  public DatabaseSession setRetainRecords(boolean iValue) {
    checkOpenness();
    return internal.setRetainRecords(iValue);
  }

  @Override
  public void checkSecurity(
      Rule.ResourceGeneric resourceGeneric, String resourceSpecific, int iOperation) {
    checkOpenness();
    internal.checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  public void checkSecurity(
      Rule.ResourceGeneric iResourceGeneric, int iOperation, Object iResourceSpecific) {
    checkOpenness();
    internal.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
  }

  @Override
  public void checkSecurity(int operation, Identifiable record, String cluster) {
    checkOpenness();
    internal.checkSecurity(operation, record, cluster);
  }

  @Override
  public void checkSecurity(
      Rule.ResourceGeneric iResourceGeneric, int iOperation, Object... iResourcesSpecific) {
    checkOpenness();
    internal.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
  }

  @Override
  public boolean isValidationEnabled() {
    checkOpenness();
    return internal.isValidationEnabled();
  }

  @Override
  public void checkSecurity(String iResource, int iOperation) {
    checkOpenness();
    internal.checkSecurity(iResource, iOperation);
  }

  @Override
  public void checkSecurity(String iResourceGeneric, int iOperation, Object iResourceSpecific) {
    checkOpenness();
    internal.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
  }

  @Override
  public void checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific) {
    checkOpenness();
    internal.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
  }

  @Override
  public boolean isPooled() {
    return false;
  }

  @Override
  public DatabaseSession open(String iUserName, String iUserPassword) {
    throw new UnsupportedOperationException();
  }

  protected void setupThreadOwner() {
    if (!ownerProtection) {
      return;
    }

    final var current = Thread.currentThread();
    final var o = owner.get();

    if (o != null || !owner.compareAndSet(null, current)) {
      throw new IllegalStateException(
          "Current instance is owned by other thread '" + (o != null ? o.getName() : "?") + "'");
    }
  }

  protected void clearOwner() {
    if (!ownerProtection) {
      return;
    }
    owner.set(null);
  }

  @Override
  public DatabaseSession create() {
    return create((Map<GlobalConfiguration, Object>) null);
  }

  @Override
  @Deprecated
  public DatabaseSession create(String incrementalBackupPath) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabaseSession create(Map<GlobalConfiguration, Object> iInitialSettings) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void activateOnCurrentThread() {
    if (internal != null) {
      internal.activateOnCurrentThread();
    }
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    if (internal != null) {
      return internal.isActiveOnCurrentThread();
    }
    return false;
  }

  @Override
  public void reload() {
    checkOpenness();
    internal.reload();
  }

  @Override
  public void drop() {
    checkOpenness();
    factory.drop(this.dbName, null, null);
    this.internal = null;
    clearOwner();
  }

  @Override
  public ContextConfiguration getConfiguration() {
    checkOpenness();
    return internal.getConfiguration();
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    clearOwner();

    if (internal != null) {
      delegateStorage = internal.getStorage();
      internal.close();
      internal = null;
    }
  }

  @Override
  public STATUS getStatus() {
    return internal.getStatus();
  }

  @Override
  public DatabaseSession setStatus(STATUS iStatus) {
    checkOpenness();
    internal.setStatus(iStatus);
    return this;
  }

  @Override
  public long getSize() {
    checkOpenness();
    return internal.getSize();
  }

  @Override
  public String getDatabaseName() {
    return dbName;
  }

  @Override
  public String getURL() {
    return url;
  }

  @Override
  public LocalRecordCache getLocalCache() {
    checkOpenness();
    return internal.getLocalCache();
  }

  @Override
  public int getClusters() {
    checkOpenness();
    return internal.getClusters();
  }

  @Override
  public boolean existsCluster(String iClusterName) {
    checkOpenness();
    return internal.existsCluster(iClusterName);
  }

  @Override
  public Collection<String> getClusterNames() {
    checkOpenness();
    return internal.getClusterNames();
  }

  @Override
  public int getClusterIdByName(String iClusterName) {
    checkOpenness();
    return internal.getClusterIdByName(iClusterName);
  }

  @Override
  public String getClusterNameById(int iClusterId) {
    checkOpenness();
    return internal.getClusterNameById(iClusterId);
  }

  @Override
  public long getClusterRecordSizeByName(String iClusterName) {
    checkOpenness();
    return internal.getClusterRecordSizeByName(iClusterName);
  }

  @Override
  public long getClusterRecordSizeById(int iClusterId) {
    checkOpenness();
    return internal.getClusterRecordSizeById(iClusterId);
  }

  @Override
  public boolean isClosed() {
    return internal == null || internal.isClosed();
  }

  @Override
  public void truncateCluster(String clusterName) {
    checkOpenness();
    internal.truncateCluster(clusterName);
  }

  @Override
  public long countClusterElements(int iCurrentClusterId) {
    checkOpenness();
    return internal.countClusterElements(iCurrentClusterId);
  }

  @Override
  public long countClusterElements(int iCurrentClusterId, boolean countTombstones) {
    checkOpenness();
    return internal.countClusterElements(iCurrentClusterId, countTombstones);
  }

  @Override
  public long countClusterElements(int[] iClusterIds) {
    checkOpenness();
    return internal.countClusterElements(iClusterIds);
  }

  @Override
  public long countClusterElements(int[] iClusterIds, boolean countTombstones) {
    checkOpenness();
    return internal.countClusterElements(iClusterIds, countTombstones);
  }

  @Override
  public long countClusterElements(String iClusterName) {
    checkOpenness();
    return internal.countClusterElements(iClusterName);
  }

  @Override
  public int addCluster(String iClusterName, Object... iParameters) {
    checkOpenness();
    return internal.addCluster(iClusterName, iParameters);
  }

  @Override
  public int addBlobCluster(String iClusterName, Object... iParameters) {
    checkOpenness();
    return internal.addBlobCluster(iClusterName, iParameters);
  }

  @Override
  public int[] getBlobClusterIds() {
    checkOpenness();
    return internal.getBlobClusterIds();
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId) {
    checkOpenness();
    return internal.addCluster(iClusterName, iRequestedId);
  }

  @Override
  public boolean dropCluster(String iClusterName) {
    checkOpenness();
    return internal.dropCluster(iClusterName);
  }

  @Override
  public boolean dropCluster(int iClusterId) {
    checkOpenness();
    return internal.dropCluster(iClusterId);
  }

  @Override
  public Object setProperty(String iName, Object iValue) {
    if (internal != null) {
      return internal.setProperty(iName, iValue);
    } else {
      return preopenProperties.put(iName, iValue);
    }
  }

  @Override
  public Object getProperty(String iName) {
    if (internal != null) {
      return internal.getProperty(iName);
    } else {
      return preopenProperties.get(iName);
    }
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    checkOpenness();
    return internal.getProperties();
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    if (internal != null) {
      return internal.get(iAttribute);
    } else {
      return preopenAttributes.get(iAttribute);
    }
  }

  @Override
  public void set(ATTRIBUTES iAttribute, Object iValue) {
    if (internal != null) {
      internal.set(iAttribute, iValue);
    } else {
      preopenAttributes.put(iAttribute, iValue);
    }
  }

  @Override
  public void registerListener(SessionListener iListener) {
    if (internal != null) {
      internal.registerListener(iListener);
    } else {
      preopenListener.add(iListener);
    }
  }

  @Override
  public void unregisterListener(SessionListener iListener) {
    checkOpenness();
    internal.unregisterListener(iListener);
  }

  @Override
  public RecordMetadata getRecordMetadata(RID rid) {
    checkOpenness();
    return internal.getRecordMetadata(rid);
  }

  @Override
  public EntityImpl newInstance(String className) {
    checkOpenness();
    return internal.newInstance(className);
  }

  @Override
  public Blob newBlob(byte[] bytes) {
    checkOpenness();
    return internal.newBlob(bytes);
  }

  @Override
  public Blob newBlob() {
    return new RecordBytes(this);
  }

  public EdgeInternal newLightweightEdgeInternal(String iClassName, Vertex from, Vertex to) {
    checkOpenness();
    return internal.newLightweightEdgeInternal(iClassName, from, to);
  }

  public Edge newRegularEdge(String iClassName, Vertex from, Vertex to) {
    checkOpenness();
    return internal.newRegularEdge(iClassName, from, to);
  }

  @Override
  public long countClass(String iClassName) {
    checkOpenness();
    return internal.countClass(iClassName);
  }

  @Override
  public long countClass(String iClassName, boolean iPolymorphic) {
    checkOpenness();
    return internal.countClass(iClassName, iPolymorphic);
  }

  public void setSerializer(RecordSerializer serializer) {
    if (internal != null) {
      internal.setSerializer(serializer);
    } else {
      this.serializer = serializer;
    }
  }

  @Override
  public ResultSet query(String query, Object... args) {
    checkOpenness();
    return internal.query(query, args);
  }

  @Override
  public ResultSet query(String query, Map args)
      throws CommandSQLParsingException, CommandExecutionException {
    checkOpenness();
    return internal.query(query, args);
  }

  @Override
  public ResultSet command(String query, Object... args)
      throws CommandSQLParsingException, CommandExecutionException {
    checkOpenness();
    return internal.command(query, args);
  }

  public ResultSet command(String query, Map args)
      throws CommandSQLParsingException, CommandExecutionException {
    checkOpenness();
    return internal.command(query, args);
  }

  @Override
  public DatabaseSession setCustom(String name, Object iValue) {
    return internal.setCustom(name, iValue);
  }

  @Override
  public boolean isPrefetchRecords() {
    checkOpenness();
    return internal.isPrefetchRecords();
  }

  public void setPrefetchRecords(boolean prefetchRecords) {
    checkOpenness();
    internal.setPrefetchRecords(prefetchRecords);
  }

  public void checkForClusterPermissions(String name) {
    checkOpenness();
    internal.checkForClusterPermissions(name);
  }

  @Override
  public ResultSet execute(String language, String script, Object... args)
      throws CommandExecutionException, CommandScriptException {
    checkOpenness();
    return internal.execute(language, script, args);
  }

  @Override
  public ResultSet execute(String language, String script, Map<String, ?> args)
      throws CommandExecutionException, CommandScriptException {
    checkOpenness();
    return internal.execute(language, script, args);
  }

  @Override
  public LiveQueryMonitor live(String query, LiveQueryResultListener listener, Object... args) {
    checkOpenness();
    return internal.live(query, listener, args);
  }

  @Override
  public LiveQueryMonitor live(
      String query, LiveQueryResultListener listener, Map<String, ?> args) {
    checkOpenness();
    return internal.live(query, listener, args);
  }

  @Override
  public void recycle(DBRecord record) {
    checkOpenness();
    internal.recycle(record);
  }

  @Override
  public void internalCommit(@Nonnull FrontendTransactionOptimistic transaction) {
    internal.internalCommit(transaction);
  }

  @Override
  public boolean isClusterVertex(int cluster) {
    checkOpenness();
    return internal.isClusterVertex(cluster);
  }

  @Override
  public boolean isClusterEdge(int cluster) {
    checkOpenness();
    return internal.isClusterEdge(cluster);
  }

  @Override
  public void deleteInternal(@Nonnull DBRecord record) {
    checkOpenness();
    internal.deleteInternal(record);
  }

  @Override
  public void beforeCreateOperations(Identifiable id, String iClusterName) {
    internal.beforeCreateOperations(id, iClusterName);
  }

  @Override
  public void beforeUpdateOperations(Identifiable id, String iClusterName) {
    internal.beforeUpdateOperations(id, iClusterName);
  }

  @Override
  public void beforeDeleteOperations(Identifiable id, String iClusterName) {
    internal.beforeDeleteOperations(id, iClusterName);
  }

  @Override
  public void afterCreateOperations(Identifiable id) {
    internal.afterCreateOperations(id);
  }

  @Override
  public void afterDeleteOperations(Identifiable id) {
    internal.afterDeleteOperations(id);
  }

  @Override
  public void afterUpdateOperations(Identifiable id) {
    internal.afterUpdateOperations(id);
  }

  @Override
  public void afterReadOperations(Identifiable identifiable) {
    internal.afterReadOperations(identifiable);
  }

  @Override
  public boolean beforeReadOperations(Identifiable identifiable) {
    return internal.beforeReadOperations(identifiable);
  }

  @Override
  public void internalClose(boolean recycle) {
    internal.internalClose(true);
  }

  @Override
  public String getClusterName(@Nonnull DBRecord record) {
    return internal.getClusterName(record);
  }

  @Override
  public Map<UUID, BonsaiCollectionPointer> getCollectionsChanges() {
    return internal.getCollectionsChanges();
  }

  @Override
  public boolean isRemote() {
    throw new UnsupportedOperationException();
  }

  @Override
  public StorageInfo getStorageInfo() {
    return internal.getStorageInfo();
  }

  @Override
  public boolean dropClusterInternal(int clusterId) {
    return internal.dropClusterInternal(clusterId);
  }

  @Override
  public long[] getClusterDataRange(int currentClusterId) {
    return internal.getClusterDataRange(currentClusterId);
  }

  @Override
  public long getLastClusterPosition(int clusterId) {
    return internal.getLastClusterPosition(clusterId);
  }

  @Override
  public String getClusterRecordConflictStrategy(int clusterId) {
    return internal.getClusterRecordConflictStrategy(clusterId);
  }

  @Override
  public int[] getClustersIds(@Nonnull Set<String> filterClusters) {
    return internal.getClustersIds(filterClusters);
  }

  @Override
  public void truncateClass(String name) {
    internal.truncateClass(name);
  }

  @Override
  public long truncateClusterInternal(String name) {
    return internal.truncateClusterInternal(name);
  }

  @Override
  public NonTxReadMode getNonTxReadMode() {
    return internal.getNonTxReadMode();
  }

  @Override
  public long truncateClass(String name, boolean polimorfic) {
    return internal.truncateClass(name, polimorfic);
  }

  @Override
  public void executeInTx(Runnable runnable) {
    internal.executeInTx(runnable);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterator<T> iterator, int batchSize, BiConsumer<DatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterator, batchSize, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterator<T> iterator, BiConsumer<DatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterator, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, int batchSize, BiConsumer<DatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterable, batchSize, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, BiConsumer<DatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterable, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Stream<T> stream, int batchSize, BiConsumer<DatabaseSession, T> consumer) {
    internal.executeInTxBatches(stream, batchSize, consumer);
  }

  @Override
  public <T> void executeInTxBatches(Stream<T> stream, BiConsumer<DatabaseSession, T> consumer) {
    internal.executeInTxBatches(stream, consumer);
  }

  @Override
  public <T> T computeInTx(Supplier<T> supplier) {
    return internal.computeInTx(supplier);
  }

  @Override
  public <T extends Identifiable> T bindToSession(T identifiable) {
    return internal.bindToSession(identifiable);
  }

  @Override
  public Schema getSchema() {
    return internal.getSchema();
  }

  @Override
  public int activeTxCount() {
    return internal.activeTxCount();
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator,
      BiFunction<DatabaseSession, T, Boolean> consumer) {
    internal.forEachInTx(iterator, consumer);
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable,
      BiFunction<DatabaseSession, T, Boolean> consumer) {
    internal.forEachInTx(iterable, consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream,
      BiFunction<DatabaseSession, T, Boolean> consumer) {
    internal.forEachInTx(stream, consumer);
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator, BiConsumer<DatabaseSession, T> consumer) {
    internal.forEachInTx(iterator, consumer);
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable, BiConsumer<DatabaseSession, T> consumer) {
    internal.forEachInTx(iterable, consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream, BiConsumer<DatabaseSession, T> consumer) {
    internal.forEachInTx(stream, consumer);
  }
}
