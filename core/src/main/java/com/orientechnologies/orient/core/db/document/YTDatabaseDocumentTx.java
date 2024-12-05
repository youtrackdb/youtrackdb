package com.orientechnologies.orient.core.db.document;

import static com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal.closeAllOnShutdown;

import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.YTCommandScriptException;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.db.YouTrackDBInternal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.exception.YTTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTView;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTEdgeInternal;
import com.orientechnologies.orient.core.record.impl.YTRecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.shutdown.OShutdownHandler;
import com.orientechnologies.orient.core.sql.YTCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionNoTx.NonTxReadMode;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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
public class YTDatabaseDocumentTx implements YTDatabaseSessionInternal {

  protected static ConcurrentMap<String, YouTrackDBInternal> embedded = new ConcurrentHashMap<>();
  protected static ConcurrentMap<String, YouTrackDBInternal> remote = new ConcurrentHashMap<>();

  protected static final Lock embeddedLock = new ReentrantLock();
  protected static final Lock remoteLock = new ReentrantLock();

  protected YTDatabaseSessionInternal internal;
  private final String url;
  private YouTrackDBInternal factory;
  private final String type;
  private final String dbName;
  private final String baseUrl;
  private final Map<String, Object> preopenProperties = new HashMap<>();
  private final Map<ATTRIBUTES, Object> preopenAttributes = new HashMap<>();
  // TODO review for the case of browseListener before open.
  private final Set<ODatabaseListener> preopenListener = new HashSet<>();
  private YTDatabaseSessionInternal databaseOwner;
  private OStorage delegateStorage;
  private ORecordConflictStrategy conflictStrategy;
  private ORecordSerializer serializer;
  protected final AtomicReference<Thread> owner = new AtomicReference<Thread>();
  private final boolean ownerProtection;

  private static final OShutdownHandler shutdownHandler =
      new OShutdownHandler() {
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
    YouTrackDBManager.instance()
        .registerOrientStartupListener(
            () -> YouTrackDBManager.instance().addShutdownHandler(shutdownHandler));
    YouTrackDBManager.instance().addShutdownHandler(shutdownHandler);
  }

  public static void closeAll() {
    embeddedLock.lock();
    try {
      for (YouTrackDBInternal factory : embedded.values()) {
        factory.close();
      }
      embedded.clear();
    } finally {
      embeddedLock.unlock();
    }

    remoteLock.lock();
    try {
      for (YouTrackDBInternal factory : remote.values()) {
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
  public boolean exists(YTRID rid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean executeExists(YTRID rid) {
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
        try {
          factory = YouTrackDBInternal.distributed(baseUrl, config);
        } catch (YTDatabaseException ignore) {
          factory = YouTrackDBInternal.embedded(baseUrl, config);
        }
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
  public YTDatabaseDocumentTx(String url) {
    this(url, true);
  }

  protected YTDatabaseDocumentTx(String url, boolean ownerProtection) {

    OURLConnection connection = OURLHelper.parse(url);
    this.url = connection.getUrl();
    type = connection.getType();
    baseUrl = connection.getPath();
    dbName = connection.getDbName();
    this.ownerProtection = ownerProtection;
  }

  public YTDatabaseDocumentTx(YTDatabaseSessionInternal ref, String baseUrl) {
    url = ref.getURL();
    type = ref.getType();
    this.baseUrl = baseUrl;
    dbName = ref.getName();
    internal = ref;
    this.ownerProtection = true;
  }

  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    if (internal == null) {
      return null;
    }
    return internal.getStorageVersions();
  }

  @Override
  public OSBTreeCollectionManager getSbTreeCollectionManager() {
    if (internal == null) {
      return null;
    }
    return internal.getSbTreeCollectionManager();
  }

  @Override
  public OBinarySerializerFactory getSerializerFactory() {
    checkOpenness();
    return internal.getSerializerFactory();
  }

  @Override
  public ORecordSerializer getSerializer() {
    if (internal == null) {
      if (serializer != null) {
        return serializer;
      }
      return ORecordSerializerFactory.instance().getDefaultRecordSerializer();
    }
    return internal.getSerializer();
  }

  @Override
  public int begin(OTransactionOptimistic tx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int assignAndCheckCluster(YTRecord record, String iClusterName) {
    return internal.assignAndCheckCluster(record, iClusterName);
  }

  @Override
  public void reloadUser() {
    checkOpenness();
    internal.reloadUser();
  }

  @Override
  public ORecordHook.RESULT callbackHooks(ORecordHook.TYPE type, YTIdentifiable id) {
    checkOpenness();
    return internal.callbackHooks(type, id);
  }

  @Nonnull
  @Override
  public <RET extends YTRecordAbstract> RET executeReadRecord(YTRecordId rid) {
    checkOpenness();

    return internal.executeReadRecord(rid);
  }

  @Override
  public void setDefaultTransactionMode() {
    checkOpenness();
    internal.setDefaultTransactionMode();
  }

  @Override
  public OMetadataInternal getMetadata() {
    checkOpenness();
    return internal.getMetadata();
  }

  @Override
  public void afterCommitOperations() {
    checkOpenness();
    internal.afterCommitOperations();
  }

  @Override
  public void registerHook(ORecordHook iHookImpl) {
    checkOpenness();
    internal.registerHook(iHookImpl);
  }

  @Override
  public void registerHook(ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition) {
    checkOpenness();
    internal.registerHook(iHookImpl, iPosition);
  }

  @Override
  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    checkOpenness();
    return internal.getHooks();
  }

  @Override
  public void unregisterHook(ORecordHook iHookImpl) {
    checkOpenness();
    internal.unregisterHook(iHookImpl);
  }

  @Override
  public boolean isMVCC() {
    return false;
  }

  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return internal.getListeners();
  }

  @Override
  public YTDatabaseSession setMVCC(boolean iValue) {
    return null;
  }

  @Override
  public String getType() {
    return this.type;
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return internal.getConflictStrategy();
  }

  @Override
  public YTDatabaseSession setConflictStrategy(String iStrategyName) {
    if (internal != null) {
      internal.setConflictStrategy(iStrategyName);
    } else {
      conflictStrategy = YouTrackDBManager.instance().getRecordConflictStrategy()
          .getStrategy(iStrategyName);
    }
    return this;
  }

  @Override
  public YTDatabaseSession setConflictStrategy(ORecordConflictStrategy iResolver) {
    if (internal != null) {
      internal.setConflictStrategy(iResolver);
    } else {
      conflictStrategy = iResolver;
    }
    return this;
  }

  @Override
  public String incrementalBackup(String path) {
    checkOpenness();
    return internal.incrementalBackup(path);
  }

  @Override
  public YTDatabaseDocumentTx copy() {
    checkOpenness();
    return new YTDatabaseDocumentTx(this.internal.copy(), this.baseUrl);
  }

  @Override
  public void checkIfActive() {
    internal.checkIfActive();
  }

  @Override
  public boolean assertIfNotActive() {
    return internal.assertIfNotActive();
  }

  protected void checkOpenness() {
    if (internal == null) {
      throw new YTDatabaseException("Database '" + url + "' is closed");
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
  public OStorage getStorage() {
    if (internal == null) {
      return delegateStorage;
    }
    return internal.getStorage();
  }

  @Override
  public void setUser(OSecurityUser user) {
    internal.setUser(user);
  }

  @Override
  public void replaceStorage(OStorage iNewStorage) {
    internal.replaceStorage(iNewStorage);
  }

  @Override
  public void resetInitialization() {
    if (internal != null) {
      internal.resetInitialization();
    }
  }

  @Override
  public YTDatabaseSessionInternal getDatabaseOwner() {
    YTDatabaseSessionInternal current = databaseOwner;

    while (current != null && current != this && current.getDatabaseOwner() != current) {
      current = current.getDatabaseOwner();
    }
    if (current == null) {
      return this;
    }
    return current;
  }

  @Override
  public YTDatabaseSessionInternal setDatabaseOwner(YTDatabaseSessionInternal iOwner) {
    databaseOwner = iOwner;
    if (internal != null) {
      internal.setDatabaseOwner(iOwner);
    }
    return this;
  }

  @Override
  public YTDatabaseSession getUnderlying() {
    return internal.getUnderlying();
  }

  @Override
  public void setInternal(ATTRIBUTES attribute, Object iValue) {
    checkOpenness();
    internal.setInternal(attribute, iValue);
  }

  @Override
  public YTDatabaseSession open(OToken iToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSharedContext getSharedContext() {
    if (internal == null) {
      return null;
    }
    return internal.getSharedContext();
  }

  @Override
  public ORecordIteratorClass<YTDocument> browseClass(String iClassName) {
    checkOpenness();
    return internal.browseClass(iClassName);
  }

  @Override
  public ORecordIteratorClass<YTDocument> browseClass(String iClassName, boolean iPolymorphic) {
    checkOpenness();
    return internal.browseClass(iClassName, iPolymorphic);
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

  public YTVertex newVertex(final String iClassName) {
    checkOpenness();
    return internal.newVertex(iClassName);
  }

  @Override
  public YTVertex newVertex(YTClass type) {
    checkOpenness();
    return internal.newVertex(type);
  }

  @Override
  public YTEdgeInternal newEdge(YTVertex from, YTVertex to, String type) {
    checkOpenness();
    return internal.newEdge(from, to, type);
  }

  @Override
  public YTEdge newEdge(YTVertex from, YTVertex to, YTClass type) {
    checkOpenness();
    return internal.newEdge(from, to, type);
  }

  @Override
  public YTEntity newElement() {
    checkOpenness();
    return internal.newInstance();
  }

  @Override
  public YTEdgeInternal addLightweightEdge(YTVertex from, YTVertex to, String className) {
    checkOpenness();

    return internal.addLightweightEdge(from, to, className);
  }

  @Override
  public YTEntity newElement(String className) {
    checkOpenness();
    return internal.newElement(className);
  }

  public void setUseLightweightEdges(boolean b) {
    internal.setUseLightweightEdges(b);
  }

  @Override
  public YTDocument newInstance() {
    checkOpenness();
    return internal.newInstance();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public ODictionary<YTRecord> getDictionary() {
    checkOpenness();
    return internal.getDictionary();
  }

  @Override
  public OSecurityUser getUser() {
    if (internal != null) {
      return internal.getUser();
    }
    return null;
  }


  @Nonnull
  @Override
  public <RET extends YTRecord> RET load(YTRID recordId) {
    checkOpenness();
    return internal.load(recordId);
  }

  @Override
  public <RET extends YTRecord> RET save(YTRecord record) {
    checkOpenness();
    return internal.save(record);
  }

  @Override
  public <RET extends YTRecord> RET save(YTRecord iObject, String iClusterName) {
    checkOpenness();
    return internal.save(iObject, iClusterName);
  }

  @Override
  public void delete(YTRecord record) {
    checkOpenness();
    internal.delete(record);
  }

  @Override
  public void delete(YTRID iRID) {
    checkOpenness();
    internal.delete(iRID);
  }

  @Override
  public YTDatabaseSessionInternal cleanOutRecord(YTRID rid, int version) {
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
  public OTransaction getTransaction() {
    checkOpenness();
    return internal.getTransaction();
  }

  @Override
  public int begin() {
    checkOpenness();
    return internal.begin();
  }

  @Override
  public boolean commit() throws YTTransactionException {
    checkOpenness();
    return internal.commit();
  }

  @Override
  public void rollback() throws YTTransactionException {
    checkOpenness();
    internal.rollback();
  }

  @Override
  public void rollback(boolean force) throws YTTransactionException {
    checkOpenness();
    internal.rollback(force);
  }

  @Override
  public <RET extends List<?>> RET query(OQuery<?> iCommand, Object... iArgs) {
    checkOpenness();
    return internal.query(iCommand, iArgs);
  }

  @Override
  public <RET extends OCommandRequest> RET command(OCommandRequest iCommand) {
    checkOpenness();
    return internal.command(iCommand);
  }

  @Override
  public ORecordIteratorCluster<YTDocument> browseCluster(String iClusterName) {
    checkOpenness();
    return internal.browseCluster(iClusterName);
  }

  @Override
  public ORecordIteratorCluster<YTDocument> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones) {
    checkOpenness();
    return internal.browseCluster(
        iClusterName, startClusterPosition, endClusterPosition, loadTombstones);
  }

  @Override
  public <REC extends YTRecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName, Class<REC> iRecordClass) {
    checkOpenness();
    return internal.browseCluster(iClusterName, iRecordClass);
  }

  @Override
  public <REC extends YTRecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition) {
    checkOpenness();
    return internal.browseCluster(
        iClusterName, iRecordClass, startClusterPosition, endClusterPosition);
  }

  @Override
  public <REC extends YTRecord> ORecordIteratorCluster<REC> browseCluster(
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
  public YTDatabaseSession setRetainRecords(boolean iValue) {
    checkOpenness();
    return internal.setRetainRecords(iValue);
  }

  @Override
  public void checkSecurity(
      ORule.ResourceGeneric resourceGeneric, String resourceSpecific, int iOperation) {
    checkOpenness();
    internal.checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  public void checkSecurity(
      ORule.ResourceGeneric iResourceGeneric, int iOperation, Object iResourceSpecific) {
    checkOpenness();
    internal.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
  }

  @Override
  public void checkSecurity(
      ORule.ResourceGeneric iResourceGeneric, int iOperation, Object... iResourcesSpecific) {
    checkOpenness();
    internal.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
  }

  @Override
  public boolean isValidationEnabled() {
    checkOpenness();
    return internal.isValidationEnabled();
  }

  @Override
  public YTDatabaseSession setValidationEnabled(boolean iEnabled) {
    checkOpenness();
    internal.setValidationEnabled(iEnabled);
    return this;
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
  public YTDatabaseSession open(String iUserName, String iUserPassword) {
    setupThreadOwner();
    try {
      if ("remote".equals(type)) {
        factory = getOrCreateRemoteFactory(baseUrl);
        YouTrackDBConfig config = buildConfig(null);
        internal = factory.open(dbName, iUserName, iUserPassword, config);

      } else {
        factory = getOrCreateEmbeddedFactory(baseUrl, null);
        YouTrackDBConfig config = buildConfig(null);
        internal = factory.open(dbName, iUserName, iUserPassword, config);
      }
      if (databaseOwner != null) {
        internal.setDatabaseOwner(databaseOwner);
      }
      if (conflictStrategy != null) {
        internal.setConflictStrategy(conflictStrategy);
      }
      if (serializer != null) {
        internal.setSerializer(serializer);
      }
      for (Entry<String, Object> pro : preopenProperties.entrySet()) {
        internal.setProperty(pro.getKey(), pro.getValue());
      }
    } catch (RuntimeException e) {
      clearOwner();
      throw e;
    }
    return this;
  }

  protected void setupThreadOwner() {
    if (!ownerProtection) {
      return;
    }

    final Thread current = Thread.currentThread();
    final Thread o = owner.get();

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
  public YTDatabaseSession create() {
    return create((Map<YTGlobalConfiguration, Object>) null);
  }

  @Override
  @Deprecated
  public YTDatabaseSession create(String incrementalBackupPath) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTDatabaseSession create(Map<YTGlobalConfiguration, Object> iInitialSettings) {
    setupThreadOwner();
    try {
      YouTrackDBConfig config = buildConfig(iInitialSettings);
      if ("remote".equals(type)) {
        throw new UnsupportedOperationException();
      } else if ("memory".equals(type)) {
        factory = getOrCreateEmbeddedFactory(baseUrl, null);
        factory.create(dbName, null, null, ODatabaseType.MEMORY, config);
        YouTrackDBConfig openConfig =
            YouTrackDBConfig.builder().fromContext(config.getConfigurations()).build();
        internal = factory.open(dbName, "admin", "admin", openConfig);
        for (Map.Entry<ATTRIBUTES, Object> attr : preopenAttributes.entrySet()) {
          internal.set(attr.getKey(), attr.getValue());
        }

        for (ODatabaseListener oDatabaseListener : preopenListener) {
          internal.registerListener(oDatabaseListener);
        }

      } else {
        factory = getOrCreateEmbeddedFactory(baseUrl, null);
        factory.create(dbName, null, null, ODatabaseType.PLOCAL, config);
        YouTrackDBConfig openConfig =
            YouTrackDBConfig.builder().fromContext(config.getConfigurations()).build();
        internal = factory.open(dbName, "admin", "admin", openConfig);
        for (Map.Entry<ATTRIBUTES, Object> attr : preopenAttributes.entrySet()) {
          internal.set(attr.getKey(), attr.getValue());
        }

        for (ODatabaseListener oDatabaseListener : preopenListener) {
          internal.registerListener(oDatabaseListener);
        }
      }
      if (databaseOwner != null) {
        internal.setDatabaseOwner(databaseOwner);
      }
      if (conflictStrategy != null) {
        internal.setConflictStrategy(conflictStrategy);
      }
      if (serializer != null) {
        internal.setSerializer(serializer);
      }
      for (Entry<String, Object> pro : preopenProperties.entrySet()) {
        internal.setProperty(pro.getKey(), pro.getValue());
      }
    } catch (RuntimeException e) {
      clearOwner();
      throw e;
    }
    return this;
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
    internal.callOnDropListeners();
    ODatabaseRecordThreadLocal.instance().remove();
    factory.drop(this.dbName, null, null);
    this.internal = null;
    clearOwner();
  }

  @Override
  public OContextConfiguration getConfiguration() {
    checkOpenness();
    return internal.getConfiguration();
  }

  @Override
  public boolean exists() {
    if (internal != null) {
      return true;
    }
    if ("remote".equals(type)) {
      throw new UnsupportedOperationException();
    } else {
      factory = getOrCreateEmbeddedFactory(baseUrl, null);
      return factory.exists(dbName, null, null);
    }
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
  public YTDatabaseSession setStatus(STATUS iStatus) {
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
  public String getName() {
    return dbName;
  }

  @Override
  public String getURL() {
    return url;
  }

  @Override
  public OLocalRecordCache getLocalCache() {
    checkOpenness();
    return internal.getLocalCache();
  }

  @Override
  public int getDefaultClusterId() {
    checkOpenness();
    return internal.getDefaultClusterId();
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
  public IntSet getBlobClusterIds() {
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
  public void registerListener(ODatabaseListener iListener) {
    if (internal != null) {
      internal.registerListener(iListener);
    } else {
      preopenListener.add(iListener);
    }
  }

  @Override
  public void unregisterListener(ODatabaseListener iListener) {
    checkOpenness();
    internal.unregisterListener(iListener);
  }

  @Override
  public ORecordMetadata getRecordMetadata(YTRID rid) {
    checkOpenness();
    return internal.getRecordMetadata(rid);
  }

  @Override
  public YTEntity newInstance(String iClassName) {
    checkOpenness();
    return internal.newInstance(iClassName);
  }

  @Override
  public YTBlob newBlob(byte[] bytes) {
    checkOpenness();
    return internal.newBlob(bytes);
  }

  @Override
  public YTBlob newBlob() {
    return new YTRecordBytes();
  }

  public YTEdgeInternal newLightweightEdge(String iClassName, YTVertex from, YTVertex to) {
    checkOpenness();
    return internal.newLightweightEdge(iClassName, from, to);
  }

  public YTEdge newRegularEdge(String iClassName, YTVertex from, YTVertex to) {
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

  @Override
  public long countView(String viewName) {
    return internal.countView(viewName);
  }

  public void setSerializer(ORecordSerializer serializer) {
    if (internal != null) {
      internal.setSerializer(serializer);
    } else {
      this.serializer = serializer;
    }
  }

  @Override
  public YTResultSet query(String query, Object... args) {
    checkOpenness();
    return internal.query(query, args);
  }

  @Override
  public YTResultSet query(String query, Map args)
      throws YTCommandSQLParsingException, YTCommandExecutionException {
    checkOpenness();
    return internal.query(query, args);
  }

  private YouTrackDBConfig buildConfig(final Map<YTGlobalConfiguration, Object> iProperties) {
    Map<String, Object> pars = new HashMap<>(preopenProperties);
    if (iProperties != null) {
      for (Map.Entry<YTGlobalConfiguration, Object> par : iProperties.entrySet()) {
        pars.put(par.getKey().getKey(), par.getValue());
      }
    }
    YouTrackDBConfigBuilder builder = YouTrackDBConfig.builder();
    final String connectionStrategy =
        pars != null
            ? (String) pars.get(YTGlobalConfiguration.CLIENT_CONNECTION_STRATEGY.getKey())
            : null;
    if (connectionStrategy != null) {
      builder.addConfig(YTGlobalConfiguration.CLIENT_CONNECTION_STRATEGY, connectionStrategy);
    }

    final String compressionMethod =
        pars != null
            ? (String) pars.get(YTGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey())
            : null;
    if (compressionMethod != null)
    // SAVE COMPRESSION METHOD IN CONFIGURATION
    {
      builder.addConfig(YTGlobalConfiguration.STORAGE_COMPRESSION_METHOD, compressionMethod);
    }

    final String encryptionKey =
        pars != null
            ? (String) pars.get(YTGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey())
            : null;
    if (encryptionKey != null)
    // SAVE ENCRYPTION KEY IN CONFIGURATION
    {
      builder.addConfig(YTGlobalConfiguration.STORAGE_ENCRYPTION_KEY, encryptionKey);
    }

    for (Map.Entry<ATTRIBUTES, Object> attr : preopenAttributes.entrySet()) {
      builder.addAttribute(attr.getKey(), attr.getValue());
    }
    builder.addConfig(YTGlobalConfiguration.CREATE_DEFAULT_USERS, true);

    for (ODatabaseListener oDatabaseListener : preopenListener) {
      builder.addListener(oDatabaseListener);
    }

    return builder.build();
  }

  @Override
  public YTResultSet command(String query, Object... args)
      throws YTCommandSQLParsingException, YTCommandExecutionException {
    checkOpenness();
    return internal.command(query, args);
  }

  public YTResultSet command(String query, Map args)
      throws YTCommandSQLParsingException, YTCommandExecutionException {
    checkOpenness();
    return internal.command(query, args);
  }

  @Override
  public YTDatabaseSession setCustom(String name, Object iValue) {
    return internal.setCustom(name, iValue);
  }

  @Override
  public void callOnDropListeners() {
    checkOpenness();
    internal.callOnDropListeners();
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
  public YTResultSet execute(String language, String script, Object... args)
      throws YTCommandExecutionException, YTCommandScriptException {
    checkOpenness();
    return internal.execute(language, script, args);
  }

  @Override
  public YTResultSet execute(String language, String script, Map<String, ?> args)
      throws YTCommandExecutionException, YTCommandScriptException {
    checkOpenness();
    return internal.execute(language, script, args);
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args) {
    checkOpenness();
    return internal.live(query, listener, args);
  }

  @Override
  public OLiveQueryMonitor live(
      String query, OLiveQueryResultListener listener, Map<String, ?> args) {
    checkOpenness();
    return internal.live(query, listener, args);
  }

  @Override
  public void recycle(YTRecord record) {
    checkOpenness();
    internal.recycle(record);
  }

  @Override
  public void internalCommit(OTransactionOptimistic transaction) {
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
  public boolean isClusterView(int cluster) {
    return internal.isClusterView(cluster);
  }

  @Override
  public YTIdentifiable beforeCreateOperations(YTIdentifiable id, String iClusterName) {
    return internal.beforeCreateOperations(id, iClusterName);
  }

  @Override
  public YTIdentifiable beforeUpdateOperations(YTIdentifiable id, String iClusterName) {
    return internal.beforeUpdateOperations(id, iClusterName);
  }

  @Override
  public void beforeDeleteOperations(YTIdentifiable id, String iClusterName) {
    internal.beforeDeleteOperations(id, iClusterName);
  }

  @Override
  public void afterCreateOperations(YTIdentifiable id) {
    internal.afterCreateOperations(id);
  }

  @Override
  public void afterDeleteOperations(YTIdentifiable id) {
    internal.afterDeleteOperations(id);
  }

  @Override
  public void afterUpdateOperations(YTIdentifiable id) {
    internal.afterUpdateOperations(id);
  }

  @Override
  public void afterReadOperations(YTIdentifiable identifiable) {
    internal.afterReadOperations(identifiable);
  }

  @Override
  public boolean beforeReadOperations(YTIdentifiable identifiable) {
    return internal.beforeReadOperations(identifiable);
  }

  @Override
  public void internalClose(boolean recycle) {
    internal.internalClose(true);
  }

  @Override
  public String getClusterName(YTRecord record) {
    return internal.getClusterName(record);
  }

  @Override
  public YTView getViewFromCluster(int cluster) {
    return internal.getViewFromCluster(cluster);
  }

  @Override
  public <T> T sendSequenceAction(OSequenceAction action)
      throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Map<UUID, OBonsaiCollectionPointer> getCollectionsChanges() {
    return internal.getCollectionsChanges();
  }

  @Override
  public boolean isRemote() {
    if (internal == null) {
      return "remote".equals(type);
    }
    return internal.isRemote();
  }

  @Override
  public OStorageInfo getStorageInfo() {
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
  public void setDefaultClusterId(int addCluster) {
    internal.setDefaultClusterId(addCluster);
  }

  @Override
  public String getClusterRecordConflictStrategy(int clusterId) {
    return internal.getClusterRecordConflictStrategy(clusterId);
  }

  @Override
  public int[] getClustersIds(Set<String> filterClusters) {
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
      Iterator<T> iterator, int batchSize, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterator, batchSize, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterator<T> iterator, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterator, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, int batchSize, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterable, batchSize, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.executeInTxBatches(iterable, consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Stream<T> stream, int batchSize, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.executeInTxBatches(stream, batchSize, consumer);
  }

  @Override
  public <T> void executeInTxBatches(Stream<T> stream, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.executeInTxBatches(stream, consumer);
  }

  @Override
  public <T> T computeInTx(Supplier<T> supplier) {
    return internal.computeInTx(supplier);
  }

  @Override
  public <T extends YTIdentifiable> T bindToSession(T identifiable) {
    return internal.bindToSession(identifiable);
  }

  @Override
  public YTSchema getSchema() {
    return internal.getSchema();
  }

  @Override
  public int activeTxCount() {
    return internal.activeTxCount();
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator,
      BiFunction<YTDatabaseSession, T, Boolean> consumer) {
    internal.forEachInTx(iterator, consumer);
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable,
      BiFunction<YTDatabaseSession, T, Boolean> consumer) {
    internal.forEachInTx(iterable, consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream,
      BiFunction<YTDatabaseSession, T, Boolean> consumer) {
    internal.forEachInTx(stream, consumer);
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.forEachInTx(iterator, consumer);
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.forEachInTx(iterable, consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream, BiConsumer<YTDatabaseSession, T> consumer) {
    internal.forEachInTx(stream, consumer);
  }
}
