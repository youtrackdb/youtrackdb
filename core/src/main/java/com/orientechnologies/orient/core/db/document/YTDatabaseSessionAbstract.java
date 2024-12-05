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

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.concur.YTNeedRetryException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.exception.YTHighLevelException;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.YTContextConfiguration;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.YTDatabaseListener;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.YTConcurrentModificationException;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.exception.YTSecurityException;
import com.orientechnologies.orient.core.exception.YTSessionNotActivatedException;
import com.orientechnologies.orient.core.exception.YTTransactionBlockedException;
import com.orientechnologies.orient.core.exception.YTTransactionException;
import com.orientechnologies.orient.core.exception.YTValidationException;
import com.orientechnologies.orient.core.hook.YTRecordHook;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableView;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTView;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.YTImmutableUser;
import com.orientechnologies.orient.core.metadata.security.YTSecurityUser;
import com.orientechnologies.orient.core.metadata.security.YTUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTEdgeDelegate;
import com.orientechnologies.orient.core.record.impl.YTEdgeDocument;
import com.orientechnologies.orient.core.record.impl.YTEdgeInternal;
import com.orientechnologies.orient.core.record.impl.YTRecordBytes;
import com.orientechnologies.orient.core.record.impl.YTVertexDocument;
import com.orientechnologies.orient.core.record.impl.YTVertexInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.cluster.YTOfflineClusterException;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.YTRollbackException;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Document API entrypoint.
 */
@SuppressWarnings("unchecked")
public abstract class YTDatabaseSessionAbstract extends OListenerManger<YTDatabaseListener>
    implements YTDatabaseSessionInternal {

  protected final Map<String, Object> properties = new HashMap<String, Object>();
  protected Map<YTRecordHook, YTRecordHook.HOOK_POSITION> unmodifiableHooks;
  protected final Set<YTIdentifiable> inHook = new HashSet<YTIdentifiable>();
  protected ORecordSerializer serializer;
  protected String url;
  protected STATUS status;
  protected YTDatabaseSessionInternal databaseOwner;
  protected OMetadataDefault metadata;
  protected YTImmutableUser user;
  protected static final byte recordType = YTDocument.RECORD_TYPE;
  protected final Map<YTRecordHook, YTRecordHook.HOOK_POSITION> hooks = new LinkedHashMap<>();
  protected boolean retainRecords = true;
  protected OLocalRecordCache localCache;
  protected OCurrentStorageComponentsFactory componentsFactory;
  protected boolean initialized = false;
  protected OTransactionAbstract currentTx;

  protected final YTRecordHook[][] hooksByScope =
      new YTRecordHook[YTRecordHook.SCOPE.values().length][];
  protected OSharedContext sharedContext;

  private boolean prefetchRecords;

  protected Map<String, OQueryDatabaseState> activeQueries = new ConcurrentHashMap<>();
  protected LinkedList<OQueryDatabaseState> queryState = new LinkedList<>();
  private Map<UUID, OBonsaiCollectionPointer> collectionsChanges;

  // database stats!
  protected long loadedRecordsCount;
  protected long totalRecordLoadMs;
  protected long minRecordLoadMs;
  protected long maxRecordLoadMs;
  protected long ridbagPrefetchCount;
  protected long totalRidbagPrefetchMs;
  protected long minRidbagPrefetchMs;
  protected long maxRidbagPrefetchMs;

  protected YTDatabaseSessionAbstract() {
    // DO NOTHING IS FOR EXTENDED OBJECTS
    super(false);
  }

  /**
   * @return default serializer which is used to serialize documents. Default serializer is common
   * for all database instances.
   */
  public static ORecordSerializer getDefaultSerializer() {
    return ORecordSerializerFactory.instance().getDefaultRecordSerializer();
  }

  /**
   * Sets default serializer. The default serializer is common for all database instances.
   *
   * @param iDefaultSerializer new default serializer value
   */
  public static void setDefaultSerializer(ORecordSerializer iDefaultSerializer) {
    ORecordSerializerFactory.instance().setDefaultRecordSerializer(iDefaultSerializer);
  }

  public void callOnOpenListeners() {
    wakeupOnOpenDbLifecycleListeners();
    wakeupOnOpenListeners();
  }

  protected abstract void loadMetadata();

  public void callOnCloseListeners() {
    wakeupOnCloseDbLifecycleListeners();
    wakeupOnCloseListeners();
  }

  private void wakeupOnOpenDbLifecycleListeners() {
    for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
        .getDbLifecycleListeners();
        it.hasNext(); ) {
      it.next().onOpen(getDatabaseOwner());
    }
  }

  private void wakeupOnOpenListeners() {
    for (YTDatabaseListener listener : getListenersCopy()) {
      try {
        //noinspection deprecation
        listener.onOpen(getDatabaseOwner());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  private void wakeupOnCloseDbLifecycleListeners() {
    for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
        .getDbLifecycleListeners();
        it.hasNext(); ) {
      it.next().onClose(getDatabaseOwner());
    }
  }

  private void wakeupOnCloseListeners() {
    for (YTDatabaseListener listener : getListenersCopy()) {
      try {
        listener.onClose(getDatabaseOwner());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  public void callOnDropListeners() {
    wakeupOnDropListeners();
  }

  private void wakeupOnDropListeners() {
    for (YTDatabaseListener listener : getListenersCopy()) {
      try {
        activateOnCurrentThread();
        //noinspection deprecation
        listener.onDelete(getDatabaseOwner());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends YTRecord> RET getRecord(final YTIdentifiable iIdentifiable) {
    if (iIdentifiable instanceof YTRecord) {
      return (RET) iIdentifiable;
    }
    return load(iIdentifiable.getIdentity());
  }

  /**
   * Deletes the record checking the version.
   */
  private void delete(final YTRID iRecord, final int iVersion) {
    final YTRecord record = load(iRecord);
    ORecordInternal.setVersion(record, iVersion);
    delete(record);
  }

  public YTDatabaseSessionInternal cleanOutRecord(final YTRID iRecord, final int iVersion) {
    delete(iRecord, iVersion);
    return this;
  }

  public String getType() {
    return TYPE;
  }

  public <REC extends YTRecord> ORecordIteratorCluster<REC> browseCluster(
      final String iClusterName, final Class<REC> iClass) {
    return (ORecordIteratorCluster<REC>) browseCluster(iClusterName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public <REC extends YTRecord> ORecordIteratorCluster<REC> browseCluster(
      final String iClusterName,
      final Class<REC> iRecordClass,
      final long startClusterPosition,
      final long endClusterPosition,
      final boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();
    final int clusterId = getClusterIdByName(iClusterName);
    return new ORecordIteratorCluster<REC>(
        this, clusterId, startClusterPosition, endClusterPosition);
  }

  @Override
  public <REC extends YTRecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();
    final int clusterId = getClusterIdByName(iClusterName);
    //noinspection deprecation
    return new ORecordIteratorCluster<>(this, clusterId, startClusterPosition, endClusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  public OCommandRequest command(final OCommandRequest iCommand) {
    checkSecurity(ORule.ResourceGeneric.COMMAND, ORole.PERMISSION_READ);
    checkIfActive();
    final OCommandRequestInternal command = (OCommandRequestInternal) iCommand;
    try {
      command.reset();
      return command;
    } catch (Exception e) {
      throw YTException.wrapException(new YTDatabaseException("Error on command execution"), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs) {
    checkIfActive();
    iCommand.reset();
    return iCommand.execute(this, iArgs);
  }

  /**
   * {@inheritDoc}
   */
  public byte getRecordType() {
    return recordType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int[] iClusterIds) {
    return countClusterElements(iClusterIds, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int iClusterId) {
    return countClusterElements(iClusterId, false);
  }

  /**
   * {@inheritDoc}
   */
  public OMetadataDefault getMetadata() {
    checkOpenness();
    return metadata;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTDatabaseSessionInternal getDatabaseOwner() {
    YTDatabaseSessionInternal current = databaseOwner;
    while (current != null && current != this && current.getDatabaseOwner() != current) {
      current = current.getDatabaseOwner();
    }
    return current;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTDatabaseSessionInternal setDatabaseOwner(YTDatabaseSessionInternal iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isRetainRecords() {
    return retainRecords;
  }

  /**
   * {@inheritDoc}
   */
  public YTDatabaseSession setRetainRecords(boolean retainRecords) {
    this.retainRecords = retainRecords;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public YTDatabaseSession setStatus(final STATUS status) {
    checkIfActive();
    this.status = status;
    return this;
  }

  public void setStatusInternal(final STATUS status) {
    this.status = status;
  }

  /**
   * {@inheritDoc}
   */
  public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
    set(iAttribute, iValue);
  }

  /**
   * {@inheritDoc}
   */
  public YTSecurityUser getUser() {
    return user;
  }

  /**
   * {@inheritDoc}
   */
  public void setUser(final YTSecurityUser user) {
    checkIfActive();
    if (user instanceof YTUser) {
      final OMetadata metadata = getMetadata();
      if (metadata != null) {
        final OSecurityInternal security = sharedContext.getSecurity();
        this.user = new YTImmutableUser(this, security.getVersion(this), user);
      } else {
        this.user = new YTImmutableUser(this, -1, user);
      }
    } else {
      this.user = (YTImmutableUser) user;
    }
  }

  public void reloadUser() {
    if (user != null) {
      activateOnCurrentThread();
      if (user.checkIfAllowed(this, ORule.ResourceGeneric.CLASS, YTUser.CLASS_NAME,
          ORole.PERMISSION_READ)
          != null) {
        OMetadata metadata = getMetadata();
        if (metadata != null) {
          final OSecurityInternal security = sharedContext.getSecurity();
          final YTUser secGetUser = security.getUser(this, user.getName(this));

          if (secGetUser != null) {
            user = new YTImmutableUser(this, security.getVersion(this), secGetUser);
          } else {
            user = new YTImmutableUser(this, -1, new YTUser());
          }
        } else {
          user = new YTImmutableUser(this, -1, new YTUser());
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isMVCC() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  public YTDatabaseSession setMVCC(boolean mvcc) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  public ODictionary<YTRecord> getDictionary() {
    checkOpenness();
    return metadata.getIndexManagerInternal().getDictionary(this);
  }

  /**
   * {@inheritDoc}
   */
  public void registerHook(final YTRecordHook iHookImpl,
      final YTRecordHook.HOOK_POSITION iPosition) {
    checkOpenness();
    checkIfActive();

    final Map<YTRecordHook, YTRecordHook.HOOK_POSITION> tmp =
        new LinkedHashMap<YTRecordHook, YTRecordHook.HOOK_POSITION>(hooks);
    tmp.put(iHookImpl, iPosition);
    hooks.clear();
    for (YTRecordHook.HOOK_POSITION p : YTRecordHook.HOOK_POSITION.values()) {
      for (Map.Entry<YTRecordHook, YTRecordHook.HOOK_POSITION> e : tmp.entrySet()) {
        if (e.getValue() == p) {
          hooks.put(e.getKey(), e.getValue());
        }
      }
    }
    compileHooks();
  }

  /**
   * {@inheritDoc}
   */
  public void registerHook(final YTRecordHook iHookImpl) {
    registerHook(iHookImpl, YTRecordHook.HOOK_POSITION.REGULAR);
  }

  /**
   * {@inheritDoc}
   */
  public void unregisterHook(final YTRecordHook iHookImpl) {
    checkIfActive();
    if (iHookImpl != null) {
      iHookImpl.onUnregister();
      hooks.remove(iHookImpl);
      compileHooks();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLocalRecordCache getLocalCache() {
    return localCache;
  }

  /**
   * {@inheritDoc}
   */
  public Map<YTRecordHook, YTRecordHook.HOOK_POSITION> getHooks() {
    return unmodifiableHooks;
  }

  /**
   * Callback the registered hooks if any.
   *
   * @param type Hook type. Define when hook is called.
   * @param id   Record received in the callback
   * @return True if the input record is changed, otherwise false
   */
  public YTRecordHook.RESULT callbackHooks(final YTRecordHook.TYPE type, final YTIdentifiable id) {
    if (id == null || hooks.isEmpty() || id.getIdentity().getClusterId() == 0) {
      return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
    }

    final YTRecordHook.SCOPE scope = YTRecordHook.SCOPE.typeToScope(type);
    final int scopeOrdinal = scope.ordinal();

    final YTRID identity = id.getIdentity().copy();
    if (!pushInHook(identity)) {
      return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
    }

    try {
      final YTRecord rec;
      try {
        rec = id.getRecord();
      } catch (YTRecordNotFoundException e) {
        return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
      }

      final OScenarioThreadLocal.RUN_MODE runMode = OScenarioThreadLocal.INSTANCE.getRunMode();

      boolean recordChanged = false;
      for (YTRecordHook hook : hooksByScope[scopeOrdinal]) {
        switch (runMode) {
          case DEFAULT: // NON_DISTRIBUTED OR PROXIED DB
            if (isDistributed()
                && hook.getDistributedExecutionMode()
                == YTRecordHook.DISTRIBUTED_EXECUTION_MODE.TARGET_NODE)
            // SKIP
            {
              continue;
            }
            break; // TARGET NODE
          case RUNNING_DISTRIBUTED:
            if (hook.getDistributedExecutionMode()
                == YTRecordHook.DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE) {
              continue;
            }
        }

        final YTRecordHook.RESULT res = hook.onTrigger(type, rec);

        if (res == YTRecordHook.RESULT.RECORD_CHANGED) {
          recordChanged = true;
        } else {
          if (res == YTRecordHook.RESULT.SKIP_IO)
          // SKIP IO OPERATION
          {
            return res;
          } else {
            if (res == YTRecordHook.RESULT.SKIP)
            // SKIP NEXT HOOKS AND RETURN IT
            {
              return res;
            } else {
              if (res == YTRecordHook.RESULT.RECORD_REPLACED) {
                return res;
              }
            }
          }
        }
      }
      return recordChanged
          ? YTRecordHook.RESULT.RECORD_CHANGED
          : YTRecordHook.RESULT.RECORD_NOT_CHANGED;
    } finally {
      popInHook(identity);
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isValidationEnabled() {
    return (Boolean) get(ATTRIBUTES.VALIDATION);
  }

  /**
   * {@inheritDoc}
   */
  public YTDatabaseSession setValidationEnabled(final boolean iEnabled) {
    set(ATTRIBUTES.VALIDATION, iEnabled);
    return this;
  }

  @Override
  public YTContextConfiguration getConfiguration() {
    checkIfActive();
    if (getStorageInfo() != null) {
      return getStorageInfo().getConfiguration().getContextConfiguration();
    }
    return null;
  }

  @Override
  public void close() {
    internalClose(false);
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public String getName() {
    return getStorageInfo() != null ? getStorageInfo().getName() : url;
  }

  @Override
  public String getURL() {
    return url != null ? url : getStorageInfo().getURL();
  }

  @Override
  public int getDefaultClusterId() {
    checkIfActive();
    return getStorageInfo().getDefaultClusterId();
  }

  @Override
  public int getClusters() {
    checkIfActive();
    return getStorageInfo().getClusters();
  }

  @Override
  public boolean existsCluster(final String iClusterName) {
    checkIfActive();
    return getStorageInfo().getClusterNames().contains(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Collection<String> getClusterNames() {
    checkIfActive();
    return getStorageInfo().getClusterNames();
  }

  @Override
  public int getClusterIdByName(final String iClusterName) {
    if (iClusterName == null) {
      return -1;
    }

    checkIfActive();
    return getStorageInfo().getClusterIdByName(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public String getClusterNameById(final int iClusterId) {
    if (iClusterId < 0) {
      return null;
    }

    checkIfActive();
    return getStorageInfo().getPhysicalClusterNameById(iClusterId);
  }

  public void checkForClusterPermissions(final String iClusterName) {
    // CHECK FOR ORESTRICTED
    final Set<YTClass> classes =
        getMetadata().getImmutableSchemaSnapshot().getClassesRelyOnCluster(iClusterName);
    for (YTClass c : classes) {
      if (c.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
        throw new YTSecurityException(
            "Class '"
                + c.getName()
                + "' cannot be truncated because has record level security enabled (extends '"
                + OSecurityShared.RESTRICTED_CLASSNAME
                + "')");
      }
    }
  }

  @Override
  public Object setProperty(final String iName, final Object iValue) {
    if (iValue == null) {
      return properties.remove(iName.toLowerCase(Locale.ENGLISH));
    } else {
      return properties.put(iName.toLowerCase(Locale.ENGLISH), iValue);
    }
  }

  @Override
  public Object getProperty(final String iName) {
    return properties.get(iName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    return properties.entrySet().iterator();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    checkIfActive();

    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }
    final OStorageInfo storage = getStorageInfo();
    return switch (iAttribute) {
      case STATUS -> getStatus();
      case DEFAULTCLUSTERID -> getDefaultClusterId();
      case TYPE ->
          getMetadata().getImmutableSchemaSnapshot().existsClass("V") ? "graph" : "document";
      case DATEFORMAT -> storage.getConfiguration().getDateFormat();
      case DATETIMEFORMAT -> storage.getConfiguration().getDateTimeFormat();
      case TIMEZONE -> storage.getConfiguration().getTimeZone().getID();
      case LOCALECOUNTRY -> storage.getConfiguration().getLocaleCountry();
      case LOCALELANGUAGE -> storage.getConfiguration().getLocaleLanguage();
      case CHARSET -> storage.getConfiguration().getCharset();
      case CUSTOM -> storage.getConfiguration().getProperties();
      case CLUSTERSELECTION -> storage.getConfiguration().getClusterSelection();
      case MINIMUMCLUSTERS -> storage.getConfiguration().getMinimumClusters();
      case CONFLICTSTRATEGY -> storage.getConfiguration().getConflictStrategy();
      case VALIDATION -> storage.getConfiguration().isValidationEnabled();
    };
  }

  public OTransaction getTransaction() {
    checkIfActive();
    return currentTx;
  }

  /**
   * Returns the schema of the database.
   *
   * @return the schema of the database
   */
  @Override
  public YTSchema getSchema() {
    return getMetadata().getSchema();
  }


  @Nonnull
  @SuppressWarnings("unchecked")
  @Override
  public <RET extends YTRecord> RET load(final YTRID recordId) {
    checkIfActive();
    return (RET) currentTx.loadRecord(recordId);
  }

  @Override
  public boolean exists(YTRID rid) {
    checkIfActive();
    return currentTx.exists(rid);
  }

  /**
   * Deletes the record without checking the version.
   */
  public void delete(final YTRID iRecord) {
    checkOpenness();
    checkIfActive();

    final YTRecord rec = load(iRecord);
    delete(rec);
  }

  @Override
  public OBinarySerializerFactory getSerializerFactory() {
    return componentsFactory.binarySerializerFactory;
  }

  @Override
  public void setPrefetchRecords(boolean prefetchRecords) {
    this.prefetchRecords = prefetchRecords;
  }

  @Override
  public boolean isPrefetchRecords() {
    return prefetchRecords;
  }

  @Override
  public <T extends YTIdentifiable> T bindToSession(T identifiable) {
    if (!(identifiable instanceof YTRecord record)) {
      return identifiable;
    }

    if (identifiable instanceof YTEdge edge && edge.isLightweight()) {
      return (T) edge;
    }

    var rid = record.getIdentity();
    if (rid == null) {
      throw new YTDatabaseException(
          "Cannot bind record to session with not persisted rid: " + rid);
    }

    checkOpenness();
    checkIfActive();

    // unwrap the record if wrapper is passed
    record = record.getRecord();

    var txRecord = currentTx.getRecord(rid);
    if (txRecord == record) {
      assert !txRecord.isUnloaded();
      assert txRecord.getSession() == this;
      return (T) record;
    }

    var cachedRecord = localCache.findRecord(rid);
    if (cachedRecord == record) {
      assert !cachedRecord.isUnloaded();
      assert cachedRecord.getSession() == this;
      return (T) record;
    }

    if (!rid.isPersistent()) {
      throw new YTDatabaseException(
          "Cannot bind record to session with not persisted rid: " + rid);
    }

    var result = executeReadRecord((YTRecordId) rid);

    assert !result.isUnloaded();
    assert result.getSession() == this;

    return (T) result;
  }

  @Nonnull
  public final <RET extends YTRecordAbstract> RET executeReadRecord(final YTRecordId rid) {
    checkOpenness();
    checkIfActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      checkSecurity(
          ORule.ResourceGeneric.CLUSTER,
          ORole.PERMISSION_READ,
          getClusterNameById(rid.getClusterId()));

      // SEARCH IN LOCAL TX
      var record = getTransaction().getRecord(rid);
      if (record == OTransactionAbstract.DELETED_RECORD) {
        // DELETED IN TX
        throw new YTRecordNotFoundException(rid);
      }

      var cachedRecord = localCache.findRecord(rid);
      if (record == null) {
        record = cachedRecord;
      }

      if (record != null && !record.isUnloaded()) {
        if (beforeReadOperations(record)) {
          throw new YTRecordNotFoundException(rid);
        }

        afterReadOperations(record);
        if (record instanceof YTDocument) {
          ODocumentInternal.checkClass((YTDocument) record, this);
        }

        localCache.updateRecord(record);

        assert !record.isUnloaded();
        assert record.getSession() == this;

        return (RET) record;
      }

      if (cachedRecord != null) {
        if (cachedRecord.isDirty()) {
          throw new IllegalStateException("Cached record is dirty");
        }

        record = cachedRecord;
      }

      loadedRecordsCount++;
      final ORawBuffer recordBuffer;
      if (!rid.isValid()) {
        recordBuffer = null;
      } else {
        recordBuffer = getStorage().readRecord(this, rid, false, prefetchRecords, null);
      }

      if (recordBuffer == null) {
        throw new YTRecordNotFoundException(rid);
      }

      if (record == null) {
        record =
            YouTrackDBManager.instance()
                .getRecordFactoryManager()
                .newInstance(recordBuffer.recordType, rid, this);
        ORecordInternal.unsetDirty(record);
      }

      if (ORecordInternal.getRecordType(record) != recordBuffer.recordType) {
        throw new YTDatabaseException("Record type is different from the one in the database");
      }

      ORecordInternal.setRecordSerializer(record, serializer);
      ORecordInternal.fill(record, rid, recordBuffer.version, recordBuffer.buffer, false, this);

      if (record instanceof YTDocument) {
        ODocumentInternal.checkClass((YTDocument) record, this);
      }

      if (beforeReadOperations(record)) {
        throw new YTRecordNotFoundException(rid);
      }

      ORecordInternal.fromStream(record, recordBuffer.buffer, this);
      afterReadOperations(record);

      localCache.updateRecord(record);

      assert !record.isUnloaded();
      assert record.getSession() == this;

      return (RET) record;
    } catch (YTOfflineClusterException | YTRecordNotFoundException t) {
      throw t;
    } catch (Exception t) {
      if (rid.isTemporary()) {
        throw YTException.wrapException(
            new YTDatabaseException("Error on retrieving record using temporary RID: " + rid), t);
      } else {
        throw YTException.wrapException(
            new YTDatabaseException(
                "Error on retrieving record "
                    + rid
                    + " (cluster: "
                    + getStorage().getPhysicalClusterNameById(rid.getClusterId())
                    + ")"),
            t);
      }
    } finally {
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public int assignAndCheckCluster(YTRecord record, String iClusterName) {
    YTRecordId rid = (YTRecordId) record.getIdentity();
    // if provided a cluster name use it.
    if (rid.getClusterId() <= YTRID.CLUSTER_POS_INVALID && iClusterName != null) {
      rid.setClusterId(getClusterIdByName(iClusterName));
      if (rid.getClusterId() == -1) {
        throw new IllegalArgumentException("Cluster name '" + iClusterName + "' is not configured");
      }
    }
    YTClass schemaClass = null;
    // if cluster id is not set yet try to find it out
    if (rid.getClusterId() <= YTRID.CLUSTER_ID_INVALID
        && getStorageInfo().isAssigningClusterIds()) {
      if (record instanceof YTDocument) {
        schemaClass = ODocumentInternal.getImmutableSchemaClass(this, ((YTDocument) record));
        if (schemaClass != null) {
          if (schemaClass.isAbstract()) {
            throw new YTSchemaException(
                "Document belongs to abstract class "
                    + schemaClass.getName()
                    + " and cannot be saved");
          }
          rid.setClusterId(schemaClass.getClusterForNewInstance((YTDocument) record));
        } else {
          var defaultCluster = getStorageInfo().getDefaultClusterId();
          if (defaultCluster < 0) {
            throw new YTDatabaseException(
                "Cannot save (1) document " + record + ": no class or cluster defined");
          }
          rid.setClusterId(defaultCluster);
        }
      } else {
        if (record instanceof YTRecordBytes) {
          IntSet blobs = getBlobClusterIds();
          if (blobs.isEmpty()) {
            rid.setClusterId(getDefaultClusterId());
          } else {
            rid.setClusterId(blobs.iterator().nextInt());
          }
        } else {
          throw new YTDatabaseException(
              "Cannot save (3) document " + record + ": no class or cluster defined");
        }
      }
    } else {
      if (record instanceof YTDocument) {
        schemaClass = ODocumentInternal.getImmutableSchemaClass(this, ((YTDocument) record));
      }
    }
    // If the cluster id was set check is validity
    if (rid.getClusterId() > YTRID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        String messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '"
                  + messageClusterName
                  + "' (id="
                  + rid.getClusterId()
                  + ") is not configured to store the class '"
                  + schemaClass.getName()
                  + "', valid are "
                  + Arrays.toString(schemaClass.getClusterIds()));
        }
      }
    }
    return rid.getClusterId();
  }

  public int begin() {
    assert assertIfNotActive();

    if (currentTx.isActive()) {
      return currentTx.begin();
    }

    return begin(newTxInstance());
  }

  public int begin(OTransactionOptimistic transaction) {
    checkOpenness();
    checkIfActive();

    // CHECK IT'S NOT INSIDE A HOOK
    if (!inHook.isEmpty()) {
      throw new IllegalStateException("Cannot begin a transaction while a hook is executing");
    }

    if (currentTx.isActive()) {
      if (currentTx instanceof OTransactionOptimistic) {
        return currentTx.begin();
      }
    }

    // WAKE UP LISTENERS
    for (YTDatabaseListener listener : browseListeners()) {
      try {
        listener.onBeforeTxBegin(this);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error before tx begin", e);
      }
    }

    currentTx = transaction;

    return currentTx.begin();
  }

  protected OTransactionOptimistic newTxInstance() {
    return new OTransactionOptimistic(this);
  }

  public void setDefaultTransactionMode() {
    if (!(currentTx instanceof OTransactionNoTx)) {
      currentTx = new OTransactionNoTx(this);
    }
  }

  /**
   * Creates a new YTDocument.
   */
  public YTDocument newInstance() {
    return new YTDocument(YTEntity.DEFAULT_CLASS_NAME, this);
  }

  @Override
  public YTBlob newBlob(byte[] bytes) {
    return new YTRecordBytes(this, bytes);
  }

  @Override
  public YTBlob newBlob() {
    return new YTRecordBytes(this);
  }

  /**
   * Creates a document with specific class.
   *
   * @param iClassName the name of class that should be used as a class of created document.
   * @return new instance of document.
   */
  @Override
  public YTDocument newInstance(final String iClassName) {
    return new YTDocument(this, iClassName);
  }

  @Override
  public YTEntity newElement() {
    return newInstance();
  }

  @Override
  public YTEntity newElement(String className) {
    return newInstance(className);
  }

  public YTEntity newElement(YTClass clazz) {
    return newInstance(clazz.getName());
  }

  public YTVertex newVertex(final String iClassName) {
    return new YTVertexDocument(this, iClassName);
  }

  private YTEdgeInternal newEdgeInternal(final String iClassName) {
    return new YTEdgeDocument(this, iClassName);
  }

  @Override
  public YTVertex newVertex(YTClass type) {
    if (type == null) {
      return newVertex("V");
    }
    return newVertex(type.getName());
  }

  @Override
  public YTEdgeInternal newEdge(YTVertex from, YTVertex to, String type) {
    YTClass cl = getMetadata().getImmutableSchemaSnapshot().getClass(type);
    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException(type + " is not an edge class");
    }

    return addEdgeInternal(from, to, type, true);
  }

  @Override
  public YTEdgeInternal addLightweightEdge(YTVertex from, YTVertex to, String className) {
    YTClass cl = getMetadata().getImmutableSchemaSnapshot().getClass(className);
    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException(className + " is not an edge class");
    }

    return addEdgeInternal(from, to, className, false);
  }

  @Override
  public YTEdge newEdge(YTVertex from, YTVertex to, YTClass type) {
    if (type == null) {
      return newEdge(from, to, "E");
    }
    return newEdge(from, to, type.getName());
  }

  private YTEdgeInternal addEdgeInternal(
      final YTVertex toVertex,
      final YTVertex inVertex,
      String className,
      boolean isRegular) {
    Objects.requireNonNull(toVertex, "From vertex is null");
    Objects.requireNonNull(inVertex, "To vertex is null");

    YTEdgeInternal edge;
    YTDocument outDocument;
    YTDocument inDocument;

    boolean outDocumentModified = false;
    if (checkDeletedInTx(toVertex)) {
      throw new YTRecordNotFoundException(
          toVertex.getIdentity(),
          "The vertex " + toVertex.getIdentity() + " has been deleted");
    }

    if (checkDeletedInTx(inVertex)) {
      throw new YTRecordNotFoundException(
          inVertex.getIdentity(), "The vertex " + inVertex.getIdentity() + " has been deleted");
    }

    try {
      outDocument = toVertex.getRecord();
    } catch (YTRecordNotFoundException e) {
      throw new IllegalArgumentException(
          "source vertex is invalid (rid=" + toVertex.getIdentity() + ")");
    }

    try {
      inDocument = inVertex.getRecord();
    } catch (YTRecordNotFoundException e) {
      throw new IllegalArgumentException(
          "source vertex is invalid (rid=" + inVertex.getIdentity() + ")");
    }

    YTSchema schema = getMetadata().getImmutableSchemaSnapshot();
    final YTClass edgeType = schema.getClass(className);

    if (edgeType == null) {
      throw new IllegalArgumentException("Class " + className + " does not exist");
    }

    className = edgeType.getName();

    var createLightweightEdge =
        !isRegular
            && (edgeType.isAbstract() || className.equals(YTEdgeInternal.CLASS_NAME));
    if (!isRegular && !createLightweightEdge) {
      throw new IllegalArgumentException(
          "Cannot create lightweight edge for class " + className + " because it is not abstract");
    }

    final String outFieldName = YTVertex.getEdgeLinkFieldName(ODirection.OUT, className);
    final String inFieldName = YTVertex.getEdgeLinkFieldName(ODirection.IN, className);

    if (createLightweightEdge) {
      edge = newLightweightEdge(className, toVertex, inVertex);
      YTVertexInternal.createLink(toVertex.getRecord(), inVertex.getRecord(), outFieldName);
      YTVertexInternal.createLink(inVertex.getRecord(), toVertex.getRecord(), inFieldName);
    } else {
      edge = newEdgeInternal(className);
      edge.setPropertyInternal(YTEdgeInternal.DIRECTION_OUT, toVertex.getRecord());
      edge.setPropertyInternal(YTEdge.DIRECTION_IN, inDocument.getRecord());

      if (!outDocumentModified) {
        // OUT-VERTEX ---> IN-VERTEX/EDGE
        YTVertexInternal.createLink(outDocument, edge.getRecord(), outFieldName);
      }

      // IN-VERTEX ---> OUT-VERTEX/EDGE
      YTVertexInternal.createLink(inDocument, edge.getRecord(), inFieldName);
    }
    // OK

    return edge;
  }

  private boolean checkDeletedInTx(YTVertex currentVertex) {
    YTRID id;
    if (!currentVertex.getRecord().exists()) {
      id = currentVertex.getRecord().getIdentity();
    } else {
      return false;
    }

    final ORecordOperation oper = getTransaction().getRecordEntry(id);
    if (oper == null) {
      return id.isTemporary();
    } else {
      return oper.type == ORecordOperation.DELETED;
    }
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<YTDocument> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<YTDocument> browseClass(
      final String iClassName, final boolean iPolymorphic) {
    if (getMetadata().getImmutableSchemaSnapshot().getClass(iClassName) == null) {
      throw new IllegalArgumentException(
          "Class '" + iClassName + "' not found in current database");
    }

    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iClassName);
    return new ORecordIteratorClass<YTDocument>(this, iClassName, iPolymorphic, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ORecordIteratorCluster<YTRecord> browseCluster(final String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<>(this, getClusterIdByName(iClusterName));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<YTDatabaseListener> getListeners() {
    return getListenersCopy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public ORecordIteratorCluster<YTDocument> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<YTDocument>(
        this, getClusterIdByName(iClusterName), startClusterPosition, endClusterPosition);
  }

  /**
   * Saves a document to the database. Behavior depends on the current running transaction if any.
   * If no transaction is running then changes apply immediately. If an Optimistic transaction is
   * running then the record will be changed at commit time. The current transaction will continue
   * to see the record as modified, while others not. If a Pessimistic transaction is running, then
   * an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the document is different by the version stored in
   * the database, then a {@link YTConcurrentModificationException} exception is thrown.Before to
   * save the document it must be valid following the constraints declared in the schema if any (can
   * work also in schema-less mode). To validate the document the {@link YTDocument#validate()} is
   * called.
   *
   * @param record Record to save.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   * @throws YTConcurrentModificationException if the version of the document is different by the
   *                                          version contained in the database.
   * @throws YTValidationException             if the document breaks some validation constraints
   *                                          defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends YTRecord> RET save(final YTRecord record) {
    return save(record, null);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the
   * current running transaction if any. If no transaction is running then changes apply
   * immediately. If an Optimistic transaction is running then the record will be changed at commit
   * time. The current transaction will continue to see the record as modified, while others not. If
   * a Pessimistic transaction is running, then an exclusive lock is acquired against the record.
   * Current transaction will continue to see the record as modified, while others cannot access to
   * it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the document is different by the version stored in
   * the database, then a {@link YTConcurrentModificationException} exception is thrown. Before to
   * save the document it must be valid following the constraints declared in the schema if any (can
   * work also in schema-less mode). To validate the document the {@link YTDocument#validate()} is
   * called.
   *
   * @param record      Record to save
   * @param clusterName Cluster name where to save the record
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   * @throws YTConcurrentModificationException if the version of the document is different by the
   *                                          version contained in the database.
   * @throws YTValidationException             if the document breaks some validation constraints
   *                                          defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, YTDocument#validate()
   */
  @Override
  public <RET extends YTRecord> RET save(YTRecord record, String clusterName) {
    checkOpenness();

    if (record instanceof YTEdge edge) {
      if (edge.isLightweight()) {
        record = edge.getFrom();
      }
    }

    // unwrap the record if wrapper is passed
    record = record.getRecord();

    if (record.isUnloaded()) {
      throw new YTDatabaseException(
          "Record "
              + record
              + " is not bound to session, please call "
              + YTDatabaseSession.class.getSimpleName()
              + ".bindToSession(record) before save it");
    }

    return saveInternal((YTRecordAbstract) record, clusterName);
  }

  private <RET extends YTRecord> RET saveInternal(YTRecordAbstract record, String clusterName) {

    if (!(record instanceof YTDocument document)) {
      assignAndCheckCluster(record, clusterName);
      return (RET) currentTx.saveRecord(record, clusterName);
    }

    YTDocument doc = document;
    ODocumentInternal.checkClass(doc, this);
    try {
      doc.autoConvertValues();
    } catch (YTValidationException e) {
      doc.undo();
      throw e;
    }
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);

    if (!doc.getIdentity().isValid()) {
      if (doc.getClassName() != null) {
        checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());
      }

      assignAndCheckCluster(doc, clusterName);
    } else {
      // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
      if (doc.getClassName() != null) {
        checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
      }
    }

    if (!serializer.equals(ORecordInternal.getRecordSerializer(doc))) {
      ORecordInternal.setRecordSerializer(doc, serializer);
    }

    doc = (YTDocument) currentTx.saveRecord(record, clusterName);
    return (RET) doc;
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countView(final String viewName) {
    final YTImmutableView cls =
        (YTImmutableView) getMetadata().getImmutableSchemaSnapshot().getView(viewName);
    if (cls == null) {
      throw new IllegalArgumentException("View '" + cls + "' not found in database");
    }

    return countClass(cls, false);
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if
   * polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    final YTImmutableClass cls =
        (YTImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);
    if (cls == null) {
      throw new IllegalArgumentException("Class '" + cls + "' not found in database");
    }

    return countClass(cls, iPolymorphic);
  }

  protected long countClass(final YTImmutableClass cls, final boolean iPolymorphic) {
    checkOpenness();

    long totalOnDb = cls.countImpl(iPolymorphic);

    long deletedInTx = 0;
    long addedInTx = 0;
    String className = cls.getName();
    if (getTransaction().isActive()) {
      for (ORecordOperation op : getTransaction().getRecordOperations()) {
        if (op.type == ORecordOperation.DELETED) {
          final YTRecord rec = op.record;
          if (rec instanceof YTDocument) {
            YTClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((YTDocument) rec));
            if (iPolymorphic) {
              if (schemaClass.isSubClassOf(className)) {
                deletedInTx++;
              }
            } else {
              if (className.equals(schemaClass.getName())
                  || className.equals(schemaClass.getShortName())) {
                deletedInTx++;
              }
            }
          }
        }
        if (op.type == ORecordOperation.CREATED) {
          final YTRecord rec = op.record;
          if (rec instanceof YTDocument) {
            YTClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((YTDocument) rec));
            if (schemaClass != null) {
              if (iPolymorphic) {
                if (schemaClass.isSubClassOf(className)) {
                  addedInTx++;
                }
              } else {
                if (className.equals(schemaClass.getName())
                    || className.equals(schemaClass.getShortName())) {
                  addedInTx++;
                }
              }
            }
          }
        }
      }
    }

    return (totalOnDb + addedInTx) - deletedInTx;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean commit() {
    checkOpenness();
    checkIfActive();

    if (currentTx.getStatus() == TXSTATUS.ROLLBACKING) {
      throw new YTRollbackException("Transaction is rolling back");
    }

    if (!currentTx.isActive()) {
      throw new YTDatabaseException("No active transaction to commit. Call begin() first");
    }

    if (currentTx.amountOfNestedTxs() > 1) {
      // This just do count down no real commit here
      currentTx.commit();
      return false;
    }

    // WAKE UP LISTENERS

    try {
      beforeCommitOperations();
    } catch (YTException e) {
      try {
        rollback();
      } catch (Exception re) {
        OLogManager.instance()
            .error(this, "Exception during rollback `%08X`", re, System.identityHashCode(re));
      }
      throw e;
    }
    try {
      currentTx.commit();
    } catch (RuntimeException e) {

      if ((e instanceof YTHighLevelException) || (e instanceof YTNeedRetryException)) {
        OLogManager.instance()
            .debug(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));
      } else {
        OLogManager.instance()
            .error(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));
      }

      // WAKE UP ROLLBACK LISTENERS
      beforeRollbackOperations();

      try {
        // ROLLBACK TX AT DB LEVEL
        currentTx.internalRollback();
      } catch (Exception re) {
        OLogManager.instance()
            .error(
                this, "Error during transaction rollback `%08X`", re, System.identityHashCode(re));
      }

      // WAKE UP ROLLBACK LISTENERS
      afterRollbackOperations();
      throw e;
    }

    return true;
  }

  protected void beforeCommitOperations() {
    for (YTDatabaseListener listener : browseListeners()) {
      try {
        listener.onBeforeTxCommit(this);
      } catch (Exception e) {
        OLogManager.instance()
            .error(
                this,
                "Cannot commit the transaction: caught exception on execution of"
                    + " %s.onBeforeTxCommit() `%08X`",
                e,
                listener.getClass().getName(),
                System.identityHashCode(e));
        throw YTException.wrapException(
            new YTTransactionException(
                "Cannot commit the transaction: caught exception on execution of "
                    + listener.getClass().getName()
                    + "#onBeforeTxCommit()"),
            e);
      }
    }
  }

  public void afterCommitOperations() {
    for (YTDatabaseListener listener : browseListeners()) {
      try {
        listener.onAfterTxCommit(this);
      } catch (Exception e) {
        final String message =
            "Error after the transaction has been committed. The transaction remains valid. The"
                + " exception caught was on execution of "
                + listener.getClass()
                + ".onAfterTxCommit() `%08X`";

        OLogManager.instance().error(this, message, e, System.identityHashCode(e));

        throw YTException.wrapException(new YTTransactionBlockedException(message), e);
      }
    }
  }

  protected void beforeRollbackOperations() {
    for (YTDatabaseListener listener : browseListeners()) {
      try {
        listener.onBeforeTxRollback(this);
      } catch (Exception t) {
        OLogManager.instance()
            .error(this, "Error before transaction rollback `%08X`", t, System.identityHashCode(t));
      }
    }
  }

  protected void afterRollbackOperations() {
    for (YTDatabaseListener listener : browseListeners()) {
      try {
        listener.onAfterTxRollback(this);
      } catch (Exception t) {
        OLogManager.instance()
            .error(this, "Error after transaction rollback `%08X`", t, System.identityHashCode(t));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback() {
    rollback(false);
  }

  @Override
  public void rollback(boolean force) throws YTTransactionException {
    checkOpenness();
    if (currentTx.isActive()) {

      if (!force && currentTx.amountOfNestedTxs() > 1) {
        // This just decrement the counter no real rollback here
        currentTx.rollback();
        return;
      }

      // WAKE UP LISTENERS
      beforeRollbackOperations();
      currentTx.rollback(force, -1);
      // WAKE UP LISTENERS
      afterRollbackOperations();
    }
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   */
  @Override
  public YTDatabaseSession getUnderlying() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    return componentsFactory;
  }

  public ORecordSerializer getSerializer() {
    return serializer;
  }

  /**
   * Sets serializer for the database which will be used for document serialization.
   *
   * @param serializer the serializer to set.
   */
  public void setSerializer(ORecordSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public void resetInitialization() {
    for (YTRecordHook h : hooks.keySet()) {
      h.onUnregister();
    }

    hooks.clear();
    compileHooks();

    close();

    initialized = false;
  }

  public void checkSecurity(final int operation, final YTIdentifiable record, String cluster) {
    if (cluster == null) {
      cluster = getClusterNameById(record.getIdentity().getClusterId());
    }
    checkSecurity(ORule.ResourceGeneric.CLUSTER, operation, cluster);

    if (record instanceof YTDocument) {
      String clazzName = ((YTDocument) record).getClassName();
      if (clazzName != null) {
        checkSecurity(ORule.ResourceGeneric.CLASS, operation, clazzName);
      }
    }
  }

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   * otherwise.
   */
  @Override
  public boolean isPooled() {
    return false;
  }

  /**
   * Use #activateOnCurrentThread instead.
   */
  @Deprecated
  public void setCurrentDatabaseInThreadLocal() {
    activateOnCurrentThread();
  }

  /**
   * Activates current database instance on current thread.
   */
  @Override
  public void activateOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    tl.set(this);
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    final YTDatabaseSessionInternal db = tl.getIfDefined();
    return db == this;
  }

  protected void checkOpenness() {
    if (status == STATUS.CLOSED) {
      throw new YTDatabaseException("Database '" + getURL() + "' is closed");
    }
  }

  private void popInHook(YTIdentifiable id) {
    inHook.remove(id);
  }

  private boolean pushInHook(YTIdentifiable id) {
    return inHook.add(id);
  }

  protected void callbackHookFailure(YTRecord record, boolean wasNew, byte[] stream) {
    if (stream != null && stream.length > 0) {
      callbackHooks(
          wasNew ? YTRecordHook.TYPE.CREATE_FAILED : YTRecordHook.TYPE.UPDATE_FAILED, record);
    }
  }

  protected void callbackHookSuccess(
      final YTRecord record,
      final boolean wasNew,
      final byte[] stream,
      final OStorageOperationResult<Integer> operationResult) {
    if (stream != null && stream.length > 0) {
      final YTRecordHook.TYPE hookType;
      if (!operationResult.isMoved()) {
        hookType = wasNew ? YTRecordHook.TYPE.AFTER_CREATE : YTRecordHook.TYPE.AFTER_UPDATE;
      } else {
        hookType =
            wasNew ? YTRecordHook.TYPE.CREATE_REPLICATED : YTRecordHook.TYPE.UPDATE_REPLICATED;
      }
      callbackHooks(hookType, record);
    }
  }

  protected void callbackHookFinalize(
      final YTRecord record, final boolean wasNew, final byte[] stream) {
    if (stream != null && stream.length > 0) {
      final YTRecordHook.TYPE hookType;
      hookType = wasNew ? YTRecordHook.TYPE.FINALIZE_CREATION : YTRecordHook.TYPE.FINALIZE_UPDATE;
      callbackHooks(hookType, record);

      clearDocumentTracking(record);
    }
  }

  protected static void clearDocumentTracking(final YTRecord record) {
    if (record instanceof YTDocument && ((YTDocument) record).isTrackingChanges()) {
      ODocumentInternal.clearTrackData((YTDocument) record);
    }
  }

  protected void checkRecordClass(
      final YTClass recordClass, final String iClusterName, final YTRecordId rid) {
    final YTClass clusterIdClass =
        metadata.getImmutableSchemaSnapshot().getClassByClusterId(rid.getClusterId());
    if (recordClass == null && clusterIdClass != null
        || clusterIdClass == null && recordClass != null
        || (recordClass != null && !recordClass.equals(clusterIdClass))) {
      throw new IllegalArgumentException(
          "Record saved into cluster '"
              + iClusterName
              + "' should be saved with class '"
              + clusterIdClass
              + "' but has been created with class '"
              + recordClass
              + "'");
    }
  }

  protected void init() {
    currentTx = new OTransactionNoTx(this);
  }

  public void checkIfActive() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    YTDatabaseSessionInternal currentDatabase = tl.get();
    //noinspection deprecation
    if (currentDatabase instanceof YTDatabaseDocumentTx databaseDocumentTx) {
      currentDatabase = databaseDocumentTx.internal;
    }
    if (currentDatabase != this) {
      throw new IllegalStateException(
          "The current database instance ("
              + this
              + ") is not active on the current thread ("
              + Thread.currentThread()
              + "). Current active database is: "
              + currentDatabase);
    }
  }

  @Override
  public boolean assertIfNotActive() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    YTDatabaseSessionInternal currentDatabase = tl.get();

    //noinspection deprecation
    if (currentDatabase instanceof YTDatabaseDocumentTx databaseDocumentTx) {
      currentDatabase = databaseDocumentTx.internal;
    }

    if (currentDatabase != this) {
      throw new YTSessionNotActivatedException(getName());
    }

    return true;
  }

  public IntSet getBlobClusterIds() {
    return getMetadata().getSchema().getBlobClusters();
  }

  private void compileHooks() {
    final List<YTRecordHook>[] intermediateHooksByScope =
        new List[YTRecordHook.SCOPE.values().length];
    for (YTRecordHook.SCOPE scope : YTRecordHook.SCOPE.values()) {
      intermediateHooksByScope[scope.ordinal()] = new ArrayList<>();
    }

    for (YTRecordHook hook : hooks.keySet()) {
      for (YTRecordHook.SCOPE scope : hook.getScopes()) {
        intermediateHooksByScope[scope.ordinal()].add(hook);
      }
    }

    for (YTRecordHook.SCOPE scope : YTRecordHook.SCOPE.values()) {
      final int ordinal = scope.ordinal();
      final List<YTRecordHook> scopeHooks = intermediateHooksByScope[ordinal];
      hooksByScope[ordinal] = scopeHooks.toArray(new YTRecordHook[0]);
    }
  }

  @Override
  public OSharedContext getSharedContext() {
    return sharedContext;
  }


  public void setUseLightweightEdges(boolean b) {
    this.setCustom("useLightweightEdges", b);
  }

  public YTEdgeInternal newLightweightEdge(String iClassName, YTVertex from, YTVertex to) {
    YTImmutableClass clazz =
        (YTImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    return new YTEdgeDelegate(from, to, clazz, iClassName);
  }

  public YTEdge newRegularEdge(String iClassName, YTVertex from, YTVertex to) {
    YTClass cl = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException(iClassName + " is not an edge class");
    }

    return addEdgeInternal(from, to, iClassName, true);
  }

  public synchronized void queryStarted(String id, OQueryDatabaseState state) {
    if (this.activeQueries.size() > 1 && this.activeQueries.size() % 10 == 0) {
      String msg =
          "This database instance has "
              + activeQueries.size()
              + " open command/query result sets, please make sure you close them with"
              + " YTResultSet.close()";
      OLogManager.instance().warn(this, msg);
      if (OLogManager.instance().isDebugEnabled()) {
        activeQueries.values().stream()
            .map(pendingQuery -> pendingQuery.getResultSet().getExecutionPlan())
            .filter(Objects::nonNull)
            .forEach(plan -> OLogManager.instance().debug(this, plan.toString()));
      }
    }
    this.activeQueries.put(id, state);

    getListeners().forEach((it) -> it.onCommandStart(this, state.getResultSet()));
  }

  public void queryClosed(String id) {
    OQueryDatabaseState removed = this.activeQueries.remove(id);
    getListeners().forEach((it) -> it.onCommandEnd(this, removed.getResultSet()));
    removed.closeInternal(this);
  }

  protected synchronized void closeActiveQueries() {
    while (!activeQueries.isEmpty()) {
      this.activeQueries
          .values()
          .iterator()
          .next()
          .close(this); // the query automatically unregisters itself
    }
  }

  public Map<String, OQueryDatabaseState> getActiveQueries() {
    return activeQueries;
  }

  public YTResultSet getActiveQuery(String id) {
    OQueryDatabaseState state = activeQueries.get(id);
    if (state != null) {
      return state.getResultSet();
    } else {
      return null;
    }
  }

  @Override
  public boolean isClusterEdge(int cluster) {
    YTClass clazz = getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(cluster);
    return clazz != null && clazz.isEdgeType();
  }

  @Override
  public boolean isClusterVertex(int cluster) {
    YTClass clazz = getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(cluster);
    return clazz != null && clazz.isVertexType();
  }

  @Override
  public boolean isClusterView(int cluster) {
    YTView view = getViewFromCluster(cluster);
    return view != null;
  }

  public YTView getViewFromCluster(int cluster) {
    return getMetadata().getImmutableSchemaSnapshot().getViewByClusterId(cluster);
  }

  public Map<UUID, OBonsaiCollectionPointer> getCollectionsChanges() {
    if (collectionsChanges == null) {
      collectionsChanges = new HashMap<>();
    }
    return collectionsChanges;
  }

  @Override
  public void executeInTx(Runnable runnable) {
    var ok = false;
    checkIfActive();
    begin();
    try {
      runnable.run();
      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, int batchSize, BiConsumer<YTDatabaseSession, T> consumer) {
    var ok = false;
    checkIfActive();
    int counter = 0;

    begin();
    try {
      for (T t : iterable) {
        consumer.accept(this, t);
        counter++;

        if (counter % batchSize == 0) {
          commit();
          begin();
        }
      }

      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator, BiConsumer<YTDatabaseSession, T> consumer) {
    forEachInTx(iterator, (db, t) -> {
      consumer.accept(db, t);
      return true;
    });
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable, BiConsumer<YTDatabaseSession, T> consumer) {
    forEachInTx(iterable.iterator(), consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream, BiConsumer<YTDatabaseSession, T> consumer) {
    try (Stream<T> s = stream) {
      forEachInTx(s.iterator(), consumer);
    }
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator,
      BiFunction<YTDatabaseSession, T, Boolean> consumer) {
    var ok = false;
    checkIfActive();

    begin();
    try {
      while (iterator.hasNext()) {
        var cont = consumer.apply(this, iterator.next());
        commit();
        if (!cont) {
          break;
        }
        begin();
      }

      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable,
      BiFunction<YTDatabaseSession, T, Boolean> consumer) {
    forEachInTx(iterable.iterator(), consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream,
      BiFunction<YTDatabaseSession, T, Boolean> consumer) {
    try (stream) {
      forEachInTx(stream.iterator(), consumer);
    }
  }

  private void finishTx(boolean ok) {
    if (currentTx.isActive()) {
      if (ok && currentTx.getStatus() != TXSTATUS.ROLLBACKING) {
        commit();
      } else {
        if (isActiveOnCurrentThread()) {
          rollback();
        } else {
          currentTx.rollback();
        }
      }
    }
  }

  @Override
  public <T> void executeInTxBatches(
      Iterator<T> iterator, int batchSize, BiConsumer<YTDatabaseSession, T> consumer) {
    var ok = false;
    checkIfActive();
    int counter = 0;

    begin();
    try {
      while (iterator.hasNext()) {
        consumer.accept(this, iterator.next());
        counter++;

        if (counter % batchSize == 0) {
          commit();
          begin();
        }
      }

      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  @Override
  public <T> void executeInTxBatches(
      Iterator<T> iterator, BiConsumer<YTDatabaseSession, T> consumer) {
    executeInTxBatches(
        iterator,
        getConfiguration().getValueAsInteger(YTGlobalConfiguration.TX_BATCH_SIZE),
        consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, BiConsumer<YTDatabaseSession, T> consumer) {
    executeInTxBatches(
        iterable,
        getConfiguration().getValueAsInteger(YTGlobalConfiguration.TX_BATCH_SIZE),
        consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Stream<T> stream, int batchSize, BiConsumer<YTDatabaseSession, T> consumer) {
    try (stream) {
      executeInTxBatches(stream.iterator(), batchSize, consumer);
    }
  }

  @Override
  public <T> void executeInTxBatches(Stream<T> stream, BiConsumer<YTDatabaseSession, T> consumer) {
    try (stream) {
      executeInTxBatches(stream.iterator(), consumer);
    }
  }

  @Override
  public <T> T computeInTx(Supplier<T> supplier) {
    checkIfActive();
    var ok = false;
    begin();
    try {
      var result = supplier.get();
      ok = true;
      return result;
    } finally {
      finishTx(ok);
    }
  }

  @Override
  public int activeTxCount() {
    var transaction = getTransaction();

    if (transaction.isActive()) {
      return transaction.amountOfNestedTxs();
    }

    return 0;
  }
}
