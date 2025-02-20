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

package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.HighLevelException;
import com.jetbrains.youtrack.db.api.exception.OfflineClusterException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.exception.TransactionException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Direction;
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
import com.jetbrains.youtrack.db.internal.common.concur.NeedRetryException;
import com.jetbrains.youtrack.db.internal.common.listener.ListenerManger;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.cache.LocalRecordCache;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.CurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.exception.SessionNotActivatedException;
import com.jetbrains.youtrack.db.internal.core.exception.TransactionBlockedException;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorClass;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.Metadata;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ImmutableUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrack.db.internal.core.query.Query;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EmbeddedEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import com.jetbrains.youtrack.db.internal.core.record.impl.StatefulEdgeImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.VertexEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.VertexInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJackson;
import com.jetbrains.youtrack.db.internal.core.storage.RawBuffer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction.TXSTATUS;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionAbstract;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionNoTx;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionOptimistic;
import com.jetbrains.youtrack.db.internal.core.tx.RollbackException;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Entity API entrypoint.
 */
@SuppressWarnings("unchecked")
public abstract class DatabaseSessionAbstract extends ListenerManger<SessionListener>
    implements DatabaseSessionInternal {

  protected final Map<String, Object> properties = new HashMap<>();
  protected Map<RecordHook, RecordHook.HOOK_POSITION> unmodifiableHooks;
  protected final Set<Identifiable> inHook = new HashSet<>();
  protected RecordSerializer serializer;
  protected String url;
  protected STATUS status;
  protected DatabaseSessionInternal databaseOwner;
  protected MetadataDefault metadata;
  protected ImmutableUser user;
  protected static final byte recordType = EntityImpl.RECORD_TYPE;
  protected final Map<RecordHook, RecordHook.HOOK_POSITION> hooks = new LinkedHashMap<>();
  protected boolean retainRecords = true;
  protected final LocalRecordCache localCache = new LocalRecordCache();
  protected CurrentStorageComponentsFactory componentsFactory;
  protected boolean initialized = false;
  protected FrontendTransactionAbstract currentTx;

  protected final RecordHook[][] hooksByScope =
      new RecordHook[RecordHook.SCOPE.values().length][];
  protected SharedContext sharedContext;

  private boolean prefetchRecords;

  protected Map<String, QueryDatabaseState> activeQueries = new ConcurrentHashMap<>();
  protected LinkedList<QueryDatabaseState> queryState = new LinkedList<>();
  private Map<UUID, BonsaiCollectionPointer> collectionsChanges;

  // database stats!
  protected long loadedRecordsCount;
  protected long totalRecordLoadMs;
  protected long minRecordLoadMs;
  protected long maxRecordLoadMs;
  protected long ridbagPrefetchCount;
  protected long totalRidbagPrefetchMs;
  protected long minRidbagPrefetchMs;
  protected long maxRidbagPrefetchMs;

  protected final ThreadLocal<DatabaseSessionInternal> activeSession = new ThreadLocal<>();

  protected DatabaseSessionAbstract() {
    // DO NOTHING IS FOR EXTENDED OBJECTS
    super(false);
  }

  /**
   * @return default serializer which is used to serialize documents. Default serializer is common
   * for all database instances.
   */
  public static RecordSerializer getDefaultSerializer() {
    return RecordSerializerFactory.instance().getDefaultRecordSerializer();
  }

  /**
   * Sets default serializer. The default serializer is common for all database instances.
   *
   * @param iDefaultSerializer new default serializer value
   */
  public static void setDefaultSerializer(RecordSerializer iDefaultSerializer) {
    RecordSerializerFactory.instance().setDefaultRecordSerializer(iDefaultSerializer);
  }

  public void callOnOpenListeners() {
    assert assertIfNotActive();
    wakeupOnOpenDbLifecycleListeners();
  }

  protected abstract void loadMetadata();

  public void callOnCloseListeners() {
    assert assertIfNotActive();
    wakeupOnCloseDbLifecycleListeners();
    wakeupOnCloseListeners();
  }

  private void wakeupOnOpenDbLifecycleListeners() {
    for (var it = YouTrackDBEnginesManager.instance()
        .getDbLifecycleListeners();
        it.hasNext(); ) {
      it.next().onOpen(getDatabaseOwner());
    }
  }


  private void wakeupOnCloseDbLifecycleListeners() {
    for (var it = YouTrackDBEnginesManager.instance()
        .getDbLifecycleListeners();
        it.hasNext(); ) {
      it.next().onClose(getDatabaseOwner());
    }
  }

  private void wakeupOnCloseListeners() {
    for (var listener : getListenersCopy()) {
      try {
        listener.onClose(getDatabaseOwner());
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends DBRecord> RET getRecord(final Identifiable iIdentifiable) {
    assert assertIfNotActive();
    if (iIdentifiable instanceof Record) {
      return (RET) iIdentifiable;
    }
    return load(iIdentifiable.getIdentity());
  }

  /**
   * Deletes the record checking the version.
   */
  private void delete(final RID iRecord, final int iVersion) {
    final var record = load(iRecord);
    RecordInternal.setVersion(record, iVersion);
    delete(record);
  }

  public DatabaseSessionInternal cleanOutRecord(final RID iRecord, final int iVersion) {
    assert assertIfNotActive();
    delete(iRecord, iVersion);
    return this;
  }

  public <REC extends DBRecord> RecordIteratorCluster<REC> browseCluster(
      final String iClusterName, final Class<REC> iClass) {
    assert assertIfNotActive();
    return (RecordIteratorCluster<REC>) browseCluster(iClusterName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public <REC extends DBRecord> RecordIteratorCluster<REC> browseCluster(
      final String iClusterName,
      final Class<REC> iRecordClass,
      final long startClusterPosition,
      final long endClusterPosition,
      final boolean loadTombstones) {
    checkSecurity(Rule.ResourceGeneric.CLUSTER, Role.PERMISSION_READ, iClusterName);
    assert assertIfNotActive();
    final var clusterId = getClusterIdByName(iClusterName);
    return new RecordIteratorCluster<>(
        this, clusterId, startClusterPosition, endClusterPosition);
  }

  @Override
  public <REC extends DBRecord> RecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition) {
    assert assertIfNotActive();
    checkSecurity(Rule.ResourceGeneric.CLUSTER, Role.PERMISSION_READ, iClusterName);
    final var clusterId = getClusterIdByName(iClusterName);
    //noinspection deprecation
    return new RecordIteratorCluster<>(this, clusterId, startClusterPosition, endClusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  public CommandRequest command(final CommandRequest iCommand) {
    checkSecurity(Rule.ResourceGeneric.COMMAND, Role.PERMISSION_READ);
    assert assertIfNotActive();
    final var command = (CommandRequestInternal) iCommand;
    try {
      command.reset();
      return command;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException(
              getDatabaseName(), "Error on command execution"), e, getDatabaseName());
    }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends List<?>> RET query(final Query<?> iCommand, final Object... iArgs) {
    assert assertIfNotActive();
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
    assert assertIfNotActive();
    return countClusterElements(iClusterIds, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int iClusterId) {
    assert assertIfNotActive();
    return countClusterElements(iClusterId, false);
  }

  /**
   * {@inheritDoc}
   */
  public MetadataDefault getMetadata() {
    assert assertIfNotActive();
    checkOpenness();
    return metadata;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DatabaseSessionInternal getDatabaseOwner() {
    assert assertIfNotActive();
    var current = databaseOwner;
    while (current != null && current != this && current.getDatabaseOwner() != current) {
      current = current.getDatabaseOwner();
    }
    return current;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DatabaseSessionInternal setDatabaseOwner(DatabaseSessionInternal iOwner) {
    assert assertIfNotActive();
    databaseOwner = iOwner;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isRetainRecords() {
    assert assertIfNotActive();
    return retainRecords;
  }

  /**
   * {@inheritDoc}
   */
  public DatabaseSession setRetainRecords(boolean retainRecords) {
    assert assertIfNotActive();
    this.retainRecords = retainRecords;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public DatabaseSession setStatus(final STATUS status) {
    assert assertIfNotActive();
    this.status = status;
    return this;
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
  public SecurityUser geCurrentUser() {
    assert assertIfNotActive();
    return user;
  }

  /**
   * {@inheritDoc}
   */
  public void setUser(final SecurityUser user) {
    assert assertIfNotActive();
    if (user instanceof SecurityUserImpl) {
      final Metadata metadata = getMetadata();
      if (metadata != null) {
        final var security = sharedContext.getSecurity();
        this.user = new ImmutableUser(this, security.getVersion(this), user);
      } else {
        this.user = new ImmutableUser(this, -1, user);
      }
    } else {
      this.user = (ImmutableUser) user;
    }
  }

  public void reloadUser() {
    assert assertIfNotActive();

    if (user != null) {
      if (user.checkIfAllowed(this, Rule.ResourceGeneric.CLASS, SecurityUserImpl.CLASS_NAME,
          Role.PERMISSION_READ)
          != null) {

        Metadata metadata = getMetadata();
        if (metadata != null) {
          final var security = sharedContext.getSecurity();
          final var secGetUser = security.getUser(this, user.getName(this));
          if (secGetUser != null) {
            user = new ImmutableUser(this, security.getVersion(this), secGetUser);
          } else {
            throw new SecurityException(url, "User not found");
          }
        } else {
          throw new SecurityException(url, "Metadata not found");
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isMVCC() {
    assert assertIfNotActive();
    return true;
  }

  /**
   * {@inheritDoc}
   */
  public DatabaseSession setMVCC(boolean mvcc) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  public void registerHook(final RecordHook iHookImpl,
      final RecordHook.HOOK_POSITION iPosition) {
    checkOpenness();
    assert assertIfNotActive();

    final Map<RecordHook, RecordHook.HOOK_POSITION> tmp =
        new LinkedHashMap<>(hooks);
    tmp.put(iHookImpl, iPosition);
    hooks.clear();
    for (var p : RecordHook.HOOK_POSITION.values()) {
      for (var e : tmp.entrySet()) {
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
  public void registerHook(final RecordHook iHookImpl) {
    assert assertIfNotActive();
    registerHook(iHookImpl, RecordHook.HOOK_POSITION.REGULAR);
  }

  /**
   * {@inheritDoc}
   */
  public void unregisterHook(final RecordHook iHookImpl) {
    assert assertIfNotActive();
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
  public LocalRecordCache getLocalCache() {
    return localCache;
  }


  @Nonnull
  @Override
  public RID refreshRid(@Nonnull RID rid) {
    if (rid.isPersistent()) {
      return rid;
    }

    if (!currentTx.isActive()) {
      throw new TransactionException(
          "Temporary rids can not be processed because transaction is not active");
    }

    var record = currentTx.getRecordEntry(rid);
    if (record == null || record.type == RecordOperation.DELETED) {
      throw new RecordNotFoundException(this, rid);
    }

    return record.record.getIdentity();
  }


  /**
   * {@inheritDoc}
   */
  public Map<RecordHook, RecordHook.HOOK_POSITION> getHooks() {
    assert assertIfNotActive();
    return unmodifiableHooks;
  }

  @Override
  public void deleteInternal(@Nonnull DBRecord record) {
    assert assertIfNotActive();
    checkOpenness();

    if (record instanceof Entity) {
      if (((Entity) record).isVertex()) {
        VertexInternal.deleteLinks(((Entity) record).castToVertex());
      } else {
        if (((Entity) record).isStatefulEdge()) {
          EdgeEntityImpl.deleteLinks(this, ((Entity) record).castToStatefulEdge());
        }
      }
    }

    // CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
    if (record instanceof EntityImpl && ((EntityImpl) record).getSchemaClassName() != null) {
      checkSecurity(
          Rule.ResourceGeneric.CLASS,
          Role.PERMISSION_DELETE,
          ((EntityImpl) record).getSchemaClassName());
    }

    try {
      currentTx.deleteRecord((RecordAbstract) record);
    } catch (BaseException e) {
      throw e;
    } catch (Exception e) {
      if (record instanceof EntityImpl) {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on deleting record "
                    + record.getIdentity()
                    + " of class '"
                    + ((EntityImpl) record).getSchemaClassName()
                    + "'"),
            e, getDatabaseName());
      } else {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on deleting record " + record.getIdentity()),
            e, getDatabaseName());
      }
    }
  }

  /**
   * Callback the registered hooks if any.
   *
   * @param type Hook type. Define when hook is called.
   * @param id   Record received in the callback
   * @return True if the input record is changed, otherwise false
   */
  public RecordHook.RESULT callbackHooks(final RecordHook.TYPE type, final Identifiable id) {
    assert assertIfNotActive();
    if (id == null || hooks.isEmpty() || id.getIdentity().getClusterId() == 0) {
      return RecordHook.RESULT.RECORD_NOT_CHANGED;
    }

    final var scope = RecordHook.SCOPE.typeToScope(type);
    final var scopeOrdinal = scope.ordinal();

    var identity = ((RecordId) id.getIdentity()).copy();
    if (!pushInHook(identity)) {
      return RecordHook.RESULT.RECORD_NOT_CHANGED;
    }

    try {
      final DBRecord rec;
      try {
        rec = id.getRecord(this);
      } catch (RecordNotFoundException e) {
        return RecordHook.RESULT.RECORD_NOT_CHANGED;
      }

      var recordChanged = false;
      for (var hook : hooksByScope[scopeOrdinal]) {
        final var res = hook.onTrigger(this, type, rec);

        if (res == RecordHook.RESULT.RECORD_CHANGED) {
          recordChanged = true;
        } else {
          if (res == RecordHook.RESULT.SKIP_IO)
          // SKIP IO OPERATION
          {
            return res;
          } else {
            if (res == RecordHook.RESULT.SKIP)
            // SKIP NEXT HOOKS AND RETURN IT
            {
              return res;
            }
          }
        }
      }
      return recordChanged
          ? RecordHook.RESULT.RECORD_CHANGED
          : RecordHook.RESULT.RECORD_NOT_CHANGED;
    } finally {
      popInHook(identity);
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isValidationEnabled() {
    assert assertIfNotActive();
    return (Boolean) get(ATTRIBUTES_INTERNAL.VALIDATION);
  }

  /**
   * {@inheritDoc}
   */
  public void setValidationEnabled(final boolean iEnabled) {
    assert assertIfNotActive();
    set(ATTRIBUTES_INTERNAL.VALIDATION, iEnabled);
  }

  @Override
  public ContextConfiguration getConfiguration() {
    assert assertIfNotActive();
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
    assert assertIfNotActive();
    return status;
  }

  @Override
  public String getDatabaseName() {
    return getStorageInfo() != null ? getStorageInfo().getName() : url;
  }

  @Override
  public String getURL() {
    return url != null ? url : getStorageInfo().getURL();
  }

  @Override
  public int getClusters() {
    assert assertIfNotActive();
    return getStorageInfo().getClusters();
  }

  @Override
  public boolean existsCluster(final String iClusterName) {
    assert assertIfNotActive();
    return getStorageInfo().getClusterNames().contains(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Collection<String> getClusterNames() {
    assert assertIfNotActive();
    return getStorageInfo().getClusterNames();
  }

  @Override
  public int getClusterIdByName(final String iClusterName) {
    if (iClusterName == null) {
      return -1;
    }

    assert assertIfNotActive();
    return getStorageInfo().getClusterIdByName(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public String getClusterNameById(final int iClusterId) {
    if (iClusterId < 0) {
      return null;
    }

    assert assertIfNotActive();
    return getStorageInfo().getPhysicalClusterNameById(iClusterId);
  }

  public void checkForClusterPermissions(final String iClusterName) {
    assert assertIfNotActive();
    // CHECK FOR ORESTRICTED
    final var classes =
        getMetadata().getImmutableSchemaSnapshot().getClassesRelyOnCluster(this, iClusterName);

    for (var c : classes) {
      if (c.isSubClassOf(this, SecurityShared.RESTRICTED_CLASSNAME)) {
        throw new SecurityException(getDatabaseName(),
            "Class '"
                + c.getName(this)
                + "' cannot be truncated because has record level security enabled (extends '"
                + SecurityShared.RESTRICTED_CLASSNAME
                + "')");
      }
    }
  }

  @Override
  public Object setProperty(final String iName, final Object iValue) {
    assert assertIfNotActive();
    if (iValue == null) {
      return properties.remove(iName.toLowerCase(Locale.ENGLISH));
    } else {
      return properties.put(iName.toLowerCase(Locale.ENGLISH), iValue);
    }
  }

  @Override
  public Object getProperty(final String iName) {
    assert assertIfNotActive();
    return properties.get(iName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    assert assertIfNotActive();
    return properties.entrySet().iterator();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    assert assertIfNotActive();

    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }
    final var storage = getStorageInfo();
    return switch (iAttribute) {
      case DATEFORMAT -> storage.getConfiguration().getDateFormat();
      case DATE_TIME_FORMAT -> storage.getConfiguration().getDateTimeFormat();
      case TIMEZONE -> storage.getConfiguration().getTimeZone().getID();
      case LOCALE_COUNTRY -> storage.getConfiguration().getLocaleCountry();
      case LOCALE_LANGUAGE -> storage.getConfiguration().getLocaleLanguage();
      case CHARSET -> storage.getConfiguration().getCharset();
      case CLUSTER_SELECTION -> storage.getConfiguration().getClusterSelection();
      case MINIMUM_CLUSTERS -> storage.getConfiguration().getMinimumClusters();
    };
  }

  @Override
  public Object get(ATTRIBUTES_INTERNAL attribute) {
    assert assertIfNotActive();

    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final var storage = getStorageInfo();
    if (attribute == ATTRIBUTES_INTERNAL.VALIDATION) {
      return storage.getConfiguration().isValidationEnabled();
    }

    throw new IllegalArgumentException("attribute is not supported: " + attribute);
  }


  public FrontendTransaction getTransaction() {
    assert assertIfNotActive();
    return currentTx;
  }

  /**
   * Returns the schema of the database.
   *
   * @return the schema of the database
   */
  @Override
  public Schema getSchema() {
    assert assertIfNotActive();
    return getMetadata().getSchema();
  }


  @Nonnull
  @SuppressWarnings("unchecked")
  @Override
  public <RET extends DBRecord> RET load(final RID recordId) {
    assert assertIfNotActive();
    return (RET) currentTx.loadRecord(recordId);
  }

  @Override
  public boolean exists(RID rid) {
    assert assertIfNotActive();
    return currentTx.exists(rid);
  }

  /**
   * Deletes the record without checking the version.
   */
  public void delete(final RID iRecord) {
    checkOpenness();
    assert assertIfNotActive();

    final var rec = load(iRecord);
    delete(rec);
  }

  @Override
  public BinarySerializerFactory getSerializerFactory() {
    assert assertIfNotActive();
    return componentsFactory.binarySerializerFactory;
  }

  @Override
  public void setPrefetchRecords(boolean prefetchRecords) {
    assert assertIfNotActive();
    this.prefetchRecords = prefetchRecords;
  }

  @Override
  public boolean isPrefetchRecords() {
    assert assertIfNotActive();
    return prefetchRecords;
  }

  @Override
  public <T extends Identifiable> T bindToSession(T identifiable) {
    if (!(identifiable instanceof DBRecord record)) {
      return identifiable;
    }

    if (identifiable instanceof Edge edge && edge.isLightweight()) {
      return (T) edge;
    }

    var rid = record.getIdentity();
    checkOpenness();
    assert assertIfNotActive();

    // unwrap the record if wrapper is passed
    record = record.getRecord(this);

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
      throw new DatabaseException(getDatabaseName(),
          "Cannot bind record to session with not persisted rid: " + rid);
    }

    var result = executeReadRecord((RecordId) rid);

    assert !result.isUnloaded();
    assert result.getSession() == this;

    return (T) result;
  }

  @Nonnull
  public final <RET extends RecordAbstract> RET executeReadRecord(final RecordId rid) {
    checkOpenness();
    assert assertIfNotActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      checkSecurity(
          Rule.ResourceGeneric.CLUSTER,
          Role.PERMISSION_READ,
          getClusterNameById(rid.getClusterId()));

      // SEARCH IN LOCAL TX
      var record = getTransaction().getRecord(rid);
      if (record == FrontendTransactionAbstract.DELETED_RECORD) {
        // DELETED IN TX
        throw new RecordNotFoundException(getDatabaseName(), rid);
      }

      var cachedRecord = localCache.findRecord(rid);
      if (record == null) {
        record = cachedRecord;
      }

      if (record != null && !record.isUnloaded()) {
        if (beforeReadOperations(record)) {
          throw new RecordNotFoundException(getDatabaseName(), rid);
        }

        afterReadOperations(record);
        if (record instanceof EntityImpl) {
          EntityInternalUtils.checkClass((EntityImpl) record, this);
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
      final RawBuffer recordBuffer;
      if (!rid.isValid()) {
        recordBuffer = null;
      } else {
        recordBuffer = getStorage().readRecord(this, rid, false, prefetchRecords, null);
      }

      if (recordBuffer == null) {
        throw new RecordNotFoundException(getDatabaseName(), rid);
      }

      if (record == null) {
        record =
            YouTrackDBEnginesManager.instance()
                .getRecordFactoryManager()
                .newInstance(recordBuffer.recordType, rid, this);
        RecordInternal.unsetDirty(record);
      }

      if (RecordInternal.getRecordType(this, record) != recordBuffer.recordType) {
        throw new DatabaseException(getDatabaseName(),
            "Record type is different from the one in the database");
      }

      RecordInternal.setRecordSerializer(record, serializer);
      RecordInternal.fill(record, rid, recordBuffer.version, recordBuffer.buffer, false);

      if (record instanceof EntityImpl) {
        EntityInternalUtils.checkClass((EntityImpl) record, this);
      }

      if (beforeReadOperations(record)) {
        throw new RecordNotFoundException(getDatabaseName(), rid);
      }

      RecordInternal.fromStream(record, recordBuffer.buffer);
      afterReadOperations(record);

      localCache.updateRecord(record);

      assert !record.isUnloaded();
      assert record.getSession() == this;

      return (RET) record;
    } catch (OfflineClusterException | RecordNotFoundException t) {
      throw t;
    } catch (Exception t) {
      if (rid.isTemporary()) {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on retrieving record using temporary RID: " + rid), t, getDatabaseName());
      } else {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on retrieving record "
                    + rid
                    + " (cluster: "
                    + getStorage().getPhysicalClusterNameById(rid.getClusterId())
                    + ")"),
            t, getDatabaseName());
      }
    } finally {
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public int assignAndCheckCluster(DBRecord record, String clusterName) {
    assert assertIfNotActive();

    var rid = (RecordId) record.getIdentity();
    // if provided a cluster name use it.
    if (rid.getClusterId() <= RID.CLUSTER_POS_INVALID && clusterName != null) {
      rid.setClusterId(getClusterIdByName(clusterName));
      if (rid.getClusterId() == -1) {
        throw new IllegalArgumentException("Cluster name '" + clusterName + "' is not configured");
      }
    }

    SchemaClassInternal schemaClass = null;
    // if cluster id is not set yet try to find it out
    if (rid.getClusterId() <= RID.CLUSTER_ID_INVALID
        && getStorageInfo().isAssigningClusterIds()) {
      if (record instanceof EntityImpl) {
        SchemaImmutableClass result = null;
        if (record != null) {
          result = ((EntityImpl) record).getImmutableSchemaClass(this);
        }
        schemaClass = result;
        if (schemaClass != null) {
          if (schemaClass.isAbstract(this)) {
            throw new SchemaException(getDatabaseName(),
                "Entity belongs to abstract class "
                    + schemaClass.getName(this)
                    + " and cannot be saved");
          }
          rid.setClusterId(schemaClass.getClusterForNewInstance(this, (EntityImpl) record));
        } else {
          throw new DatabaseException(getDatabaseName(),
              "Cannot save (1) entity " + record + ": no class or cluster defined");
        }
      } else {
        if (record instanceof RecordBytes) {
          var blobs = getBlobClusterIds();
          if (blobs.length == 0) {
            throw new DatabaseException(getDatabaseName(),
                "Cannot blob (2) " + record + ": no cluster defined");
          } else {
            rid.setClusterId(blobs[ThreadLocalRandom.current().nextInt(blobs.length)]);
          }

        } else {
          throw new DatabaseException(getDatabaseName(),
              "Cannot save (3) entity " + record + ": no class or cluster defined");
        }
      }
    } else {
      if (record instanceof EntityImpl) {
        SchemaImmutableClass result = null;
        if (record != null) {
          result = ((EntityImpl) record).getImmutableSchemaClass(this);
        }
        schemaClass = result;
      }
    }
    // If the cluster id was set check is validity
    if (rid.getClusterId() > RID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        var messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '"
                  + messageClusterName
                  + "' (id="
                  + rid.getClusterId()
                  + ") is not configured to store the class '"
                  + schemaClass.getName(this)
                  + "', valid are "
                  + Arrays.toString(schemaClass.getClusterIds(this)));
        }
      }
    }

    var clusterId = rid.getClusterId();
    if (clusterId < 0) {
      if (schemaClass == null && clusterName != null) {
        throw new DatabaseException(getDatabaseName(),
            "Impossible to set cluster for record " + record
                + " both cluster name and class are null");
      }

      throw new DatabaseException(getDatabaseName(),
          "Impossible to set cluster for record " + record + " class : " + schemaClass
              + " cluster name : " + clusterName);
    }

    return clusterId;
  }

  public int begin() {
    assert assertIfNotActive();

    if (currentTx.isActive()) {
      return currentTx.begin();
    }

    return begin(newTxInstance());
  }

  public int begin(FrontendTransactionOptimistic transaction) {
    checkOpenness();
    assert assertIfNotActive();

    // CHECK IT'S NOT INSIDE A HOOK
    if (!inHook.isEmpty()) {
      throw new IllegalStateException("Cannot begin a transaction while a hook is executing");
    }

    if (currentTx.isActive()) {
      if (currentTx instanceof FrontendTransactionOptimistic) {
        return currentTx.begin();
      }
    }

    // WAKE UP LISTENERS
    for (var listener : browseListeners()) {
      try {
        listener.onBeforeTxBegin(this);
      } catch (Exception e) {
        LogManager.instance().error(this, "Error before tx begin", e);
      }
    }

    currentTx = transaction;

    return currentTx.begin();
  }

  protected FrontendTransactionOptimistic newTxInstance() {
    assert assertIfNotActive();
    return new FrontendTransactionOptimistic(this);
  }

  public void setDefaultTransactionMode() {
    if (!(currentTx instanceof FrontendTransactionNoTx)) {
      currentTx = new FrontendTransactionNoTx(this);
    }
  }

  /**
   * Creates a new EntityImpl.
   */
  public EntityImpl newInstance() {
    assert assertIfNotActive();
    return newInstance(Entity.DEFAULT_CLASS_NAME);
  }

  @Override
  public EntityImpl newInternalInstance() {
    assert assertIfNotActive();

    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "New instance can be created only if transaction is active");
    }

    var clusterId = getClusterIdByName(MetadataDefault.CLUSTER_INTERNAL_NAME);
    var rid = new ChangeableRecordId();
    rid.setClusterId(clusterId);
    var entity = new EntityImpl(this, rid);

    var tx = (FrontendTransactionOptimistic) currentTx;
    tx.addRecordOperation(entity, RecordOperation.CREATED, null);

    return entity;
  }

  @Override
  public Blob newBlob(byte[] bytes) {
    assert assertIfNotActive();

    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "New instance can be created only if transaction is active");
    }

    var blob = new RecordBytes(this, bytes);
    assignAndCheckCluster(blob, null);

    var tx = (FrontendTransactionOptimistic) currentTx;
    tx.addRecordOperation(blob, RecordOperation.CREATED, null);

    return blob;
  }

  @Override
  public Blob newBlob() {
    assert assertIfNotActive();

    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "New instance can be created only if transaction is active");
    }

    var blob = new RecordBytes(this);
    assignAndCheckCluster(blob, null);

    var tx = (FrontendTransactionOptimistic) currentTx;
    tx.addRecordOperation(blob, RecordOperation.CREATED, null);

    return blob;
  }

  /**
   * Creates a entity with specific class.
   *
   * @param className the name of class that should be used as a class of created entity.
   * @return new instance of entity.
   */
  @Override
  public EntityImpl newInstance(final String className) {
    assert assertIfNotActive();
    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "New instance can be created only if transaction is active");
    }

    var entity = new EntityImpl(this, className);
    assignAndCheckCluster(entity, null);

    var tx = (FrontendTransactionOptimistic) currentTx;
    tx.addRecordOperation(entity, RecordOperation.CREATED, null);

    return entity;
  }

  @Override
  public Entity newEntity() {
    assert assertIfNotActive();
    return newInstance(Entity.DEFAULT_CLASS_NAME);
  }

  @Override
  public <T extends DBRecord> T fromJson(String json) {
    assert assertIfNotActive();
    return (T) RecordSerializerJackson.fromString(this, json);
  }

  @Override
  public Entity entityFromJson(String json) {
    assert assertIfNotActive();
    var result = RecordSerializerJackson.fromString(this, json);

    if (result instanceof Entity) {
      return (Entity) result;
    }

    throw new DatabaseException(getDatabaseName(), "The record is not an entity");
  }

  @Override
  public Entity newEntity(String className) {
    assert assertIfNotActive();
    return newInstance(className);
  }

  @Override
  public Entity newEmbededEntity(String className) {
    assert assertIfNotActive();
    return new EmbeddedEntityImpl(className, this);
  }

  @Override
  public Entity newEmbededEntity(SchemaClass schemaClass) {
    assert assertIfNotActive();
    return new EmbeddedEntityImpl(schemaClass.getName(this), this);
  }

  @Override
  public Entity newEmbededEntity() {
    assert assertIfNotActive();
    return new EmbeddedEntityImpl(this);
  }


  public Entity newEntity(SchemaClass clazz) {
    assert assertIfNotActive();
    return newInstance(clazz.getName(this));
  }

  public Vertex newVertex(final String className) {
    assert assertIfNotActive();

    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "New instance can be created only if transaction is active");
    }

    checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_CREATE, className);
    var vertex = new VertexEntityImpl(this, className);
    assignAndCheckCluster(vertex, null);

    var tx = (FrontendTransactionOptimistic) currentTx;
    tx.addRecordOperation(vertex, RecordOperation.CREATED, null);

    return vertex;
  }

  private StatefulEdgeImpl newStatefulEdgeInternal(final String className) {
    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "New instance can be created only if transaction is active");
    }

    checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_CREATE, className);
    return new StatefulEdgeImpl(newInstance(className), this);
  }

  @Override
  public Vertex newVertex(SchemaClass type) {
    assert assertIfNotActive();
    if (type == null) {
      return newVertex("V");
    }
    return newVertex(type.getName(this));
  }

  @Override
  public StatefulEdge newStatefulEdge(Vertex from, Vertex to, String type) {
    assert assertIfNotActive();
    var cl = getMetadata().getImmutableSchemaSnapshot().getClass(type);
    if (cl == null || !cl.isEdgeType(this)) {
      throw new IllegalArgumentException(type + " is not a regular edge class");
    }
    if (cl.isAbstract(this)) {
      throw new IllegalArgumentException(
          type + " is an abstract class and can not be used for creation of regular edge");
    }

    return (StatefulEdge) addEdgeInternal(from, to, type, true);
  }

  @Override
  public Edge newLightweightEdge(Vertex from, Vertex to, @Nonnull String type) {
    assert assertIfNotActive();
    var cl = getMetadata().getImmutableSchemaSnapshot().getClass(type);
    if (cl == null || !cl.isEdgeType(this)) {
      throw new IllegalArgumentException(type + " is not a lightweight edge class");
    }
    if (!cl.isAbstract(this)) {
      throw new IllegalArgumentException(
          type + " is not an abstract class and can not be used for creation of lightweight edge");
    }

    return addEdgeInternal(from, to, type, false);
  }

  @Override
  public StatefulEdge newStatefulEdge(Vertex from, Vertex to, SchemaClass type) {
    assert assertIfNotActive();
    if (type == null) {
      return newStatefulEdge(from, to, "E");
    }

    return newStatefulEdge(from, to, type.getName(this));
  }

  @Override
  public Edge newLightweightEdge(Vertex from, Vertex to, @Nonnull SchemaClass type) {
    assert assertIfNotActive();
    return newLightweightEdge(from, to, type.getName(this));
  }

  private EdgeInternal addEdgeInternal(
      final Vertex toVertex,
      final Vertex inVertex,
      String className,
      boolean isRegular) {
    Objects.requireNonNull(toVertex, "From vertex is null");
    Objects.requireNonNull(inVertex, "To vertex is null");

    EdgeInternal edge;
    EntityImpl outEntity;
    EntityImpl inEntity;

    var outEntityModified = false;
    if (checkDeletedInTx(toVertex)) {
      throw new RecordNotFoundException(getDatabaseName(),
          toVertex.getIdentity(), "The vertex " + toVertex.getIdentity() + " has been deleted");
    }

    if (checkDeletedInTx(inVertex)) {
      throw new RecordNotFoundException(getDatabaseName(),
          inVertex.getIdentity(), "The vertex " + inVertex.getIdentity() + " has been deleted");
    }

    try {
      outEntity = toVertex.getRecord(this);
    } catch (RecordNotFoundException e) {
      throw new IllegalArgumentException(
          "source vertex is invalid (rid=" + toVertex.getIdentity() + ")");
    }

    try {
      inEntity = inVertex.getRecord(this);
    } catch (RecordNotFoundException e) {
      throw new IllegalArgumentException(
          "source vertex is invalid (rid=" + inVertex.getIdentity() + ")");
    }

    Schema schema = getMetadata().getImmutableSchemaSnapshot();
    final var edgeType = schema.getClass(className);

    if (edgeType == null) {
      throw new IllegalArgumentException("Class " + className + " does not exist");
    }

    className = edgeType.getName(this);

    var createLightweightEdge =
        !isRegular
            && (edgeType.isAbstract(this) || className.equals(EdgeInternal.CLASS_NAME));
    if (!isRegular && !createLightweightEdge) {
      throw new IllegalArgumentException(
          "Cannot create lightweight edge for class " + className + " because it is not abstract");
    }

    final var outFieldName = Vertex.getEdgeLinkFieldName(Direction.OUT, className);
    final var inFieldName = Vertex.getEdgeLinkFieldName(Direction.IN, className);

    if (createLightweightEdge) {
      var lightWeightEdge = newLightweightEdgeInternal(className, toVertex, inVertex);
      VertexInternal.createLink(this, toVertex.getRecord(this), inVertex.getRecord(this),
          outFieldName);
      VertexInternal.createLink(this, inVertex.getRecord(this), toVertex.getRecord(this),
          inFieldName);
      edge = lightWeightEdge;
    } else {
      var statefulEdge = newStatefulEdgeInternal(className);
      statefulEdge.setPropertyInternal(EdgeInternal.DIRECTION_OUT, toVertex.getRecord(this));
      statefulEdge.setPropertyInternal(Edge.DIRECTION_IN, inEntity.getRecord(this));

      if (!outEntityModified) {
        // OUT-VERTEX ---> IN-VERTEX/EDGE
        VertexInternal.createLink(this, outEntity, statefulEdge.getRecord(this), outFieldName);
      }

      // IN-VERTEX ---> OUT-VERTEX/EDGE
      VertexInternal.createLink(this, inEntity, statefulEdge.getRecord(this), inFieldName);
      edge = statefulEdge;
    }
    // OK

    return edge;
  }

  private boolean checkDeletedInTx(Vertex currentVertex) {
    RID id;
    if (!currentVertex.getRecord(this).exists()) {
      id = currentVertex.getRecord(this).getIdentity();
    } else {
      return false;
    }

    final var oper = getTransaction().getRecordEntry(id);
    if (oper == null) {
      return id.isTemporary();
    } else {
      return oper.type == RecordOperation.DELETED;
    }
  }

  /**
   * {@inheritDoc}
   */
  public RecordIteratorClass<EntityImpl> browseClass(final @Nonnull String className) {
    assert assertIfNotActive();
    return browseClass(className, true);
  }

  /**
   * {@inheritDoc}
   */
  public RecordIteratorClass<EntityImpl> browseClass(
      final @Nonnull String className, final boolean iPolymorphic) {
    assert assertIfNotActive();
    if (getMetadata().getImmutableSchemaSnapshot().getClass(className) == null) {
      throw new IllegalArgumentException(
          "Class '" + className + "' not found in current database");
    }

    checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_READ, className);
    return new RecordIteratorClass<>(this, className, iPolymorphic, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordIteratorCluster<DBRecord> browseCluster(final String iClusterName) {
    assert assertIfNotActive();
    checkSecurity(Rule.ResourceGeneric.CLUSTER, Role.PERMISSION_READ, iClusterName);

    return new RecordIteratorCluster<>(this, getClusterIdByName(iClusterName));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<SessionListener> getListeners() {
    assert assertIfNotActive();
    return getListenersCopy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public RecordIteratorCluster<EntityImpl> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones) {
    assert assertIfNotActive();
    checkSecurity(Rule.ResourceGeneric.CLUSTER, Role.PERMISSION_READ, iClusterName);

    return new RecordIteratorCluster<>(
        this, getClusterIdByName(iClusterName), startClusterPosition, endClusterPosition);
  }

  /**
   * Saves a entity to the database. Behavior depends on the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is
   * running then the record will be changed at commit time. The current transaction will continue
   * to see the record as modified, while others not. If a Pessimistic transaction is running, then
   * an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   *
   * @param record Record to save.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   * @throws ConcurrentModificationException if the version of the entity is different by the
   *                                         version contained in the database.
   * @throws ValidationException             if the entity breaks some validation constraints
   *                                         defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends DBRecord> RET save(final DBRecord record) {
    assert assertIfNotActive();
    return save(record, null);
  }

  /**
   * Saves a entity specifying a cluster where to store the record. Behavior depends by the current
   * running transaction if any. If no transaction is running then changes apply immediately. If an
   * Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current
   * transaction will continue to see the record as modified, while others cannot access to it since
   * it's locked.
   *
   * @param record      Record to save
   * @param clusterName Cluster name where to save the record
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   * methods in chain.
   * @throws ConcurrentModificationException if the version of the entity is different by the
   *                                         version contained in the database.
   * @throws ValidationException             if the entity breaks some validation constraints
   *                                         defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, EntityImpl#validate()
   */
  @Override
  public <RET extends DBRecord> RET save(DBRecord record, String clusterName) {
    assert assertIfNotActive();

    checkOpenness();

    if (clusterName != null && record instanceof EntityImpl entity
        && entity.getSchemaClassName() != null) {
      throw new DatabaseException(getDatabaseName(),
          "Only entities without class name can be saved in predefined clusters");
    }

    if (record instanceof Edge edge) {
      if (edge.isLightweight()) {
        record = edge.getFrom();
      }
    }

    //unwrap the record if wrapper is passed
    record = record.getRecord(this);

    if (record.isUnloaded()) {
      throw new DatabaseException(getDatabaseName(),
          "Record "
              + record
              + " is not bound to session, please call "
              + DatabaseSession.class.getSimpleName()
              + ".bindToSession(record) before save it");
    }

    return saveInternal((RecordAbstract) record, clusterName);
  }

  private <RET extends DBRecord> RET saveInternal(RecordAbstract record, String clusterName) {

    if (!(record instanceof EntityImpl entity)) {
      assignAndCheckCluster(record, clusterName);
      return (RET) currentTx.saveRecord(record, clusterName);
    }

    EntityInternalUtils.checkClass(entity, this);
    try {
      entity.autoConvertValues();
    } catch (ValidationException e) {
      entity.undo();
      throw e;
    }
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(entity);

    if (!entity.getIdentity().isValid()) {
      if (entity.getSchemaClassName() != null) {
        checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_CREATE,
            entity.getSchemaClassName());
      }

      assignAndCheckCluster(entity, clusterName);
    } else {
      // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
      if (entity.getSchemaClassName() != null) {
        checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_UPDATE,
            entity.getSchemaClassName());
      }
    }

    if (!serializer.equals(RecordInternal.getRecordSerializer(entity))) {
      RecordInternal.setRecordSerializer(entity, serializer);
    }

    return (RET) currentTx.saveRecord(record, clusterName);
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    assert assertIfNotActive();
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if
   * polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    assert assertIfNotActive();
    final var cls =
        (SchemaImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);
    if (cls == null) {
      throw new IllegalArgumentException("Class not found in database");
    }

    return countClass(cls, iPolymorphic);
  }

  protected long countClass(final SchemaImmutableClass cls, final boolean iPolymorphic) {
    checkOpenness();
    assert assertIfNotActive();

    var totalOnDb = cls.countImpl(iPolymorphic, this);

    long deletedInTx = 0;
    long addedInTx = 0;
    var className = cls.getName(this);
    if (getTransaction().isActive()) {
      for (var op : getTransaction().getRecordOperations()) {
        if (op.type == RecordOperation.DELETED) {
          final DBRecord rec = op.record;
          if (rec instanceof EntityImpl) {
            SchemaImmutableClass result = null;
            result = ((EntityImpl) rec).getImmutableSchemaClass(this);
            SchemaClass schemaClass = result;
            if (iPolymorphic) {
              if (schemaClass.isSubClassOf(this, className)) {
                deletedInTx++;
              }
            } else {
              if (className.equals(schemaClass.getName(this))
                  || className.equals(schemaClass.getShortName(this))) {
                deletedInTx++;
              }
            }
          }
        }
        if (op.type == RecordOperation.CREATED) {
          final DBRecord rec = op.record;
          if (rec instanceof EntityImpl) {
            SchemaImmutableClass result = null;
            result = ((EntityImpl) rec).getImmutableSchemaClass(this);
            SchemaClass schemaClass = result;
            if (schemaClass != null) {
              if (iPolymorphic) {
                if (schemaClass.isSubClassOf(this, className)) {
                  addedInTx++;
                }
              } else {
                if (className.equals(schemaClass.getName(this))
                    || className.equals(schemaClass.getShortName(this))) {
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
    assert assertIfNotActive();

    if (currentTx.getStatus() == TXSTATUS.ROLLBACKING) {
      throw new RollbackException("Transaction is rolling back");
    }

    if (!currentTx.isActive()) {
      throw new DatabaseException(getDatabaseName(),
          "No active transaction to commit. Call begin() first");
    }

    if (currentTx.amountOfNestedTxs() > 1) {
      // This just do count down no real commit here
      currentTx.commit();
      return false;
    }

    // WAKE UP LISTENERS

    try {
      beforeCommitOperations();
    } catch (BaseException e) {
      try {
        rollback();
      } catch (Exception re) {
        LogManager.instance()
            .error(this, "Exception during rollback `%08X`", re, System.identityHashCode(re));
      }
      throw e;
    }
    try {
      currentTx.commit();
    } catch (RuntimeException e) {

      if ((e instanceof HighLevelException) || (e instanceof NeedRetryException)) {
        LogManager.instance()
            .debug(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));
      } else {
        LogManager.instance()
            .error(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));
      }

      // WAKE UP ROLLBACK LISTENERS
      beforeRollbackOperations();

      try {
        // ROLLBACK TX AT DB LEVEL
        currentTx.internalRollback();
      } catch (Exception re) {
        LogManager.instance()
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
    assert assertIfNotActive();
    for (var listener : browseListeners()) {
      try {
        listener.onBeforeTxCommit(this);
      } catch (Exception e) {
        LogManager.instance()
            .error(
                this,
                "Cannot commit the transaction: caught exception on execution of"
                    + " %s.onBeforeTxCommit() `%08X`",
                e,
                listener.getClass().getName(),
                System.identityHashCode(e));
        throw BaseException.wrapException(
            new TransactionException(getDatabaseName(),
                "Cannot commit the transaction: caught exception on execution of "
                    + listener.getClass().getName()
                    + "#onBeforeTxCommit()"),
            e, getDatabaseName());
      }
    }
  }

  public void afterCommitOperations() {
    assert assertIfNotActive();
    for (var listener : browseListeners()) {
      try {
        listener.onAfterTxCommit(this);
      } catch (Exception e) {
        final var message =
            "Error after the transaction has been committed. The transaction remains valid. The"
                + " exception caught was on execution of "
                + listener.getClass()
                + ".onAfterTxCommit() `%08X`";

        LogManager.instance().error(this, message, e, System.identityHashCode(e));

        throw BaseException.wrapException(
            new TransactionBlockedException(getDatabaseName(), message), e,
            getDatabaseName());
      }
    }
  }

  protected void beforeRollbackOperations() {
    assert assertIfNotActive();
    for (var listener : browseListeners()) {
      try {
        listener.onBeforeTxRollback(this);
      } catch (Exception t) {
        LogManager.instance()
            .error(this, "Error before transaction rollback `%08X`", t, System.identityHashCode(t));
      }
    }
  }

  protected void afterRollbackOperations() {
    assert assertIfNotActive();
    for (var listener : browseListeners()) {
      try {
        listener.onAfterTxRollback(this);
      } catch (Exception t) {
        LogManager.instance()
            .error(this, "Error after transaction rollback `%08X`", t, System.identityHashCode(t));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback() {
    assert assertIfNotActive();
    rollback(false);
  }

  @Override
  public void rollback(boolean force) throws TransactionException {
    checkOpenness();
    assert assertIfNotActive();

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
  public DatabaseSession getUnderlying() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CurrentStorageComponentsFactory getStorageVersions() {
    assert assertIfNotActive();
    return componentsFactory;
  }

  public RecordSerializer getSerializer() {
    assert assertIfNotActive();
    return serializer;
  }

  /**
   * Sets serializer for the database which will be used for entity serialization.
   *
   * @param serializer the serializer to set.
   */
  public void setSerializer(RecordSerializer serializer) {
    assert assertIfNotActive();
    this.serializer = serializer;
  }

  @Override
  public void resetInitialization() {
    assert assertIfNotActive();
    for (var h : hooks.keySet()) {
      h.onUnregister();
    }

    hooks.clear();
    compileHooks();

    close();

    initialized = false;
  }

  public void checkSecurity(final int operation, final Identifiable record, String cluster) {
    assert assertIfNotActive();
    if (cluster == null) {
      cluster = getClusterNameById(record.getIdentity().getClusterId());
    }
    checkSecurity(Rule.ResourceGeneric.CLUSTER, operation, cluster);

    if (record instanceof EntityImpl) {
      var clazzName = ((EntityImpl) record).getSchemaClassName();
      if (clazzName != null) {
        checkSecurity(Rule.ResourceGeneric.CLASS, operation, clazzName);
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
    activeSession.set(this);
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    return activeSession.get() == this;
  }

  protected void checkOpenness() {
    if (status == STATUS.CLOSED) {
      throw new DatabaseException(getDatabaseName(), "Database '" + getURL() + "' is closed");
    }
  }

  private void popInHook(Identifiable id) {
    inHook.remove(id);
  }

  private boolean pushInHook(Identifiable id) {
    return inHook.add(id);
  }

  protected void checkRecordClass(
      final SchemaClass recordClass, final String iClusterName, final RecordId rid) {
    assert assertIfNotActive();

    final var clusterIdClass =
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
    assert assertIfNotActive();
    currentTx = new FrontendTransactionNoTx(this);
  }

  @Override
  public boolean assertIfNotActive() {
    var currentDatabase = activeSession.get();

    //noinspection deprecation
    if (currentDatabase instanceof DatabaseDocumentTx databaseDocumentTx) {
      currentDatabase = databaseDocumentTx.internal;
    }

    if (currentDatabase != this) {
      throw new SessionNotActivatedException(getDatabaseName());
    }

    return true;
  }

  public int[] getBlobClusterIds() {
    assert assertIfNotActive();
    return getMetadata().getSchema().getBlobClusters().toIntArray();
  }

  private void compileHooks() {
    final List<RecordHook>[] intermediateHooksByScope =
        new List[RecordHook.SCOPE.values().length];
    for (var scope : RecordHook.SCOPE.values()) {
      intermediateHooksByScope[scope.ordinal()] = new ArrayList<>();
    }

    for (var hook : hooks.keySet()) {
      for (var scope : hook.getScopes()) {
        intermediateHooksByScope[scope.ordinal()].add(hook);
      }
    }

    for (var scope : RecordHook.SCOPE.values()) {
      final var ordinal = scope.ordinal();
      final var scopeHooks = intermediateHooksByScope[ordinal];
      hooksByScope[ordinal] = scopeHooks.toArray(new RecordHook[0]);
    }
  }

  @Override
  public SharedContext getSharedContext() {
    assert assertIfNotActive();
    return sharedContext;
  }


  public EdgeInternal newLightweightEdgeInternal(String iClassName, Vertex from, Vertex to) {
    assert assertIfNotActive();
    var clazz =
        (SchemaImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    return new EdgeImpl(this, from, to, clazz);
  }

  public Edge newRegularEdge(String iClassName, Vertex from, Vertex to) {
    assert assertIfNotActive();
    var cl = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cl == null || !cl.isEdgeType(this)) {
      throw new IllegalArgumentException(iClassName + " is not an edge class");
    }

    return addEdgeInternal(from, to, iClassName, true);
  }

  public synchronized void queryStarted(String id, QueryDatabaseState state) {
    assert assertIfNotActive();

    if (this.activeQueries.size() > 1 && this.activeQueries.size() % 10 == 0) {
      var msg =
          "This database instance has "
              + activeQueries.size()
              + " open command/query result sets, please make sure you close them with"
              + " ResultSet.close()";
      LogManager.instance().warn(this, msg);
      if (LogManager.instance().isDebugEnabled()) {
        activeQueries.values().stream()
            .map(pendingQuery -> pendingQuery.getResultSet().getExecutionPlan())
            .filter(Objects::nonNull)
            .forEach(plan -> LogManager.instance().debug(this, plan.toString()));
      }
    }
    this.activeQueries.put(id, state);

    getListeners().forEach((it) -> it.onCommandStart(this, state.getResultSet()));
  }

  public void queryClosed(String id) {
    assert assertIfNotActive();

    var removed = this.activeQueries.remove(id);
    getListeners().forEach((it) -> it.onCommandEnd(this, removed.getResultSet()));

  }

  protected synchronized void closeActiveQueries() {
    while (!activeQueries.isEmpty()) {
      this.activeQueries
          .values()
          .iterator()
          .next()
          .close(); // the query automatically unregisters itself
    }
  }

  public Map<String, QueryDatabaseState> getActiveQueries() {
    assert assertIfNotActive();

    return activeQueries;
  }

  public ResultSet getActiveQuery(String id) {
    assert assertIfNotActive();

    var state = activeQueries.get(id);
    if (state != null) {
      return state.getResultSet();
    } else {
      return null;
    }
  }

  @Override
  public boolean isClusterEdge(int cluster) {
    assert assertIfNotActive();
    var clazz = getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(cluster);
    return clazz != null && clazz.isEdgeType(this);
  }

  @Override
  public boolean isClusterVertex(int cluster) {
    assert assertIfNotActive();
    var clazz = getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(cluster);
    return clazz != null && clazz.isVertexType(this);
  }


  public Map<UUID, BonsaiCollectionPointer> getCollectionsChanges() {
    assert assertIfNotActive();

    if (collectionsChanges == null) {
      collectionsChanges = new HashMap<>();
    }

    return collectionsChanges;
  }

  @Override
  public void executeInTx(Runnable runnable) {
    var ok = false;
    assert assertIfNotActive();
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
      Iterable<T> iterable, int batchSize, BiConsumer<DatabaseSession, T> consumer) {
    var ok = false;
    assert assertIfNotActive();
    var counter = 0;

    begin();
    try {
      for (var t : iterable) {
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
  public <T> void forEachInTx(Iterator<T> iterator, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    forEachInTx(iterator, (db, t) -> {
      consumer.accept(db, t);
      return true;
    });
  }

  @Override
  public <T> void forEachInTx(Iterable<T> iterable, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    forEachInTx(iterable.iterator(), consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    try (var s = stream) {
      forEachInTx(s.iterator(), consumer);
    }
  }

  @Override
  public <T> void forEachInTx(Iterator<T> iterator,
      BiFunction<DatabaseSession, T, Boolean> consumer) {
    var ok = false;
    assert assertIfNotActive();

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
      BiFunction<DatabaseSession, T, Boolean> consumer) {
    assert assertIfNotActive();

    forEachInTx(iterable.iterator(), consumer);
  }

  @Override
  public <T> void forEachInTx(Stream<T> stream,
      BiFunction<DatabaseSession, T, Boolean> consumer) {
    assert assertIfNotActive();

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
      Iterator<T> iterator, int batchSize, BiConsumer<DatabaseSession, T> consumer) {
    var ok = false;
    assert assertIfNotActive();
    var counter = 0;

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
      Iterator<T> iterator, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    executeInTxBatches(
        iterator,
        getConfiguration().getValueAsInteger(GlobalConfiguration.TX_BATCH_SIZE),
        consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Iterable<T> iterable, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    executeInTxBatches(
        iterable,
        getConfiguration().getValueAsInteger(GlobalConfiguration.TX_BATCH_SIZE),
        consumer);
  }

  @Override
  public <T> void executeInTxBatches(
      Stream<T> stream, int batchSize, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    try (stream) {
      executeInTxBatches(stream.iterator(), batchSize, consumer);
    }
  }

  @Override
  public <T> void executeInTxBatches(Stream<T> stream, BiConsumer<DatabaseSession, T> consumer) {
    assert assertIfNotActive();

    try (stream) {
      executeInTxBatches(stream.iterator(), consumer);
    }
  }

  @Override
  public <T> T computeInTx(Supplier<T> supplier) {
    assert assertIfNotActive();
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
    assert assertIfNotActive();

    var transaction = getTransaction();

    if (transaction.isActive()) {
      return transaction.amountOfNestedTxs();
    }

    return 0;
  }

  @Override
  public <T> List<T> newEmbeddedList() {
    return new TrackedList<T>();
  }

  @Override
  public <T> List<T> newEmbeddedList(int size) {
    return new TrackedList<>(size);
  }

  @Override
  public List<Identifiable> newLinkList() {
    return new LinkList();
  }

  @Override
  public List<Identifiable> newLinkList(int size) {
    return new LinkList(size);
  }

  @Override
  public <T> Set<T> newEmbeddedSet() {
    return new TrackedSet<>();
  }

  @Override
  public <T> Set<T> newEmbeddedSet(int size) {
    return new TrackedSet<>(size);
  }

  @Override
  public Set<Identifiable> newLinkSet() {
    return new LinkSet();
  }

  @Override
  public Set<Identifiable> newLinkSet(int size) {
    return new LinkSet(size);
  }

  @Override
  public <V> Map<String, V> newEmbeddedMap() {
    return new TrackedMap<>();
  }

  @Override
  public <V> Map<String, V> newEmbeddedMap(int size) {
    return new TrackedMap<>(size);
  }

  @Override
  public Map<String, Identifiable> newLinkMap() {
    return new LinkMap();
  }

  @Override
  public Map<String, Identifiable> newLinkMap(int size) {
    return new LinkMap(size);
  }
}
