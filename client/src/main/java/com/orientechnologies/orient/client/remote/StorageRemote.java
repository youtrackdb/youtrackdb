/*
 *
 *  *  Copyright YouTrackDB
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
 */
package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.common.concur.OfflineNodeException;
import com.jetbrains.youtrack.db.internal.common.concur.lock.ThreadInterruptedException;
import com.jetbrains.youtrack.db.internal.common.concur.lock.ModificationOperationProhibitedException;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.io.YTIOException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutors;
import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.StorageClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.RecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.CredentialInterceptor;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.query.LiveQuery;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.RawBuffer;
import com.jetbrains.youtrack.db.internal.core.tx.TransactionOptimistic;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.SocketChannelBinary;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.TokenSecurityException;
import com.orientechnologies.orient.client.NotSendRequestException;
import com.orientechnologies.orient.client.binary.SocketChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.db.document.DatabaseSessionRemote;
import com.orientechnologies.orient.client.remote.db.document.TransactionOptimisticClient;
import com.orientechnologies.orient.client.remote.db.document.YTLiveQueryMonitorRemote;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OAddClusterResponse;
import com.orientechnologies.orient.client.remote.message.OBeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCloseQueryRequest;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommandResponse;
import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.client.remote.message.OCommit38Request;
import com.orientechnologies.orient.client.remote.message.OCountRecordsRequest;
import com.orientechnologies.orient.client.remote.message.OCountRecordsResponse;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCountResponse;
import com.orientechnologies.orient.client.remote.message.ODropClusterRequest;
import com.orientechnologies.orient.client.remote.message.ODropClusterResponse;
import com.orientechnologies.orient.client.remote.message.OFetchTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OFetchTransaction38Response;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeResponse;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataResponse;
import com.orientechnologies.orient.client.remote.message.OGetSizeRequest;
import com.orientechnologies.orient.client.remote.message.OGetSizeResponse;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OImportResponse;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupResponse;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OOpen37Request;
import com.orientechnologies.orient.client.remote.message.OOpen37Response;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OQueryNextPageRequest;
import com.orientechnologies.orient.client.remote.message.OQueryRequest;
import com.orientechnologies.orient.client.remote.message.OQueryResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordResponse;
import com.orientechnologies.orient.client.remote.message.ORecordExistsRequest;
import com.orientechnologies.orient.client.remote.message.OReloadRequest37;
import com.orientechnologies.orient.client.remote.message.OReloadResponse37;
import com.orientechnologies.orient.client.remote.message.OReopenRequest;
import com.orientechnologies.orient.client.remote.message.OReopenResponse;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OSendTransactionStateRequest;
import com.orientechnologies.orient.client.remote.message.OSendTransactionStateResponse;
import com.orientechnologies.orient.client.remote.message.OSubscribeDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeLiveQueryRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeLiveQueryResponse;
import com.orientechnologies.orient.client.remote.message.OSubscribeSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeStorageConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeLiveQueryRequest;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeRequest;
import com.orientechnologies.orient.client.remote.message.RemoteResultSet;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestAsynch;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.LiveQueryMonitor;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseDocumentTxInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.CurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.TokenException;
import com.jetbrains.youtrack.db.internal.core.record.RecordVersionHelper;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster;
import com.jetbrains.youtrack.db.internal.core.storage.RecordCallback;
import com.jetbrains.youtrack.db.internal.core.storage.RecordMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.StorageProxy;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.PaginatedCluster;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.RecordSerializationContext;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.SBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.tx.TransactionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.DistributedRedirectException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This object is bound to each remote ODatabase instances.
 */
public class StorageRemote implements StorageProxy, ORemotePushHandler, Storage {

  @Deprecated
  public static final String PARAM_CONNECTION_STRATEGY = "connectionStrategy";

  public static final String DRIVER_NAME = "YouTrackDB Java";

  private static final AtomicInteger sessionSerialId = new AtomicInteger(-1);

  public enum CONNECTION_STRATEGY {
    STICKY,
    ROUND_ROBIN_CONNECT,
    ROUND_ROBIN_REQUEST
  }

  private CONNECTION_STRATEGY connectionStrategy = CONNECTION_STRATEGY.STICKY;

  private final SBTreeCollectionManagerRemote sbTreeCollectionManager =
      new SBTreeCollectionManagerRemote();
  private final ORemoteURLs serverURLs;
  private final Map<String, StorageCluster> clusterMap = new ConcurrentHashMap<String, StorageCluster>();
  private final ExecutorService asynchExecutor;
  private final EntityImpl clusterConfiguration = new EntityImpl();
  private final AtomicInteger users = new AtomicInteger(0);
  private final ContextConfiguration clientConfiguration;
  private final int connectionRetry;
  private final int connectionRetryDelay;
  private StorageCluster[] clusters = CommonConst.EMPTY_CLUSTER_ARRAY;
  private int defaultClusterId;
  public ORemoteConnectionManager connectionManager;
  private final Set<OStorageRemoteSession> sessions =
      Collections.newSetFromMap(new ConcurrentHashMap<OStorageRemoteSession, Boolean>());

  private final Map<Integer, OLiveQueryClientListener> liveQueryListener =
      new ConcurrentHashMap<>();
  private volatile OStorageRemotePushThread pushThread;
  protected final YouTrackDBRemote context;
  protected SharedContext sharedContext = null;
  protected final String url;
  protected final ReentrantReadWriteLock stateLock;

  protected volatile StorageConfiguration configuration;
  protected volatile CurrentStorageComponentsFactory componentsFactory;
  protected String name;

  protected volatile STATUS status = STATUS.CLOSED;

  public static final String ADDRESS_SEPARATOR = ";";

  private static String buildUrl(String[] hosts, String name) {
    return String.join(ADDRESS_SEPARATOR, hosts) + "/" + name;
  }

  public StorageRemote(
      final ORemoteURLs hosts,
      String name,
      YouTrackDBRemote context,
      final String iMode,
      ORemoteConnectionManager connectionManager,
      YouTrackDBConfig config)
      throws IOException {
    this(hosts, name, context, iMode, connectionManager, null, config);
  }

  public StorageRemote(
      final ORemoteURLs hosts,
      String name,
      YouTrackDBRemote context,
      final String iMode,
      ORemoteConnectionManager connectionManager,
      final STATUS status,
      YouTrackDBConfig config)
      throws IOException {

    this.name = normalizeName(name);

    if (StringSerializerHelper.contains(this.name, ',')) {
      throw new IllegalArgumentException("Invalid character in storage name: " + this.name);
    }

    url = buildUrl(hosts.getUrls().toArray(new String[]{}), name);

    stateLock = new ReentrantReadWriteLock();
    if (status != null) {
      this.status = status;
    }

    configuration = null;

    if (config != null) {
      clientConfiguration = config.getConfigurations();
    } else {
      clientConfiguration = new ContextConfiguration();
    }
    connectionRetry =
        clientConfiguration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay =
        clientConfiguration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    serverURLs = hosts;

    asynchExecutor = ThreadPoolExecutors.newSingleThreadScheduledPool("StorageRemote Async");

    this.connectionManager = connectionManager;
    this.context = context;
  }

  private String normalizeName(String name) {
    if (StringSerializerHelper.contains(name, '/')) {
      name = name.substring(name.lastIndexOf('/') + 1);

      if (StringSerializerHelper.contains(name, '\\')) {
        return name.substring(name.lastIndexOf('\\') + 1);
      } else {
        return name;
      }

    } else {
      if (StringSerializerHelper.contains(name, '\\')) {
        name = name.substring(name.lastIndexOf('\\') + 1);

        if (StringSerializerHelper.contains(name, '/')) {
          return name.substring(name.lastIndexOf('/') + 1);
        } else {
          return name;
        }
      } else {
        return name;
      }
    }
  }

  public StorageConfiguration getConfiguration() {
    return configuration;
  }

  public boolean checkForRecordValidity(final PhysicalPosition ppos) {
    return ppos != null && !RecordVersionHelper.isTombstone(ppos.recordVersion);
  }

  public String getName() {
    return name;
  }

  public void setSharedContext(SharedContext sharedContext) {
    this.sharedContext = sharedContext;
  }

  public <T extends OBinaryResponse> T asyncNetworkOperationNoRetry(
      DatabaseSessionRemote database, final OBinaryAsyncRequest<T> request,
      int mode,
      final RecordId recordId,
      final RecordCallback<T> callback,
      final String errorMessage) {
    return asyncNetworkOperationRetry(database, request, mode, recordId, callback, errorMessage, 0);
  }

  public <T extends OBinaryResponse> T asyncNetworkOperationRetry(
      DatabaseSessionRemote database, final OBinaryAsyncRequest<T> request,
      int mode,
      final RecordId recordId,
      final RecordCallback<T> callback,
      final String errorMessage,
      int retry) {
    final int pMode;
    if (mode == 1 && callback == null)
    // ASYNCHRONOUS MODE NO ANSWER
    {
      pMode = 2;
    } else {
      pMode = mode;
    }
    request.setMode((byte) pMode);
    return baseNetworkOperation(database,
        (network, session) -> {
          // Send The request
          try {
            try {
              network.beginRequest(request.getCommand(), session);
              request.write(database, network, session);
            } finally {
              network.endRequest();
            }
          } catch (IOException e) {
            throw new NotSendRequestException("Cannot send request on this channel");
          }
          final T response = request.createResponse();
          T ret = null;
          if (pMode == 0) {
            // SYNC
            try {
              beginResponse(database, network, session);
              response.read(database, network, session);
            } finally {
              endResponse(network);
            }
            ret = response;
            connectionManager.release(network);
          } else {
            if (pMode == 1) {
              // ASYNC
              asynchExecutor.submit(
                  () -> {
                    try {
                      try {
                        beginResponse(database, network, session);
                        response.read(database, network, session);
                      } finally {
                        endResponse(network);
                      }
                      callback.call(recordId, response);
                      connectionManager.release(network);
                    } catch (Exception e) {
                      connectionManager.remove(network);
                      LogManager.instance().error(this, "Exception on async query", e);
                    } catch (Error e) {
                      connectionManager.remove(network);
                      LogManager.instance().error(this, "Exception on async query", e);
                      throw e;
                    }
                  });
            } else {
              // NO RESPONSE
              connectionManager.release(network);
            }
          }
          return ret;
        },
        errorMessage, retry);
  }

  public <T extends OBinaryResponse> T networkOperationRetryTimeout(
      DatabaseSessionRemote database, final OBinaryRequest<T> request, final String errorMessage,
      int retry, int timeout) {
    return baseNetworkOperation(database,
        (network, session) -> {
          try {
            try {
              network.beginRequest(request.getCommand(), session);
              request.write(database, network, session);
            } finally {
              network.endRequest();
            }
          } catch (IOException e) {
            if (network.isConnected()) {
              LogManager.instance().warn(this, "Error Writing request on the network", e);
            }
            throw new NotSendRequestException("Cannot send request on this channel");
          }

          int prev = network.getSocketTimeout();
          T response = request.createResponse();
          try {
            if (timeout > 0) {
              network.setSocketTimeout(timeout);
            }
            beginResponse(database, network, session);
            response.read(database, network, session);
          } finally {
            endResponse(network);
            if (timeout > 0) {
              network.setSocketTimeout(prev);
            }
          }
          connectionManager.release(network);
          return response;
        },
        errorMessage, retry);
  }

  public <T extends OBinaryResponse> T networkOperationNoRetry(
      DatabaseSessionRemote database, final OBinaryRequest<T> request,
      final String errorMessage) {
    return networkOperationRetryTimeout(database, request, errorMessage, 0, 0);
  }

  public <T extends OBinaryResponse> T networkOperation(
      DatabaseSessionRemote database, final OBinaryRequest<T> request,
      final String errorMessage) {
    return networkOperationRetryTimeout(database, request, errorMessage, connectionRetry, 0);
  }

  public <T> T baseNetworkOperation(
      DatabaseSessionRemote remoteSession, final OStorageRemoteOperation<T> operation,
      final String errorMessage, int retry) {
    OStorageRemoteSession session = getCurrentSession(remoteSession);
    if (session.commandExecuting) {
      throw new DatabaseException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use"
              + " a different connection");
    }

    String serverUrl = null;
    do {
      SocketChannelBinaryAsynchClient network = null;

      if (serverUrl == null) {
        serverUrl = getNextAvailableServerURL(false, session);
      }

      do {
        try {
          network = getNetwork(serverUrl);
        } catch (BaseException e) {
          if (session.isStickToSession()) {
            throw e;
          } else {
            serverUrl = useNewServerURL(remoteSession, serverUrl);
            if (serverUrl == null) {
              throw e;
            }
          }
        }
      } while (network == null);

      try {
        session.commandExecuting = true;

        // In case i do not have a token or i'm switching between server i've to execute a open
        // operation.
        OStorageRemoteNodeSession nodeSession = session.getServerSession(network.getServerURL());
        if (nodeSession == null || !nodeSession.isValid() && !session.isStickToSession()) {
          if (nodeSession != null) {
            session.removeServerSession(nodeSession.getServerURL());
          }
          openRemoteDatabase(remoteSession, network);
          if (!network.tryLock()) {
            continue;
          }
        }

        return operation.execute(network, session);
      } catch (NotSendRequestException e) {
        connectionManager.remove(network);
        serverUrl = null;
      } catch (DistributedRedirectException e) {
        connectionManager.release(network);
        LogManager.instance()
            .debug(
                this,
                "Redirecting the request from server '%s' to the server '%s' because %s",
                e,
                e.getFromServer(),
                e.toString(),
                e.getMessage());

        // RECONNECT TO THE SERVER SUGGESTED IN THE EXCEPTION
        serverUrl = e.getToServerAddress();
      } catch (ModificationOperationProhibitedException mope) {
        connectionManager.release(network);
        handleDBFreeze();
        serverUrl = null;
      } catch (TokenException | TokenSecurityException e) {
        connectionManager.release(network);
        session.removeServerSession(network.getServerURL());

        if (session.isStickToSession()) {
          retry--;
          if (retry <= 0) {
            throw BaseException.wrapException(new StorageException(errorMessage), e);
          } else {
            LogManager.instance()
                .warn(
                    this,
                    "Caught Network I/O errors on %s, trying an automatic reconnection... (error:"
                        + " %s)",
                    network.getServerURL(),
                    e.getMessage());
            LogManager.instance().debug(this, "I/O error stack: ", e);

            connectionManager.remove(network);
            try {
              Thread.sleep(connectionRetryDelay);
            } catch (java.lang.InterruptedException e1) {
              LogManager.instance()
                  .error(this, "Exception was suppressed, original exception is ", e);
              throw BaseException.wrapException(new ThreadInterruptedException(e1.getMessage()),
                  e1);
            }
          }
        }

        serverUrl = null;
      } catch (OfflineNodeException e) {
        connectionManager.release(network);
        // Remove the current url because the node is offline
        this.serverURLs.remove(serverUrl);
        for (OStorageRemoteSession activeSession : sessions) {
          // Not thread Safe ...
          activeSession.removeServerSession(serverUrl);
        }
        serverUrl = null;
      } catch (IOException | YTIOException e) {
        LogManager.instance()
            .warn(
                this,
                "Caught Network I/O errors on %s, trying an automatic reconnection... (error: %s)",
                network.getServerURL(),
                e.getMessage());
        LogManager.instance().debug(this, "I/O error stack: ", e);
        connectionManager.remove(network);
        if (--retry <= 0) {
          throw BaseException.wrapException(new YTIOException(e.getMessage()), e);
        } else {
          try {
            Thread.sleep(connectionRetryDelay);
          } catch (java.lang.InterruptedException e1) {
            LogManager.instance()
                .error(this, "Exception was suppressed, original exception is ", e);
            throw BaseException.wrapException(new ThreadInterruptedException(e1.getMessage()), e1);
          }
        }
        serverUrl = null;
      } catch (BaseException e) {
        connectionManager.release(network);
        throw e;
      } catch (Exception e) {
        connectionManager.release(network);
        throw BaseException.wrapException(new StorageException(errorMessage), e);
      } finally {
        session.commandExecuting = false;
      }
    } while (true);
  }

  public boolean isAssigningClusterIds() {
    return false;
  }

  /**
   * Supported only in embedded storage. Use <code>SELECT FROM metadata:storage</code> instead.
   */
  public String getCreatedAtVersion() {
    throw new UnsupportedOperationException(
        "Supported only in embedded storage. Use 'SELECT FROM metadata:storage' instead.");
  }

  public int getSessionId(DatabaseSessionRemote database) {
    OStorageRemoteSession session = getCurrentSession(database);
    return session != null ? session.getSessionId() : -1;
  }

  public void open(
      DatabaseSessionInternal db, final String iUserName, final String iUserPassword,
      final ContextConfiguration conf) {
    var remoteDb = (DatabaseSessionRemote) db;
    addUser();
    try {
      OStorageRemoteSession session = getCurrentSession(remoteDb);
      if (status == STATUS.CLOSED
          || !iUserName.equals(session.connectionUserName)
          || !iUserPassword.equals(session.connectionUserPassword)
          || session.sessions.isEmpty()) {

        CredentialInterceptor ci = SecurityManager.instance().newCredentialInterceptor();

        if (ci != null) {
          ci.intercept(getURL(), iUserName, iUserPassword);
          session.connectionUserName = ci.getUsername();
          session.connectionUserPassword = ci.getPassword();
        } else {
          // Do Nothing
          session.connectionUserName = iUserName;
          session.connectionUserPassword = iUserPassword;
        }

        String strategy = conf.getValueAsString(GlobalConfiguration.CLIENT_CONNECTION_STRATEGY);
        if (strategy != null) {
          connectionStrategy = CONNECTION_STRATEGY.valueOf(strategy.toUpperCase(Locale.ENGLISH));
        }

        openRemoteDatabase(remoteDb);

        reload(db);
        initPush(remoteDb, session);

        componentsFactory = new CurrentStorageComponentsFactory(configuration);

      } else {
        reopenRemoteDatabase(remoteDb);
      }
    } catch (Exception e) {
      removeUser();
      if (e instanceof RuntimeException)
      // PASS THROUGH
      {
        throw (RuntimeException) e;
      } else {
        throw BaseException.wrapException(
            new StorageException("Cannot open the remote storage: " + name), e);
      }
    }
  }

  public SBTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public void reload(DatabaseSessionInternal database) {
    OReloadResponse37 res =
        networkOperation((DatabaseSessionRemote) database, new OReloadRequest37(),
            "error loading storage configuration");
    final StorageConfiguration storageConfiguration =
        new StorageConfigurationRemote(
            RecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            res.getPayload(),
            clientConfiguration);

    updateStorageConfiguration(storageConfiguration);
  }

  public void create(ContextConfiguration contextConfiguration) {
    throw new UnsupportedOperationException(
        "Cannot create a database in a remote server. Please use the console or the OServerAdmin"
            + " class.");
  }

  public boolean exists() {
    throw new UnsupportedOperationException(
        "Cannot check the existence of a database in a remote server. Please use the console or the"
            + " OServerAdmin class.");
  }

  public void close(DatabaseSessionInternal database, final boolean iForce) {
    if (status == STATUS.CLOSED) {
      return;
    }

    final OStorageRemoteSession session = getCurrentSession((DatabaseSessionRemote) database);
    if (session != null) {
      final Collection<OStorageRemoteNodeSession> nodes = session.getAllServerSessions();
      if (!nodes.isEmpty()) {
        ContextConfiguration config = null;
        if (configuration != null) {
          config = configuration.getContextConfiguration();
        }
        session.closeAllSessions(connectionManager, config);
        if (!checkForClose(iForce)) {
          return;
        }
      } else {
        if (!iForce) {
          return;
        }
      }
      sessions.remove(session);
      checkForClose(iForce);
    }
  }

  public void shutdown() {
    if (status == STATUS.CLOSED || status == STATUS.CLOSING) {
      return;
    }

    // FROM HERE FORWARD COMPLETELY CLOSE THE STORAGE
    for (Entry<Integer, OLiveQueryClientListener> listener : liveQueryListener.entrySet()) {
      listener.getValue().onEnd();
    }
    liveQueryListener.clear();

    stateLock.writeLock().lock();
    try {
      if (status == STATUS.CLOSED) {
        return;
      }

      status = STATUS.CLOSING;
      close(null, true);
    } finally {
      stateLock.writeLock().unlock();
    }
    if (pushThread != null) {
      pushThread.shutdown();
      try {
        pushThread.join();
      } catch (java.lang.InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    stateLock.writeLock().lock();
    try {
      // CLOSE ALL THE SOCKET POOLS
      sbTreeCollectionManager.close();

      status = STATUS.CLOSED;

    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private boolean checkForClose(final boolean force) {
    if (status == STATUS.CLOSED) {
      return false;
    }

    if (status == STATUS.CLOSED) {
      return false;
    }

    final int remainingUsers = getUsers() > 0 ? removeUser() : 0;

    return force || remainingUsers == 0;
  }

  public int getUsers() {
    return users.get();
  }

  public int addUser() {
    return users.incrementAndGet();
  }

  public int removeUser() {
    if (users.get() < 1) {
      throw new IllegalStateException(
          "Cannot remove user of the remote storage '" + this + "' because no user is using it");
    }

    return users.decrementAndGet();
  }

  public void delete() {
    throw new UnsupportedOperationException(
        "Cannot delete a database in a remote server. Please use the console or the OServerAdmin"
            + " class.");
  }

  public Set<String> getClusterNames() {
    stateLock.readLock().lock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      stateLock.readLock().unlock();
    }
  }

  private void updateCollectionsFromChanges(
      final SBTreeCollectionManager collectionManager,
      final Map<UUID, BonsaiCollectionPointer> changes) {
    if (collectionManager != null) {
      for (Entry<UUID, BonsaiCollectionPointer> coll : changes.entrySet()) {
        collectionManager.updateCollectionPointer(coll.getKey(), coll.getValue());
      }
      if (RecordSerializationContext.getDepth() <= 1) {
        collectionManager.clearPendingCollections();
      }
    }
  }

  public RecordMetadata getRecordMetadata(DatabaseSessionInternal session, final RID rid) {
    OGetRecordMetadataRequest request = new OGetRecordMetadataRequest(rid);
    OGetRecordMetadataResponse response =
        networkOperation((DatabaseSessionRemote) session, request,
            "Error on record metadata read " + rid);

    return response.getMetadata();
  }

  @Override
  public boolean recordExists(DatabaseSessionInternal session, RID rid) {
    var remoteSession = (DatabaseSessionRemote) session;
    if (getCurrentSession(remoteSession).commandExecuting)
    // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
    {
      throw new IllegalStateException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use"
              + " a different connection");
    }

    var request = new ORecordExistsRequest(rid);
    var response = networkOperation(remoteSession, request,
        "Error on record existence check " + rid);

    return response.isRecordExists();
  }

  public @Nonnull RawBuffer readRecord(
      DatabaseSessionInternal session, final RecordId iRid,
      final boolean iIgnoreCache,
      boolean prefetchRecords,
      final RecordCallback<RawBuffer> iCallback) {

    var remoteSession = (DatabaseSessionRemote) session;
    if (getCurrentSession(remoteSession).commandExecuting)
    // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
    {
      throw new IllegalStateException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use"
              + " a different connection");
    }

    OReadRecordRequest request = new OReadRecordRequest(iIgnoreCache, iRid, null, false);
    OReadRecordResponse response = networkOperation(remoteSession, request,
        "Error on read record " + iRid);

    return response.getResult();
  }

  public String incrementalBackup(DatabaseSessionInternal session, final String backupDirectory,
      CallableFunction<Void, Void> started) {
    OIncrementalBackupRequest request = new OIncrementalBackupRequest(backupDirectory);
    OIncrementalBackupResponse response =
        networkOperationNoRetry((DatabaseSessionRemote) session, request,
            "Error on incremental backup");
    return response.getFileName();
  }

  public boolean supportIncremental() {
    // THIS IS FALSE HERE THOUGH WE HAVE SOME SUPPORT FOR SOME SPECIFIC CASES FROM REMOTE
    return false;
  }

  public void fullIncrementalBackup(final OutputStream stream)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "This operations is part of internal API and is not supported in remote storage");
  }

  public void restoreFromIncrementalBackup(DatabaseSessionInternal session,
      final String filePath) {
    throw new UnsupportedOperationException(
        "This operations is part of internal API and is not supported in remote storage");
  }

  public void restoreFullIncrementalBackup(DatabaseSessionInternal session,
      final InputStream stream)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "This operations is part of internal API and is not supported in remote storage");
  }

  public boolean cleanOutRecord(
      DatabaseSessionInternal session, final RecordId recordId,
      final int recordVersion,
      final int iMode,
      final RecordCallback<Boolean> callback) {

    RecordCallback<OCleanOutRecordResponse> realCallback = null;
    if (callback != null) {
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());
    }

    final OCleanOutRecordRequest request = new OCleanOutRecordRequest(recordVersion, recordId);
    final OCleanOutRecordResponse response =
        asyncNetworkOperationNoRetry((DatabaseSessionRemote) session,
            request, iMode, recordId, realCallback, "Error on delete record " + recordId);
    Boolean result = null;
    if (response != null) {
      result = response.getResult();
    }
    return result != null ? result : false;
  }

  public List<String> backup(
      OutputStream out,
      Map<String, Object> options,
      Callable<Object> callable,
      final CommandOutputListener iListener,
      int compressionLevel,
      int bufferSize)
      throws IOException {
    throw new UnsupportedOperationException(
        "backup is not supported against remote storage. Open the database with plocal or use the"
            + " incremental backup in the Enterprise Edition");
  }

  public void restore(
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      final CommandOutputListener iListener)
      throws IOException {
    throw new UnsupportedOperationException(
        "restore is not supported against remote storage. Open the database with plocal or use"
            + " Enterprise Edition");
  }

  public ContextConfiguration getClientConfiguration() {
    return clientConfiguration;
  }

  public long count(DatabaseSessionInternal session, final int iClusterId) {
    return count(session, new int[]{iClusterId});
  }

  public long count(DatabaseSessionInternal session, int iClusterId, boolean countTombstones) {
    return count(session, new int[]{iClusterId}, countTombstones);
  }

  public long[] getClusterDataRange(DatabaseSessionInternal session, final int iClusterId) {
    OGetClusterDataRangeRequest request = new OGetClusterDataRangeRequest(iClusterId);
    OGetClusterDataRangeResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request, "Error on getting last entry position count in cluster: " + iClusterId);
    return response.getPos();
  }

  public PhysicalPosition[] higherPhysicalPositions(
      DatabaseSessionInternal session, final int iClusterId,
      final PhysicalPosition iClusterPosition) {
    OHigherPhysicalPositionsRequest request =
        new OHigherPhysicalPositionsRequest(iClusterId, iClusterPosition);

    OHigherPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
    return response.getNextPositions();
  }

  public PhysicalPosition[] ceilingPhysicalPositions(
      DatabaseSessionInternal session, final int clusterId,
      final PhysicalPosition physicalPosition) {

    OCeilingPhysicalPositionsRequest request =
        new OCeilingPhysicalPositionsRequest(clusterId, physicalPosition);

    OCeilingPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public PhysicalPosition[] lowerPhysicalPositions(
      DatabaseSessionInternal session, final int iClusterId,
      final PhysicalPosition physicalPosition) {
    OLowerPhysicalPositionsRequest request =
        new OLowerPhysicalPositionsRequest(physicalPosition, iClusterId);
    OLowerPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
    return response.getPreviousPositions();
  }

  public PhysicalPosition[] floorPhysicalPositions(
      DatabaseSessionInternal session, final int clusterId,
      final PhysicalPosition physicalPosition) {
    OFloorPhysicalPositionsRequest request =
        new OFloorPhysicalPositionsRequest(physicalPosition, clusterId);
    OFloorPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public long getSize(DatabaseSessionInternal session) {
    OGetSizeRequest request = new OGetSizeRequest();
    OGetSizeResponse response = networkOperation((DatabaseSessionRemote) session, request,
        "Error on read database size");
    return response.getSize();
  }

  public long countRecords(DatabaseSessionInternal session) {
    OCountRecordsRequest request = new OCountRecordsRequest();
    OCountRecordsResponse response =
        networkOperation((DatabaseSessionRemote) session, request,
            "Error on read database record count");
    return response.getCountRecords();
  }

  public long count(DatabaseSessionInternal session, final int[] iClusterIds) {
    return count(session, iClusterIds, false);
  }

  public long count(DatabaseSessionInternal session, final int[] iClusterIds,
      final boolean countTombstones) {
    OCountRequest request = new OCountRequest(iClusterIds, countTombstones);
    OCountResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request, "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
    return response.getCount();
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(DatabaseSessionInternal database, final CommandRequestText iCommand) {

    final boolean live = iCommand instanceof LiveQuery;
    final boolean asynch =
        iCommand instanceof CommandRequestAsynch
            && ((CommandRequestAsynch) iCommand).isAsynchronous();

    OCommandRequest request = new OCommandRequest(database, asynch, iCommand, live);
    OCommandResponse response =
        networkOperation((DatabaseSessionRemote) database, request,
            "Error on executing command: " + iCommand);
    return response.getResult();
  }

  public void stickToSession(DatabaseSessionRemote database) {
    OStorageRemoteSession session = getCurrentSession(database);
    session.stickToSession();
  }

  public void unstickToSession(DatabaseSessionRemote database) {
    OStorageRemoteSession session = getCurrentSession(database);
    session.unStickToSession();
  }

  public ORemoteQueryResult query(DatabaseSessionRemote db, String query, Object[] args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(db,
            "sql", query, args, OQueryRequest.QUERY, db.getSerializer(), recordsPerPage);
    OQueryResponse response = networkOperation(db, request, "Error on executing command: " + query);
    try {
      if (response.isTxChanges()) {
        fetchTransaction(db);
      }

      RemoteResultSet rs =
          new RemoteResultSet(
              db,
              response.getQueryId(),
              response.getResult(),
              response.getExecutionPlan(),
              response.getQueryStats(),
              response.isHasNextPage());

      if (response.isHasNextPage()) {
        stickToSession(db);
      } else {
        db.queryClosed(response.getQueryId());
      }
      return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public ORemoteQueryResult query(DatabaseSessionRemote db, String query, Map args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(db,
            "sql", query, args, OQueryRequest.QUERY, db.getSerializer(), recordsPerPage);
    OQueryResponse response = networkOperation(db, request, "Error on executing command: " + query);

    try {
      if (response.isTxChanges()) {
        fetchTransaction(db);
      }

      RemoteResultSet rs =
          new RemoteResultSet(
              db,
              response.getQueryId(),
              response.getResult(),
              response.getExecutionPlan(),
              response.getQueryStats(),
              response.isHasNextPage());

      if (response.isHasNextPage()) {
        stickToSession(db);
      } else {
        db.queryClosed(response.getQueryId());
      }
      return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public ORemoteQueryResult command(DatabaseSessionRemote db, String query, Object[] args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(db,
            "sql", query, args, OQueryRequest.COMMAND, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(db, request, "Error on executing command: " + query);

    try {
      if (response.isTxChanges()) {
        fetchTransaction(db);
      }

      RemoteResultSet rs =
          new RemoteResultSet(
              db,
              response.getQueryId(),
              response.getResult(),
              response.getExecutionPlan(),
              response.getQueryStats(),
              response.isHasNextPage());
      if (response.isHasNextPage()) {
        stickToSession(db);
      } else {
        db.queryClosed(response.getQueryId());
      }
      return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public ORemoteQueryResult command(DatabaseSessionRemote db, String query, Map args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(db,
            "sql", query, args, OQueryRequest.COMMAND, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(db, request, "Error on executing command: " + query);
    try {
      if (response.isTxChanges()) {
        fetchTransaction(db);
      }

      RemoteResultSet rs =
          new RemoteResultSet(
              db,
              response.getQueryId(),
              response.getResult(),
              response.getExecutionPlan(),
              response.getQueryStats(),
              response.isHasNextPage());
      if (response.isHasNextPage()) {
        stickToSession(db);
      } else {
        db.queryClosed(response.getQueryId());
      }
      return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public ORemoteQueryResult execute(
      DatabaseSessionRemote db, String language, String query, Object[] args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(db,
            language, query, args, OQueryRequest.EXECUTE, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(db, request, "Error on executing command: " + query);

    try {
      if (response.isTxChanges()) {
        fetchTransaction(db);
      }

      RemoteResultSet rs =
          new RemoteResultSet(
              db,
              response.getQueryId(),
              response.getResult(),
              response.getExecutionPlan(),
              response.getQueryStats(),
              response.isHasNextPage());

      if (response.isHasNextPage()) {
        stickToSession(db);
      } else {
        db.queryClosed(response.getQueryId());
      }

      return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public ORemoteQueryResult execute(
      DatabaseSessionRemote db, String language, String query, Map args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(db,
            language, query, args, OQueryRequest.EXECUTE, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(db, request, "Error on executing command: " + query);

    try {
      if (response.isTxChanges()) {
        fetchTransaction(db);
      }

      RemoteResultSet rs =
          new RemoteResultSet(
              db,
              response.getQueryId(),
              response.getResult(),
              response.getExecutionPlan(),
              response.getQueryStats(),
              response.isHasNextPage());
      if (response.isHasNextPage()) {
        stickToSession(db);
      } else {
        db.queryClosed(response.getQueryId());
      }
      return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public void closeQuery(DatabaseSessionRemote database, String queryId) {
    unstickToSession(database);
    OCloseQueryRequest request = new OCloseQueryRequest(queryId);
    networkOperation(database, request, "Error closing query: " + queryId);
  }

  public void fetchNextPage(DatabaseSessionRemote database, RemoteResultSet rs) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryNextPageRequest request = new OQueryNextPageRequest(rs.getQueryId(), recordsPerPage);
    OQueryResponse response =
        networkOperation(database, request,
            "Error on fetching next page for statment: " + rs.getQueryId());

    rs.fetched(
        response.getResult(),
        response.isHasNextPage(),
        response.getExecutionPlan(),
        response.getQueryStats());
    if (!response.isHasNextPage()) {
      unstickToSession(database);
      database.queryClosed(response.getQueryId());
    }
  }

  public List<RecordOperation> commit(final TransactionOptimistic iTx) {
    var remoteSession = (DatabaseSessionRemote) iTx.getDatabase();
    unstickToSession(remoteSession);

    final OCommit38Request request =
        new OCommit38Request(iTx.getDatabase(),
            iTx.getId(), true, true, iTx.getRecordOperations(), Collections.emptyMap());

    final OCommit37Response response = networkOperationNoRetry(remoteSession, request,
        "Error on commit");

    // two pass iteration, we update cluster ids, and then update positions
    for (var updatedPair : response.getUpdatedRids()) {
      iTx.updateIdentityAfterCommit(updatedPair.first(), updatedPair.second());
    }

    updateCollectionsFromChanges(
        iTx.getDatabase().getSbTreeCollectionManager(), response.getCollectionChanges());
    // SET ALL THE RECORDS AS UNDIRTY
    for (RecordOperation txEntry : iTx.getRecordOperations()) {
      RecordInternal.unsetDirty(txEntry.record);
    }

    return null;
  }

  public void rollback(TransactionInternal iTx) {
    var remoteSession = (DatabaseSessionRemote) iTx.getDatabase();
    try {
      if (((TransactionOptimistic) iTx).isStartedOnServer()
          && !getCurrentSession(remoteSession).getAllServerSessions().isEmpty()) {
        ORollbackTransactionRequest request = new ORollbackTransactionRequest(iTx.getId());
        networkOperation(remoteSession, request,
            "Error on fetching next page for statment: " + request);
      }
    } finally {
      unstickToSession(remoteSession);
    }
  }

  public int getClusterIdByName(final String iClusterName) {
    stateLock.readLock().lock();
    try {

      if (iClusterName == null) {
        return -1;
      }

      if (Character.isDigit(iClusterName.charAt(0))) {
        return Integer.parseInt(iClusterName);
      }

      final StorageCluster cluster = clusterMap.get(iClusterName.toLowerCase(Locale.ENGLISH));
      if (cluster == null) {
        return -1;
      }

      return cluster.getId();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  public int addCluster(DatabaseSessionInternal database, final String iClusterName,
      final Object... iArguments) {
    return addCluster(database, iClusterName, -1);
  }

  public int addCluster(DatabaseSessionInternal database, final String iClusterName,
      final int iRequestedId) {
    OAddClusterRequest request = new OAddClusterRequest(iRequestedId, iClusterName);
    OAddClusterResponse response = networkOperationNoRetry((DatabaseSessionRemote) database,
        request,
        "Error on add new cluster");
    addNewClusterToConfiguration(response.getClusterId(), iClusterName);
    return response.getClusterId();
  }

  public String getClusterNameById(int clusterId) {
    stateLock.readLock().lock();
    try {
      if (clusterId < 0 || clusterId >= clusters.length) {
        throw new StorageException("Cluster with id " + clusterId + " does not exist");
      }

      final StorageCluster cluster = clusters[clusterId];
      return cluster.getName();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public long getClusterRecordsSizeById(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public long getClusterRecordsSizeByName(String clusterName) {
    throw new UnsupportedOperationException();
  }

  public String getClusterRecordConflictStrategy(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public String getClusterEncryption(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public boolean isSystemCluster(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public long getLastClusterPosition(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public long getClusterNextPosition(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public PaginatedCluster.RECORD_STATUS getRecordStatus(RID rid) {
    throw new UnsupportedOperationException();
  }

  public boolean dropCluster(DatabaseSessionInternal database, final int iClusterId) {

    ODropClusterRequest request = new ODropClusterRequest(iClusterId);

    ODropClusterResponse response =
        networkOperationNoRetry((DatabaseSessionRemote) database, request,
            "Error on removing of cluster");
    if (response.getResult()) {
      removeClusterFromConfiguration(iClusterId);
    }
    return response.getResult();
  }

  public String getClusterName(DatabaseSessionInternal database, int clusterId) {
    stateLock.readLock().lock();
    try {
      if (clusterId == RID.CLUSTER_ID_INVALID)
      // GET THE DEFAULT CLUSTER
      {
        clusterId = defaultClusterId;
      }

      if (clusterId >= clusters.length) {
        stateLock.readLock().unlock();
        reload(database);
        stateLock.readLock().lock();
      }

      if (clusterId < clusters.length) {
        return clusters[clusterId].getName();
      }
    } finally {
      stateLock.readLock().unlock();
    }

    throw new StorageException("Cluster " + clusterId + " is absent in storage.");
  }

  public boolean setClusterAttribute(int id, StorageCluster.ATTRIBUTES attribute, Object value) {
    return false;
  }

  public void removeClusterFromConfiguration(int iClusterId) {
    stateLock.writeLock().lock();
    try {
      // If this is false the clusters may be already update by a push
      if (clusters.length > iClusterId && clusters[iClusterId] != null) {
        // Remove cluster locally waiting for the push
        final StorageCluster cluster = clusters[iClusterId];
        clusters[iClusterId] = null;
        clusterMap.remove(cluster.getName());
        ((StorageConfigurationRemote) configuration)
            .dropCluster(iClusterId); // endResponse must be called before this line, which
        // call updateRecord
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  public void synch() {
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    stateLock.readLock().lock();
    try {

      if (iClusterId >= clusters.length) {
        return null;
      }

      final StorageCluster cluster = clusters[iClusterId];
      return cluster != null ? cluster.getName() : null;

    } finally {
      stateLock.readLock().unlock();
    }
  }

  public int getClusterMap() {
    stateLock.readLock().lock();
    try {
      return clusterMap.size();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public Collection<StorageCluster> getClusterInstances() {
    stateLock.readLock().lock();
    try {

      return Arrays.asList(clusters);

    } finally {
      stateLock.readLock().unlock();
    }
  }

  public long getVersion() {
    throw new UnsupportedOperationException("getVersion");
  }

  public EntityImpl getClusterConfiguration() {
    return clusterConfiguration;
  }

  /**
   * Ends the request and unlock the write lock
   */
  public void endRequest(final SocketChannelBinaryAsynchClient iNetwork) throws IOException {
    if (iNetwork == null) {
      return;
    }

    iNetwork.flush();
    iNetwork.releaseWriteLock();
  }

  /**
   * End response reached: release the channel in the pool to being reused
   */
  public void endResponse(final SocketChannelBinaryAsynchClient iNetwork) throws IOException {
    iNetwork.endResponse();
  }

  public boolean isRemote() {
    return true;
  }

  public boolean isPermanentRequester() {
    return false;
  }

  public RecordConflictStrategy getRecordConflictStrategy() {
    throw new UnsupportedOperationException("getRecordConflictStrategy");
  }

  public void setConflictStrategy(final RecordConflictStrategy iResolver) {
    throw new UnsupportedOperationException("setConflictStrategy");
  }

  public String getURL() {
    return EngineRemote.NAME + ":" + url;
  }

  public int getClusters() {
    stateLock.readLock().lock();
    try {
      return clusterMap.size();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public String getType() {
    return EngineRemote.NAME;
  }

  public String getUserName(DatabaseSessionInternal database) {
    final OStorageRemoteSession session = getCurrentSession((DatabaseSessionRemote) database);
    if (session == null) {
      return null;
    }
    return session.connectionUserName;
  }

  protected void reopenRemoteDatabase(DatabaseSessionRemote database) {
    String currentURL = getCurrentServerURL(database);
    do {
      do {
        final SocketChannelBinaryAsynchClient network = getNetwork(currentURL);
        try {
          OStorageRemoteSession session = getCurrentSession(database);
          OStorageRemoteNodeSession nodeSession =
              session.getOrCreateServerSession(network.getServerURL());
          if (nodeSession == null || !nodeSession.isValid()) {
            openRemoteDatabase(database, network);
            return;
          } else {
            OReopenRequest request = new OReopenRequest();

            try {
              network.writeByte(request.getCommand());
              network.writeInt(nodeSession.getSessionId());
              network.writeBytes(nodeSession.getToken());
              request.write(null, network, session);
            } finally {
              endRequest(network);
            }

            OReopenResponse response = request.createResponse();
            try {
              byte[] newToken = network.beginResponse(database, nodeSession.getSessionId(), true);
              response.read(database, network, session);
              if (newToken != null && newToken.length > 0) {
                nodeSession.setSession(response.getSessionId(), newToken);
              } else {
                nodeSession.setSession(response.getSessionId(), nodeSession.getToken());
              }
              LogManager.instance()
                  .debug(
                      this,
                      "Client connected to %s with session id=%d",
                      network.getServerURL(),
                      response.getSessionId());
              return;
            } finally {
              endResponse(network);
              connectionManager.release(network);
            }
          }
        } catch (YTIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          LogManager.instance().error(this, "Cannot open database with url " + currentURL, e);
        } catch (OfflineNodeException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          LogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);
        } catch (SecurityException ex) {
          LogManager.instance().debug(this, "Invalidate token for url=%s", ex, currentURL);
          OStorageRemoteSession session = getCurrentSession(database);
          session.removeServerSession(currentURL);

          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception e) {
              // IGNORE ANY EXCEPTION
              LogManager.instance()
                  .debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        } catch (BaseException e) {
          connectionManager.release(network);
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (Exception e) {
          LogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception ex) {
              // IGNORE ANY EXCEPTION
              LogManager.instance()
                  .debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        }
      } while (connectionManager.getAvailableConnections(currentURL) > 0);

      currentURL = useNewServerURL(database, currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    serverURLs.reloadOriginalURLs();

    throw new StorageException(
        "Cannot create a connection to remote server address(es): " + serverURLs.getUrls());
  }

  protected void openRemoteDatabase(DatabaseSessionRemote database) throws IOException {
    final String currentURL = getNextAvailableServerURL(true, getCurrentSession(database));
    openRemoteDatabase(database, currentURL);
  }

  public void openRemoteDatabase(DatabaseSessionRemote database,
      SocketChannelBinaryAsynchClient network) throws IOException {

    OStorageRemoteSession session = getCurrentSession(database);
    OStorageRemoteNodeSession nodeSession =
        session.getOrCreateServerSession(network.getServerURL());
    OOpen37Request request =
        new OOpen37Request(name, session.connectionUserName, session.connectionUserPassword);
    try {
      network.writeByte(request.getCommand());
      network.writeInt(nodeSession.getSessionId());
      network.writeBytes(null);
      request.write(null, network, session);
    } finally {
      endRequest(network);
    }
    final int sessionId;
    OOpen37Response response = request.createResponse();
    try {
      network.beginResponse(database, nodeSession.getSessionId(), true);
      response.read(database, network, session);
    } finally {
      endResponse(network);
      connectionManager.release(network);
    }
    sessionId = response.getSessionId();
    byte[] token = response.getSessionToken();
    if (token.length == 0) {
      token = null;
    }

    nodeSession.setSession(sessionId, token);

    LogManager.instance()
        .debug(
            this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);

    // READ CLUSTER CONFIGURATION
    // updateClusterConfiguration(network.getServerURL(),
    // response.getDistributedConfiguration());

    // This need to be protected by a lock for now, let's see in future
    stateLock.writeLock().lock();
    try {
      status = STATUS.OPEN;
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private void initPush(DatabaseSessionRemote database, OStorageRemoteSession session) {
    if (pushThread == null) {
      stateLock.writeLock().lock();
      try {
        if (pushThread == null) {
          pushThread =
              new OStorageRemotePushThread(
                  this,
                  getCurrentServerURL(database),
                  connectionRetryDelay,
                  configuration
                      .getContextConfiguration()
                      .getValueAsLong(GlobalConfiguration.NETWORK_REQUEST_TIMEOUT));
          pushThread.start();
          subscribeStorageConfiguration(session);
          subscribeDistributedConfiguration(session);
          subscribeSchema(session);
          subscribeIndexManager(session);
          subscribeFunctions(session);
          subscribeSequences(session);
        }
      } finally {
        stateLock.writeLock().unlock();
      }
    }
  }

  private void subscribeDistributedConfiguration(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeDistributedConfigurationRequest(), nodeSession);
  }

  private void subscribeStorageConfiguration(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeStorageConfigurationRequest(), nodeSession);
  }

  private void subscribeSchema(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeSchemaRequest(), nodeSession);
  }

  private void subscribeFunctions(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeFunctionsRequest(), nodeSession);
  }

  private void subscribeSequences(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeSequencesRequest(), nodeSession);
  }

  private void subscribeIndexManager(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeIndexManagerRequest(), nodeSession);
  }

  protected void openRemoteDatabase(DatabaseSessionRemote database, String currentURL) {
    do {
      do {
        SocketChannelBinaryAsynchClient network = null;
        try {
          network = getNetwork(currentURL);
          openRemoteDatabase(database, network);
          return;
        } catch (DistributedRedirectException e) {
          connectionManager.release(network);
          // RECONNECT TO THE SERVER SUGGESTED IN THE EXCEPTION
          currentURL = e.getToServerAddress();
        } catch (ModificationOperationProhibitedException mope) {
          connectionManager.release(network);
          handleDBFreeze();
          currentURL = useNewServerURL(database, currentURL);
        } catch (OfflineNodeException e) {
          connectionManager.release(network);
          currentURL = useNewServerURL(database, currentURL);
        } catch (YTIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          LogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);

        } catch (BaseException e) {
          connectionManager.release(network);
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (IOException e) {
          if (network != null) {
            connectionManager.remove(network);
          }
        } catch (Exception e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }
          throw BaseException.wrapException(new StorageException(e.getMessage()), e);
        }
      } while (connectionManager.getReusableConnections(currentURL) > 0);

      if (currentURL != null) {
        currentURL = useNewServerURL(database, currentURL);
      }

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    serverURLs.reloadOriginalURLs();

    throw new StorageException(
        "Cannot create a connection to remote server address(es): " + serverURLs.getUrls());
  }

  protected String useNewServerURL(DatabaseSessionRemote database, final String iUrl) {
    int pos = iUrl.indexOf('/');
    if (pos >= iUrl.length() - 1)
    // IGNORE ENDING /
    {
      pos = -1;
    }

    final String url = pos > -1 ? iUrl.substring(0, pos) : iUrl;
    String newUrl = serverURLs.removeAndGet(url);
    OStorageRemoteSession session = getCurrentSession(database);
    if (session != null) {
      session.currentUrl = newUrl;
      session.serverURLIndex = 0;
    }
    return newUrl;
  }

  /**
   * Parse the URLs. Multiple URLs must be separated by semicolon (;)
   */
  protected void parseServerURLs() {
    this.name = serverURLs.parseServerUrls(this.url, clientConfiguration);
  }

  /**
   * Acquire a network channel from the pool. Don't lock the write stream since the connection usage
   * is exclusive.
   *
   * @param iCommand id. Ids described at {@link ChannelBinaryProtocol}
   * @return connection to server
   */
  public SocketChannelBinaryAsynchClient beginRequest(
      final SocketChannelBinaryAsynchClient network, final byte iCommand,
      OStorageRemoteSession session)
      throws IOException {
    network.beginRequest(iCommand, session);
    return network;
  }

  protected String getNextAvailableServerURL(
      boolean iIsConnectOperation, OStorageRemoteSession session) {

    ContextConfiguration config = null;
    if (configuration != null) {
      config = configuration.getContextConfiguration();
    }
    return serverURLs.getNextAvailableServerURL(
        iIsConnectOperation, session, config, connectionStrategy);
  }

  protected String getCurrentServerURL(DatabaseSessionRemote database) {
    return serverURLs.getServerURFromList(
        false, getCurrentSession(database), configuration.getContextConfiguration());
  }

  public SocketChannelBinaryAsynchClient getNetwork(final String iCurrentURL) {
    return getNetwork(iCurrentURL, connectionManager, clientConfiguration);
  }

  public static SocketChannelBinaryAsynchClient getNetwork(
      final String iCurrentURL,
      ORemoteConnectionManager connectionManager,
      ContextConfiguration config) {
    SocketChannelBinaryAsynchClient network;
    do {
      try {
        network = connectionManager.acquire(iCurrentURL, config);
      } catch (YTIOException cause) {
        throw cause;
      } catch (Exception cause) {
        throw BaseException.wrapException(
            new StorageException("Cannot open a connection to remote server: " + iCurrentURL),
            cause);
      }
      if (!network.tryLock()) {
        // CANNOT LOCK IT, MAYBE HASN'T BE CORRECTLY UNLOCKED BY PREVIOUS USER?
        LogManager.instance()
            .error(
                StorageRemote.class,
                "Removing locked network channel '%s' (connected=%s)...",
                null,
                iCurrentURL,
                network.isConnected());
        connectionManager.remove(network);
        network = null;
      }
    } while (network == null);
    return network;
  }

  public static void beginResponse(
      DatabaseSessionInternal db, SocketChannelBinaryAsynchClient iNetwork,
      OStorageRemoteSession session) throws IOException {
    OStorageRemoteNodeSession nodeSession = session.getServerSession(iNetwork.getServerURL());
    byte[] newToken = iNetwork.beginResponse(db, nodeSession.getSessionId(), true);
    if (newToken != null && newToken.length > 0) {
      nodeSession.setSession(nodeSession.getSessionId(), newToken);
    }
  }

  private boolean handleDBFreeze() {

    boolean retry;
    LogManager.instance()
        .warn(
            this,
            "DB is frozen will wait for "
                + clientConfiguration.getValue(GlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT)
                + " ms. and then retry.");
    retry = true;
    try {
      Thread.sleep(
          clientConfiguration.getValueAsInteger(
              GlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT));
    } catch (java.lang.InterruptedException ie) {
      retry = false;

      Thread.currentThread().interrupt();
    }
    return retry;
  }

  public void updateStorageConfiguration(StorageConfiguration storageConfiguration) {
    if (status != STATUS.OPEN) {
      return;
    }
    stateLock.writeLock().lock();
    try {
      if (status != STATUS.OPEN) {
        return;
      }
      this.configuration = storageConfiguration;
      final List<StorageClusterConfiguration> configClusters = storageConfiguration.getClusters();
      StorageCluster[] clusters = new StorageCluster[configClusters.size()];
      for (StorageClusterConfiguration clusterConfig : configClusters) {
        if (clusterConfig != null) {
          final StorageClusterRemote cluster = new StorageClusterRemote();
          String clusterName = clusterConfig.getName();
          final int clusterId = clusterConfig.getId();
          if (clusterName != null) {
            clusterName = clusterName.toLowerCase(Locale.ENGLISH);
            cluster.configure(clusterId, clusterName);
            if (clusterId >= clusters.length) {
              clusters = Arrays.copyOf(clusters, clusterId + 1);
            }
            clusters[clusterId] = cluster;
          }
        }
      }

      this.clusters = clusters;
      clusterMap.clear();
      for (int i = 0; i < clusters.length; ++i) {
        if (clusters[i] != null) {
          clusterMap.put(clusters[i].getName(), clusters[i]);
        }
      }
      final StorageCluster defaultCluster = clusterMap.get(Storage.CLUSTER_DEFAULT_NAME);
      if (defaultCluster != null) {
        defaultClusterId = clusterMap.get(Storage.CLUSTER_DEFAULT_NAME).getId();
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  @Nullable
  protected OStorageRemoteSession getCurrentSession(@Nullable DatabaseSessionRemote db) {
    if (db == null) {
      return null;
    }

    OStorageRemoteSession session = db.getSessionMetadata();
    if (session == null) {
      session = new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      sessions.add(session);
      db.setSessionMetadata(session);
    }

    return session;
  }

  public boolean isClosed(DatabaseSessionInternal database) {
    if (status == STATUS.CLOSED) {
      return true;
    }
    final OStorageRemoteSession session = getCurrentSession((DatabaseSessionRemote) database);
    if (session == null) {
      return false;
    }
    return session.isClosed();
  }

  public StorageRemote copy(
      final DatabaseSessionRemote source, final DatabaseSessionRemote dest) {
    DatabaseSessionInternal origin = null;
    if (DatabaseRecordThreadLocal.instance() != null) {
      origin = DatabaseRecordThreadLocal.instance().getIfDefined();
    }

    origin = DatabaseDocumentTxInternal.getInternal(origin);

    final OStorageRemoteSession session = source.getSessionMetadata();
    if (session != null) {
      // TODO:may run a session reopen
      final OStorageRemoteSession newSession =
          new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      newSession.connectionUserName = session.connectionUserName;
      newSession.connectionUserPassword = session.connectionUserPassword;
      dest.setSessionMetadata(newSession);
    }
    try {
      dest.activateOnCurrentThread();
      openRemoteDatabase(dest);
    } catch (IOException e) {
      LogManager.instance().error(this, "Error during database open", e);
    } finally {
      DatabaseRecordThreadLocal.instance().set(origin);
    }
    return this;
  }

  public void importDatabase(
      DatabaseSessionRemote database, final String options,
      final InputStream inputStream,
      final String name,
      final CommandOutputListener listener) {
    OImportRequest request = new OImportRequest(inputStream, options, name);

    OImportResponse response =
        networkOperationRetryTimeout(database,
            request,
            "Error sending import request",
            0,
            clientConfiguration.getValueAsInteger(GlobalConfiguration.NETWORK_REQUEST_TIMEOUT));

    for (String message : response.getMessages()) {
      listener.onMessage(message);
    }
  }

  public void addNewClusterToConfiguration(int clusterId, String iClusterName) {
    stateLock.writeLock().lock();
    try {
      // If this if is false maybe the content was already update by the push
      if (clusters.length <= clusterId || clusters[clusterId] == null) {
        // Adding the cluster waiting for the push
        final StorageClusterRemote cluster = new StorageClusterRemote();
        cluster.configure(clusterId, iClusterName.toLowerCase(Locale.ENGLISH));

        if (clusters.length <= clusterId) {
          clusters = Arrays.copyOf(clusters, clusterId + 1);
        }
        clusters[cluster.getId()] = cluster;
        clusterMap.put(cluster.getName().toLowerCase(Locale.ENGLISH), cluster);
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  public void beginTransaction(TransactionOptimistic transaction) {
    var database = (DatabaseSessionRemote) transaction.getDatabase();
    OBeginTransaction38Request request =
        new OBeginTransaction38Request(database,
            transaction.getId(),
            true,
            true,
            transaction.getRecordOperations(), Collections.emptyMap());
    OBeginTransactionResponse response =
        networkOperationNoRetry(database, request, "Error on remote transaction begin");
    for (Map.Entry<RID, RID> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getValue(), entry.getKey());
    }

    stickToSession(database);
  }

  public void sendTransactionState(TransactionOptimistic transaction) {
    var database = (DatabaseSessionRemote) transaction.getDatabase();
    OSendTransactionStateRequest request =
        new OSendTransactionStateRequest(database, transaction.getId(),
            transaction.getRecordOperations());

    OSendTransactionStateResponse response =
        networkOperationNoRetry(database, request,
            "Error on remote transaction state send");

    for (Map.Entry<RID, RID> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getValue(), entry.getKey());
    }

    stickToSession(database);
  }


  public void fetchTransaction(DatabaseSessionRemote remote) {
    TransactionOptimisticClient transaction = remote.getActiveTx();
    OFetchTransaction38Request request = new OFetchTransaction38Request(transaction.getId());
    OFetchTransaction38Response response =
        networkOperation(remote, request, "Error fetching transaction from server side");

    transaction.replaceContent(response.getOperations());
  }

  public OBinaryPushRequest createPush(byte type) {
    return switch (type) {
      case ChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG ->
          new OPushDistributedConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY -> new OLiveQueryPushRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG ->
          new OPushStorageConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_SCHEMA -> new OPushSchemaRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER -> new OPushIndexManagerRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_FUNCTIONS -> new OPushFunctionsRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_SEQUENCES -> new OPushSequencesRequest();
      default -> null;
    };
  }

  public OBinaryPushResponse executeUpdateDistributedConfig(
      OPushDistributedConfigurationRequest request) {
    serverURLs.updateDistributedNodes(request.getHosts(), configuration.getContextConfiguration());
    return null;
  }

  public OBinaryPushResponse executeUpdateFunction(OPushFunctionsRequest request) {
    DatabaseSessionRemote.updateFunction(this);
    return null;
  }

  public OBinaryPushResponse executeUpdateSequences(OPushSequencesRequest request) {
    DatabaseSessionRemote.updateSequences(this);
    return null;
  }

  public OBinaryPushResponse executeUpdateStorageConfig(OPushStorageConfigurationRequest payload) {
    final StorageConfiguration storageConfiguration =
        new StorageConfigurationRemote(
            RecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            payload.getPayload(),
            clientConfiguration);

    updateStorageConfiguration(storageConfiguration);
    return null;
  }

  public OBinaryPushResponse executeUpdateSchema(OPushSchemaRequest request) {
    EntityImpl schema = request.getSchema();
    RecordInternal.setIdentity(schema, new RecordId(configuration.getSchemaRecordId()));
    DatabaseSessionRemote.updateSchema(this, schema);
    return null;
  }

  public OBinaryPushResponse executeUpdateIndexManager(OPushIndexManagerRequest request) {
    EntityImpl indexManager = request.getIndexManager();
    RecordInternal.setIdentity(indexManager, new RecordId(configuration.getIndexMgrRecordId()));
    DatabaseSessionRemote.updateIndexManager(this, indexManager);
    return null;
  }

  public LiveQueryMonitor liveQuery(
      DatabaseSessionRemote database,
      String query,
      OLiveQueryClientListener listener,
      Object[] params) {

    OSubscribeLiveQueryRequest request = new OSubscribeLiveQueryRequest(query, params);
    OSubscribeLiveQueryResponse response = pushThread.subscribe(request,
        getCurrentSession(database));
    if (response == null) {
      throw new DatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new YTLiveQueryMonitorRemote(database, response.getMonitorId());
  }

  public LiveQueryMonitor liveQuery(
      DatabaseSessionRemote database,
      String query,
      OLiveQueryClientListener listener,
      Map<String, ?> params) {
    OSubscribeLiveQueryRequest request =
        new OSubscribeLiveQueryRequest(query, (Map<String, Object>) params);
    OSubscribeLiveQueryResponse response = pushThread.subscribe(request,
        getCurrentSession(database));
    if (response == null) {
      throw new DatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new YTLiveQueryMonitorRemote(database, response.getMonitorId());
  }

  public void unsubscribeLive(DatabaseSessionRemote database, int monitorId) {
    OUnsubscribeRequest request =
        new OUnsubscribeRequest(new OUnsubscribeLiveQueryRequest(monitorId));
    networkOperation(database, request, "Error on unsubscribe of live query");
  }

  public void registerLiveListener(int monitorId, OLiveQueryClientListener listener) {
    liveQueryListener.put(monitorId, listener);
  }

  public static HashMap<String, Object> paramsArrayToParamsMap(Object[] positionalParams) {
    HashMap<String, Object> params = new HashMap<>();
    if (positionalParams != null) {
      for (int i = 0; i < positionalParams.length; i++) {
        params.put(Integer.toString(i), positionalParams[i]);
      }
    }
    return params;
  }

  public void executeLiveQueryPush(OLiveQueryPushRequest pushRequest) {
    OLiveQueryClientListener listener = liveQueryListener.get(pushRequest.getMonitorId());
    if (listener.onEvent(pushRequest)) {
      liveQueryListener.remove(pushRequest.getMonitorId());
    }
  }

  public void onPushReconnect(String host) {
    if (status != STATUS.OPEN) {
      // AVOID RECONNECT ON CLOSE
      return;
    }
    OStorageRemoteSession aValidSession = null;
    for (OStorageRemoteSession session : sessions) {
      if (session.getServerSession(host) != null) {
        aValidSession = session;
        break;
      }
    }
    if (aValidSession != null) {
      subscribeDistributedConfiguration(aValidSession);
      subscribeStorageConfiguration(aValidSession);
    } else {
      LogManager.instance()
          .warn(
              this,
              "Cannot find a valid session for subscribe for event to host '%s' forward the"
                  + " subscribe for the next session open ",
              host);
      OStorageRemotePushThread old;
      stateLock.writeLock().lock();
      try {
        old = pushThread;
        pushThread = null;
      } finally {
        stateLock.writeLock().unlock();
      }
      old.shutdown();
    }
  }

  public void onPushDisconnect(SocketChannelBinary network, Exception e) {
    if (this.connectionManager.getPool(((SocketChannelBinaryAsynchClient) network).getServerURL())
        != null) {
      this.connectionManager.remove((SocketChannelBinaryAsynchClient) network);
    }
    if (e instanceof java.lang.InterruptedException) {
      for (OLiveQueryClientListener liveListener : liveQueryListener.values()) {
        liveListener.onEnd();
      }
    } else {
      for (OLiveQueryClientListener liveListener : liveQueryListener.values()) {
        if (e instanceof BaseException) {
          liveListener.onError((BaseException) e);
        } else {
          liveListener.onError(
              BaseException.wrapException(new DatabaseException("Live query disconnection "), e));
        }
      }
    }
  }

  public void returnSocket(SocketChannelBinary network) {
    this.connectionManager.remove((SocketChannelBinaryAsynchClient) network);
  }

  public void setSchemaRecordId(String schemaRecordId) {
    throw new UnsupportedOperationException();
  }

  public void setDateFormat(String dateFormat) {
    throw new UnsupportedOperationException();
  }

  public void setTimeZone(TimeZone timeZoneValue) {
    throw new UnsupportedOperationException();
  }

  public void setLocaleLanguage(String locale) {
    throw new UnsupportedOperationException();
  }

  public void setCharset(String charset) {
    throw new UnsupportedOperationException();
  }

  public void setIndexMgrRecordId(String indexMgrRecordId) {
    throw new UnsupportedOperationException();
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    throw new UnsupportedOperationException();
  }

  public void setLocaleCountry(String localeCountry) {
    throw new UnsupportedOperationException();
  }

  public void setClusterSelection(String clusterSelection) {
    throw new UnsupportedOperationException();
  }

  public void setMinimumClusters(int minimumClusters) {
    throw new UnsupportedOperationException();
  }

  public void setValidation(boolean validation) {
    throw new UnsupportedOperationException();
  }

  public void removeProperty(String property) {
    throw new UnsupportedOperationException();
  }

  public void setProperty(String property, String value) {
    throw new UnsupportedOperationException();
  }

  public void setRecordSerializer(String recordSerializer, int version) {
    throw new UnsupportedOperationException();
  }

  public void clearProperties() {
    throw new UnsupportedOperationException();
  }

  public List<String> getServerURLs() {
    return serverURLs.getUrls();
  }

  public SharedContext getSharedContext() {
    return sharedContext;
  }

  public boolean isDistributed() {
    return false;
  }

  public STATUS getStatus() {
    return status;
  }

  public void close(DatabaseSessionInternal session) {
    close(session, false);
  }

  public boolean dropCluster(DatabaseSessionInternal session, final String iClusterName) {
    return dropCluster(session, getClusterIdByName(iClusterName));
  }

  public CurrentStorageComponentsFactory getComponentsFactory() {
    return componentsFactory;
  }

  @Override
  public Storage getUnderlying() {
    return null;
  }

  @Override
  public int[] getClustersIds(Set<String> filterClusters) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YouTrackDBInternal getContext() {
    return context;
  }
}
