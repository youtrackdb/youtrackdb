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
package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.ModificationOperationProhibitedException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.client.NotSendRequestException;
import com.jetbrains.youtrack.db.internal.client.binary.SocketChannelBinaryAsynchClient;
import com.jetbrains.youtrack.db.internal.client.remote.db.DatabaseSessionRemote;
import com.jetbrains.youtrack.db.internal.client.remote.db.FrontendTransactionOptimisticClient;
import com.jetbrains.youtrack.db.internal.client.remote.db.YTLiveQueryMonitorRemote;
import com.jetbrains.youtrack.db.internal.client.remote.message.AddClusterRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.AddClusterResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransaction38Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransactionResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.BinaryPushRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.BinaryPushResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CeilingPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CeilingPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CleanOutRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CleanOutRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CloseQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CommandRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CommandResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit37Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit38Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountRecordsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountRecordsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropClusterRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropClusterResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransaction38Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransaction38Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.FloorPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.FloorPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetClusterDataRangeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetClusterDataRangeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetRecordMetadataRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetRecordMetadataResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetSizeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetSizeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.HigherPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.HigherPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ImportRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ImportResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.IncrementalBackupRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.IncrementalBackupResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.LiveQueryPushRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.LowerPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.LowerPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.Open37Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.Open37Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.PushDistributedConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.PushSchemaRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.PushSequencesRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.PushStorageConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryNextPageRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReadRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReadRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.RecordExistsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReloadRequest37;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReloadResponse37;
import com.jetbrains.youtrack.db.internal.client.remote.message.RemoteResultSet;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReopenRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReopenResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.RollbackTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SendTransactionStateRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SendTransactionStateResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeFunctionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeIndexManagerRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeLiveQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeLiveQueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeSchemaRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeSequencesRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeStorageConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UnsubscribeLiveQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UnsubscribeRequest;
import com.jetbrains.youtrack.db.internal.common.concur.OfflineNodeException;
import com.jetbrains.youtrack.db.internal.common.concur.lock.ThreadInterruptedException;
import com.jetbrains.youtrack.db.internal.common.io.YTIOException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutors;
import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestAsynch;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.config.StorageClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.RecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTxInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.CurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.security.TokenException;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordVersionHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.CredentialInterceptor;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.sql.query.LiveQuery;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.RawBuffer;
import com.jetbrains.youtrack.db.internal.core.storage.RecordCallback;
import com.jetbrains.youtrack.db.internal.core.storage.RecordMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster;
import com.jetbrains.youtrack.db.internal.core.storage.StorageProxy;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.PaginatedCluster;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.RecordSerializationContext;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionOptimistic;
import com.jetbrains.youtrack.db.internal.core.tx.TransactionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.DistributedRedirectException;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.SocketChannelBinary;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.TokenSecurityException;
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
public class StorageRemote implements StorageProxy, RemotePushHandler, Storage {

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

  private final BTreeCollectionManagerRemote sbTreeCollectionManager =
      new BTreeCollectionManagerRemote();
  private final RemoteURLs serverURLs;
  private final Map<String, StorageCluster> clusterMap = new ConcurrentHashMap<String, StorageCluster>();
  private final ExecutorService asynchExecutor;
  private final AtomicInteger users = new AtomicInteger(0);
  private final ContextConfiguration clientConfiguration;
  private final int connectionRetry;
  private final int connectionRetryDelay;
  private StorageCluster[] clusters = CommonConst.EMPTY_CLUSTER_ARRAY;
  private int defaultClusterId;
  public RemoteConnectionManager connectionManager;
  private final Set<StorageRemoteSession> sessions =
      Collections.newSetFromMap(new ConcurrentHashMap<StorageRemoteSession, Boolean>());

  private final Map<Integer, LiveQueryClientListener> liveQueryListener =
      new ConcurrentHashMap<>();
  private volatile StorageRemotePushThread pushThread;
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
      final RemoteURLs hosts,
      String name,
      YouTrackDBRemote context,
      final String iMode,
      RemoteConnectionManager connectionManager,
      YouTrackDBConfigImpl config)
      throws IOException {
    this(hosts, name, context, iMode, connectionManager, null, config);
  }

  public StorageRemote(
      final RemoteURLs hosts,
      String name,
      YouTrackDBRemote context,
      final String iMode,
      RemoteConnectionManager connectionManager,
      final STATUS status,
      YouTrackDBConfigImpl config)
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
      clientConfiguration = config.getConfiguration();
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

  public <T extends BinaryResponse> T asyncNetworkOperationNoRetry(
      DatabaseSessionRemote database, final BinaryAsyncRequest<T> request,
      int mode,
      final RecordId recordId,
      final RecordCallback<T> callback,
      final String errorMessage) {
    return asyncNetworkOperationRetry(database, request, mode, recordId, callback, errorMessage, 0);
  }

  public <T extends BinaryResponse> T asyncNetworkOperationRetry(
      DatabaseSessionRemote database, final BinaryAsyncRequest<T> request,
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

  public <T extends BinaryResponse> T networkOperationRetryTimeout(
      DatabaseSessionRemote database, final BinaryRequest<T> request, final String errorMessage,
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

  public <T extends BinaryResponse> T networkOperationNoRetry(
      DatabaseSessionRemote database, final BinaryRequest<T> request,
      final String errorMessage) {
    return networkOperationRetryTimeout(database, request, errorMessage, 0, 0);
  }

  public <T extends BinaryResponse> T networkOperation(
      DatabaseSessionRemote database, final BinaryRequest<T> request,
      final String errorMessage) {
    return networkOperationRetryTimeout(database, request, errorMessage, connectionRetry, 0);
  }

  public <T> T baseNetworkOperation(
      DatabaseSessionRemote remoteSession, final StorageRemoteOperation<T> operation,
      final String errorMessage, int retry) {
    StorageRemoteSession session = getCurrentSession(remoteSession);
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
        StorageRemoteNodeSession nodeSession = session.getServerSession(network.getServerURL());
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
        for (StorageRemoteSession activeSession : sessions) {
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
    StorageRemoteSession session = getCurrentSession(database);
    return session != null ? session.getSessionId() : -1;
  }

  public void open(
      DatabaseSessionInternal db, final String iUserName, final String iUserPassword,
      final ContextConfiguration conf) {
    var remoteDb = (DatabaseSessionRemote) db;
    addUser();
    try {
      StorageRemoteSession session = getCurrentSession(remoteDb);
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

  public BTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public void reload(DatabaseSessionInternal database) {
    ReloadResponse37 res =
        networkOperation((DatabaseSessionRemote) database, new ReloadRequest37(),
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
        "Cannot create a database in a remote server. Please use the console or the ServerAdmin"
            + " class.");
  }

  public boolean exists() {
    throw new UnsupportedOperationException(
        "Cannot check the existence of a database in a remote server. Please use the console or the"
            + " ServerAdmin class.");
  }

  public void close(DatabaseSessionInternal database, final boolean iForce) {
    if (status == STATUS.CLOSED) {
      return;
    }

    final StorageRemoteSession session = getCurrentSession((DatabaseSessionRemote) database);
    if (session != null) {
      final Collection<StorageRemoteNodeSession> nodes = session.getAllServerSessions();
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
    for (Entry<Integer, LiveQueryClientListener> listener : liveQueryListener.entrySet()) {
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
        "Cannot delete a database in a remote server. Please use the console or the ServerAdmin"
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
      final BTreeCollectionManager collectionManager,
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
    GetRecordMetadataRequest request = new GetRecordMetadataRequest(rid);
    GetRecordMetadataResponse response =
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

    var request = new RecordExistsRequest(rid);
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

    ReadRecordRequest request = new ReadRecordRequest(iIgnoreCache, iRid, null, false);
    ReadRecordResponse response = networkOperation(remoteSession, request,
        "Error on read record " + iRid);

    return response.getResult();
  }

  public String incrementalBackup(DatabaseSessionInternal session, final String backupDirectory,
      CallableFunction<Void, Void> started) {
    IncrementalBackupRequest request = new IncrementalBackupRequest(backupDirectory);
    IncrementalBackupResponse response =
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

    RecordCallback<CleanOutRecordResponse> realCallback = null;
    if (callback != null) {
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());
    }

    final CleanOutRecordRequest request = new CleanOutRecordRequest(recordVersion, recordId);
    final CleanOutRecordResponse response =
        asyncNetworkOperationNoRetry((DatabaseSessionRemote) session,
            request, iMode, recordId, realCallback, "Error on delete record " + recordId);
    Boolean result = null;
    if (response != null) {
      result = response.getResult();
    }
    return result != null ? result : false;
  }

  public List<String> backup(
      DatabaseSessionInternal db, OutputStream out,
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
    GetClusterDataRangeRequest request = new GetClusterDataRangeRequest(iClusterId);
    GetClusterDataRangeResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request, "Error on getting last entry position count in cluster: " + iClusterId);
    return response.getPos();
  }

  public PhysicalPosition[] higherPhysicalPositions(
      DatabaseSessionInternal session, final int iClusterId,
      final PhysicalPosition iClusterPosition) {
    HigherPhysicalPositionsRequest request =
        new HigherPhysicalPositionsRequest(iClusterId, iClusterPosition);

    HigherPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
    return response.getNextPositions();
  }

  public PhysicalPosition[] ceilingPhysicalPositions(
      DatabaseSessionInternal session, final int clusterId,
      final PhysicalPosition physicalPosition) {

    CeilingPhysicalPositionsRequest request =
        new CeilingPhysicalPositionsRequest(clusterId, physicalPosition);

    CeilingPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public PhysicalPosition[] lowerPhysicalPositions(
      DatabaseSessionInternal session, final int iClusterId,
      final PhysicalPosition physicalPosition) {
    LowerPhysicalPositionsRequest request =
        new LowerPhysicalPositionsRequest(physicalPosition, iClusterId);
    LowerPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
    return response.getPreviousPositions();
  }

  public PhysicalPosition[] floorPhysicalPositions(
      DatabaseSessionInternal session, final int clusterId,
      final PhysicalPosition physicalPosition) {
    FloorPhysicalPositionsRequest request =
        new FloorPhysicalPositionsRequest(physicalPosition, clusterId);
    FloorPhysicalPositionsResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request,
            "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public long getSize(DatabaseSessionInternal session) {
    GetSizeRequest request = new GetSizeRequest();
    GetSizeResponse response = networkOperation((DatabaseSessionRemote) session, request,
        "Error on read database size");
    return response.getSize();
  }

  public long countRecords(DatabaseSessionInternal session) {
    CountRecordsRequest request = new CountRecordsRequest();
    CountRecordsResponse response =
        networkOperation((DatabaseSessionRemote) session, request,
            "Error on read database record count");
    return response.getCountRecords();
  }

  public long count(DatabaseSessionInternal session, final int[] iClusterIds) {
    return count(session, iClusterIds, false);
  }

  public long count(DatabaseSessionInternal session, final int[] iClusterIds,
      final boolean countTombstones) {
    CountRequest request = new CountRequest(iClusterIds, countTombstones);
    CountResponse response =
        networkOperation((DatabaseSessionRemote) session,
            request, "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
    return response.getCount();
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(DatabaseSessionInternal db, final CommandRequestText iCommand) {

    final boolean live = iCommand instanceof LiveQuery;
    final boolean asynch =
        iCommand instanceof CommandRequestAsynch
            && ((CommandRequestAsynch) iCommand).isAsynchronous();

    var remoteDb = (DatabaseSessionRemote) db;
    CommandRequest request = new CommandRequest(remoteDb, asynch, iCommand, live);
    CommandResponse response =
        networkOperation(remoteDb, request,
            "Error on executing command: " + iCommand);
    return response.getResult();
  }

  public void stickToSession(DatabaseSessionRemote database) {
    StorageRemoteSession session = getCurrentSession(database);
    session.stickToSession();
  }

  public void unstickToSession(DatabaseSessionRemote database) {
    StorageRemoteSession session = getCurrentSession(database);
    session.unStickToSession();
  }

  public RemoteQueryResult query(DatabaseSessionRemote db, String query, Object[] args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryRequest request =
        new QueryRequest(db,
            "sql", query, args, QueryRequest.QUERY, db.getSerializer(), recordsPerPage);
    QueryResponse response = networkOperation(db, request, "Error on executing command: " + query);
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
      return new RemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public RemoteQueryResult query(DatabaseSessionRemote db, String query, Map args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryRequest request =
        new QueryRequest(db,
            "sql", query, args, QueryRequest.QUERY, db.getSerializer(), recordsPerPage);
    QueryResponse response = networkOperation(db, request, "Error on executing command: " + query);

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
      return new RemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public RemoteQueryResult command(DatabaseSessionRemote db, String query, Object[] args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryRequest request =
        new QueryRequest(db,
            "sql", query, args, QueryRequest.COMMAND, db.getSerializer(), recordsPerPage);
    QueryResponse response =
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
      return new RemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public RemoteQueryResult command(DatabaseSessionRemote db, String query, Map args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryRequest request =
        new QueryRequest(db,
            "sql", query, args, QueryRequest.COMMAND, db.getSerializer(), recordsPerPage);
    QueryResponse response =
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
      return new RemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public RemoteQueryResult execute(
      DatabaseSessionRemote db, String language, String query, Object[] args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryRequest request =
        new QueryRequest(db,
            language, query, args, QueryRequest.EXECUTE, db.getSerializer(), recordsPerPage);
    QueryResponse response =
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

      return new RemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public RemoteQueryResult execute(
      DatabaseSessionRemote db, String language, String query, Map args) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryRequest request =
        new QueryRequest(db,
            language, query, args, QueryRequest.EXECUTE, db.getSerializer(), recordsPerPage);
    QueryResponse response =
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
      return new RemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
    } catch (Exception e) {
      db.queryClosed(response.getQueryId());
      throw e;
    }
  }

  public void closeQuery(DatabaseSessionRemote database, String queryId) {
    unstickToSession(database);
    CloseQueryRequest request = new CloseQueryRequest(queryId);
    networkOperation(database, request, "Error closing query: " + queryId);
  }

  public void fetchNextPage(DatabaseSessionRemote database, RemoteResultSet rs) {
    int recordsPerPage = GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    QueryNextPageRequest request = new QueryNextPageRequest(rs.getQueryId(), recordsPerPage);
    QueryResponse response =
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

  public List<RecordOperation> commit(final FrontendTransactionOptimistic iTx) {
    var remoteSession = (DatabaseSessionRemote) iTx.getDatabase();
    unstickToSession(remoteSession);

    final Commit38Request request =
        new Commit38Request(iTx.getDatabase(),
            iTx.getId(), true, true, iTx.getRecordOperations(), Collections.emptyMap());

    final Commit37Response response = networkOperationNoRetry(remoteSession, request,
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
      if (((FrontendTransactionOptimistic) iTx).isStartedOnServer()
          && !getCurrentSession(remoteSession).getAllServerSessions().isEmpty()) {
        RollbackTransactionRequest request = new RollbackTransactionRequest(iTx.getId());
        networkOperation(remoteSession, request,
            "Error on fetching next page for statement: " + request);
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
    AddClusterRequest request = new AddClusterRequest(iRequestedId, iClusterName);
    AddClusterResponse response = networkOperationNoRetry((DatabaseSessionRemote) database,
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

    DropClusterRequest request = new DropClusterRequest(iClusterId);

    DropClusterResponse response =
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
    final StorageRemoteSession session = getCurrentSession((DatabaseSessionRemote) database);
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
          StorageRemoteSession session = getCurrentSession(database);
          StorageRemoteNodeSession nodeSession =
              session.getOrCreateServerSession(network.getServerURL());
          if (nodeSession == null || !nodeSession.isValid()) {
            openRemoteDatabase(database, network);
            return;
          } else {
            ReopenRequest request = new ReopenRequest();

            try {
              network.writeByte(request.getCommand());
              network.writeInt(nodeSession.getSessionId());
              network.writeBytes(nodeSession.getToken());
              request.write(null, network, session);
            } finally {
              endRequest(network);
            }

            ReopenResponse response = request.createResponse();
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
          StorageRemoteSession session = getCurrentSession(database);
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
          // PROPAGATE ANY OTHER EXCEPTION
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

    StorageRemoteSession session = getCurrentSession(database);
    StorageRemoteNodeSession nodeSession =
        session.getOrCreateServerSession(network.getServerURL());
    Open37Request request =
        new Open37Request(name, session.connectionUserName, session.connectionUserPassword);
    try {
      network.writeByte(request.getCommand());
      network.writeInt(nodeSession.getSessionId());
      network.writeBytes(null);
      request.write(null, network, session);
    } finally {
      endRequest(network);
    }
    final int sessionId;
    Open37Response response = request.createResponse();
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

  private void initPush(DatabaseSessionRemote database, StorageRemoteSession session) {
    if (pushThread == null) {
      stateLock.writeLock().lock();
      try {
        if (pushThread == null) {
          pushThread =
              new StorageRemotePushThread(
                  this,
                  getCurrentServerURL(database),
                  connectionRetryDelay,
                  configuration
                      .getContextConfiguration()
                      .getValueAsLong(GlobalConfiguration.NETWORK_REQUEST_TIMEOUT));
          pushThread.start();
          subscribeStorageConfiguration(session);
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


  private void subscribeStorageConfiguration(StorageRemoteSession nodeSession) {
    pushThread.subscribe(new SubscribeStorageConfigurationRequest(), nodeSession);
  }

  private void subscribeSchema(StorageRemoteSession nodeSession) {
    pushThread.subscribe(new SubscribeSchemaRequest(), nodeSession);
  }

  private void subscribeFunctions(StorageRemoteSession nodeSession) {
    pushThread.subscribe(new SubscribeFunctionsRequest(), nodeSession);
  }

  private void subscribeSequences(StorageRemoteSession nodeSession) {
    pushThread.subscribe(new SubscribeSequencesRequest(), nodeSession);
  }

  private void subscribeIndexManager(StorageRemoteSession nodeSession) {
    pushThread.subscribe(new SubscribeIndexManagerRequest(), nodeSession);
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
          // PROPAGATE ANY OTHER EXCEPTION
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
    StorageRemoteSession session = getCurrentSession(database);
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
      StorageRemoteSession session)
      throws IOException {
    network.beginRequest(iCommand, session);
    return network;
  }

  protected String getNextAvailableServerURL(
      boolean iIsConnectOperation, StorageRemoteSession session) {

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
      RemoteConnectionManager connectionManager,
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
      StorageRemoteSession session) throws IOException {
    StorageRemoteNodeSession nodeSession = session.getServerSession(iNetwork.getServerURL());
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
  protected StorageRemoteSession getCurrentSession(@Nullable DatabaseSessionRemote db) {
    if (db == null) {
      return null;
    }

    StorageRemoteSession session = db.getSessionMetadata();
    if (session == null) {
      session = new StorageRemoteSession(sessionSerialId.decrementAndGet());
      sessions.add(session);
      db.setSessionMetadata(session);
    }

    return session;
  }

  public boolean isClosed(DatabaseSessionInternal database) {
    if (status == STATUS.CLOSED) {
      return true;
    }
    final StorageRemoteSession session = getCurrentSession((DatabaseSessionRemote) database);
    if (session == null) {
      return false;
    }
    return session.isClosed();
  }

  public StorageRemote copy(
      final DatabaseSessionRemote source, final DatabaseSessionRemote dest) {
    final StorageRemoteSession session = source.getSessionMetadata();
    DatabaseSessionInternal origin = null;
    if (DatabaseRecordThreadLocal.instance() != null) {
      origin = DatabaseRecordThreadLocal.instance().getIfDefined();
    }

    origin = DatabaseDocumentTxInternal.getInternal(origin);
    if (session != null) {
      // TODO:may run a session reopen
      final StorageRemoteSession newSession =
          new StorageRemoteSession(sessionSerialId.decrementAndGet());
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
    ImportRequest request = new ImportRequest(inputStream, options, name);

    ImportResponse response =
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

  public void beginTransaction(FrontendTransactionOptimistic transaction) {
    var database = (DatabaseSessionRemote) transaction.getDatabase();
    BeginTransaction38Request request =
        new BeginTransaction38Request(database,
            transaction.getId(),
            true,
            true,
            transaction.getRecordOperations(), Collections.emptyMap());
    BeginTransactionResponse response =
        networkOperationNoRetry(database, request, "Error on remote transaction begin");
    for (Map.Entry<RecordId, RecordId> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getValue(), entry.getKey());
    }

    stickToSession(database);
  }

  public void sendTransactionState(FrontendTransactionOptimistic transaction) {
    var database = (DatabaseSessionRemote) transaction.getDatabase();
    SendTransactionStateRequest request =
        new SendTransactionStateRequest(database, transaction.getId(),
            transaction.getRecordOperations());

    SendTransactionStateResponse response =
        networkOperationNoRetry(database, request,
            "Error on remote transaction state send");

    for (Map.Entry<RecordId, RecordId> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getValue(), entry.getKey());
    }

    stickToSession(database);
  }


  public void fetchTransaction(DatabaseSessionRemote remote) {
    FrontendTransactionOptimisticClient transaction = remote.getActiveTx();
    FetchTransaction38Request request = new FetchTransaction38Request(transaction.getId());
    FetchTransaction38Response response =
        networkOperation(remote, request, "Error fetching transaction from server side");

    transaction.replaceContent(response.getOperations());
  }

  public BinaryPushRequest createPush(byte type) {
    return switch (type) {
      case ChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG ->
          new PushDistributedConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY -> new LiveQueryPushRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG ->
          new PushStorageConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_PUSH_SCHEMA -> new PushSchemaRequest();
      default -> null;
    };
  }

  public BinaryPushResponse executeUpdateDistributedConfig(
      PushDistributedConfigurationRequest request) {
    serverURLs.updateDistributedNodes(request.getHosts(), configuration.getContextConfiguration());
    return null;
  }

  public BinaryPushResponse executeUpdateSequences(PushSequencesRequest request) {
    DatabaseSessionRemote.updateSequences(this);
    return null;
  }

  public BinaryPushResponse executeUpdateStorageConfig(PushStorageConfigurationRequest payload) {
    final StorageConfiguration storageConfiguration =
        new StorageConfigurationRemote(
            RecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            payload.getPayload(),
            clientConfiguration);

    updateStorageConfiguration(storageConfiguration);
    return null;
  }

  public BinaryPushResponse executeUpdateSchema(PushSchemaRequest request) {
    EntityImpl schema = request.getSchema();
    RecordInternal.setIdentity(schema, new RecordId(configuration.getSchemaRecordId()));
    DatabaseSessionRemote.updateSchema(this, schema);
    return null;
  }


  public LiveQueryMonitor liveQuery(
      DatabaseSessionRemote database,
      String query,
      LiveQueryClientListener listener,
      Object[] params) {

    SubscribeLiveQueryRequest request = new SubscribeLiveQueryRequest(query, params);
    SubscribeLiveQueryResponse response = pushThread.subscribe(request,
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
      LiveQueryClientListener listener,
      Map<String, ?> params) {
    SubscribeLiveQueryRequest request =
        new SubscribeLiveQueryRequest(query, (Map<String, Object>) params);
    SubscribeLiveQueryResponse response = pushThread.subscribe(request,
        getCurrentSession(database));
    if (response == null) {
      throw new DatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new YTLiveQueryMonitorRemote(database, response.getMonitorId());
  }

  public void unsubscribeLive(DatabaseSessionRemote database, int monitorId) {
    UnsubscribeRequest request =
        new UnsubscribeRequest(new UnsubscribeLiveQueryRequest(monitorId));
    networkOperation(database, request, "Error on unsubscribe of live query");
  }

  public void registerLiveListener(int monitorId, LiveQueryClientListener listener) {
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

  public void executeLiveQueryPush(LiveQueryPushRequest pushRequest) {
    LiveQueryClientListener listener = liveQueryListener.get(pushRequest.getMonitorId());
    if (listener.onEvent(pushRequest)) {
      liveQueryListener.remove(pushRequest.getMonitorId());
    }
  }

  public void onPushReconnect(String host) {
    if (status != STATUS.OPEN) {
      // AVOID RECONNECT ON CLOSE
      return;
    }
    StorageRemoteSession aValidSession = null;
    for (StorageRemoteSession session : sessions) {
      if (session.getServerSession(host) != null) {
        aValidSession = session;
        break;
      }
    }
    if (aValidSession != null) {
      subscribeStorageConfiguration(aValidSession);
    } else {
      LogManager.instance()
          .warn(
              this,
              "Cannot find a valid session for subscribe for event to host '%s' forward the"
                  + " subscribe for the next session open ",
              host);
      StorageRemotePushThread old;
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
      for (LiveQueryClientListener liveListener : liveQueryListener.values()) {
        liveListener.onEnd();
      }
    } else {
      for (LiveQueryClientListener liveListener : liveQueryListener.values()) {
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
