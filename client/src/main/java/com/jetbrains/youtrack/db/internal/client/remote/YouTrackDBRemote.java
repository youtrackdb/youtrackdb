/*
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
 */

package com.jetbrains.youtrack.db.internal.client.remote;

import static com.jetbrains.youtrack.db.api.config.GlobalConfiguration.NETWORK_SOCKET_RETRY;
import static com.jetbrains.youtrack.db.internal.client.remote.StorageRemote.ADDRESS_SEPARATOR;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.client.binary.SocketChannelBinaryAsynchClient;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemote.CONNECTION_STRATEGY;
import com.jetbrains.youtrack.db.internal.client.remote.db.DatabaseSessionRemote;
import com.jetbrains.youtrack.db.internal.client.remote.db.SharedContextRemote;
import com.jetbrains.youtrack.db.internal.client.remote.message.Connect37Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.ConnectResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.DistributedStatusRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.DistributedStatusResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ExistsDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ExistsDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.FreezeDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.FreezeDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetGlobalConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetGlobalConfigurationResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListDatabasesRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListDatabasesResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListGlobalConfigurationsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListGlobalConfigurationsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReleaseDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReleaseDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.RemoteResultSet;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerInfoRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerInfoResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerQueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SetGlobalConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SetGlobalConfigurationResponse;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutors;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.CachedDatabasePoolFactory;
import com.jetbrains.youtrack.db.internal.core.db.CachedDatabasePoolFactoryImpl;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePoolImpl;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePoolInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseTask;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.AuthenticationInfo;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.CredentialInterceptor;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.security.SecuritySystem;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.TokenSecurityException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class YouTrackDBRemote implements YouTrackDBInternal {

  protected final Map<String, SharedContext> sharedContexts = new HashMap<>();
  private final Map<String, StorageRemote> storages = new HashMap<>();
  private final Set<DatabasePoolInternal> pools = new HashSet<>();
  private final String[] hosts;
  private final YouTrackDBConfigImpl configurations;
  private final YouTrackDBEnginesManager youTrack;
  private final CachedDatabasePoolFactory cachedPoolFactory;
  protected volatile RemoteConnectionManager connectionManager;
  private volatile boolean open = true;
  private final Timer timer;
  private final RemoteURLs urls;
  private final ExecutorService executor;

  public YouTrackDBRemote(String[] hosts, YouTrackDBConfig configurations,
      YouTrackDBEnginesManager youTrack) {
    super();

    this.hosts = hosts;
    this.youTrack = youTrack;

    this.configurations =
        (YouTrackDBConfigImpl) (configurations != null ? configurations
            : YouTrackDBConfig.defaultConfig());

    timer = new Timer("Remote background operations timer", true);
    connectionManager =
        new RemoteConnectionManager(this.configurations.getConfiguration(), timer);
    youTrack.addYouTrackDB(this);
    cachedPoolFactory = createCachedDatabasePoolFactory(this.configurations);
    urls = new RemoteURLs(hosts, this.configurations.getConfiguration());
    int size =
        this.configurations
            .getConfiguration()
            .getValueAsInteger(GlobalConfiguration.EXECUTOR_POOL_MAX_SIZE);
    if (size == -1) {
      size = Runtime.getRuntime().availableProcessors() / 2;
    }
    if (size <= 0) {
      size = 1;
    }

    executor =
        ThreadPoolExecutors.newScalingThreadPool(
            "YouTrackDBRemote", 0, size, 100, 1, TimeUnit.MINUTES);
  }

  protected CachedDatabasePoolFactory createCachedDatabasePoolFactory(YouTrackDBConfigImpl config) {
    int capacity =
        config.getConfiguration().getValueAsInteger(GlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout =
        config
            .getConfiguration()
            .getValueAsInteger(GlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
    return new CachedDatabasePoolFactoryImpl(this, capacity, timeout);
  }

  private String buildUrl(String name) {
    if (name == null) {
      return String.join(ADDRESS_SEPARATOR, hosts);
    }

    return String.join(ADDRESS_SEPARATOR, hosts) + "/" + name;
  }

  public DatabaseSessionInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  @Override
  public DatabaseSessionInternal open(
      String name, String user, String password, YouTrackDBConfig config) {
    checkOpen();
    YouTrackDBConfigImpl resolvedConfig = solveConfig((YouTrackDBConfigImpl) config);
    try {
      StorageRemote storage;
      synchronized (this) {
        storage = storages.get(name);
        if (storage == null) {
          storage = new StorageRemote(urls, name, this, "rw", connectionManager, resolvedConfig);
          storages.put(name, storage);
        }
      }
      DatabaseSessionRemote db =
          new DatabaseSessionRemote(storage, getOrCreateSharedContext(storage));
      db.internalOpen(user, password, resolvedConfig);
      return db;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public DatabaseSessionInternal open(
      AuthenticationInfo authenticationInfo, YouTrackDBConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(String name, String user, String password, DatabaseType databaseType) {
    create(name, user, password, databaseType, null);
  }

  @Override
  public synchronized void create(
      String name,
      String user,
      String password,
      DatabaseType databaseType,
      YouTrackDBConfig config) {

    config = solveConfig((YouTrackDBConfigImpl) config);

    if (name == null || name.length() <= 0 || name.contains("`")) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      LogManager.instance().error(this, message, null);
      throw new StorageException(message);
    }
    String create = String.format("CREATE DATABASE `%s` %s ", name, databaseType.name());
    Map<String, Object> parameters = new HashMap<String, Object>();
    Set<String> keys = config.getConfiguration().getContextKeys();
    if (!keys.isEmpty()) {
      List<String> entries = new ArrayList<String>();
      for (String key : keys) {
        GlobalConfiguration globalKey = GlobalConfiguration.findByKey(key);
        entries.add(String.format("\"%s\": :%s", key, globalKey.name()));
        parameters.put(globalKey.name(), config.getConfiguration().getValue(globalKey));
      }
      create += String.format("{\"config\":{%s}}", String.join(",", entries));
    }

    executeServerStatementNamedParams(create, user, password, parameters).close();
  }

  public DatabaseSessionRemotePooled poolOpen(
      String name, String user, String password, DatabasePoolInternal pool) {
    StorageRemote storage;
    synchronized (this) {
      storage = storages.get(name);
      if (storage == null) {
        try {
          storage =
              new StorageRemote(
                  urls, name, this, "rw", connectionManager, solveConfig(pool.getConfig()));
          storages.put(name, storage);
        } catch (Exception e) {
          throw BaseException.wrapException(
              new DatabaseException("Cannot open database '" + name + "'"), e);
        }
      }
    }
    DatabaseSessionRemotePooled db =
        new DatabaseSessionRemotePooled(pool, storage, getOrCreateSharedContext(storage));
    db.internalOpen(user, password, pool.getConfig());
    return db;
  }

  public synchronized void closeStorage(StorageRemote remote) {
    SharedContext ctx = sharedContexts.get(remote.getName());
    if (ctx != null) {
      ctx.close();
      sharedContexts.remove(remote.getName());
    }
    storages.remove(remote.getName());
    remote.shutdown();
  }

  public EntityImpl getServerInfo(String username, String password) {
    ServerInfoRequest request = new ServerInfoRequest();
    ServerInfoResponse response = connectAndSend(null, username, password, request);
    EntityImpl res = new EntityImpl();
    res.fromJSON(response.getResult());

    return res;
  }

  public EntityImpl getClusterStatus(String username, String password) {
    DistributedStatusRequest request = new DistributedStatusRequest();
    DistributedStatusResponse response = connectAndSend(null, username, password, request);

    LogManager.instance()
        .debug(this, "Cluster status %s", response.getClusterConfig().toJSON("prettyPrint"));
    return response.getClusterConfig();
  }

  public String getGlobalConfiguration(
      String username, String password, GlobalConfiguration config) {
    GetGlobalConfigurationRequest request = new GetGlobalConfigurationRequest(config.getKey());
    GetGlobalConfigurationResponse response = connectAndSend(null, username, password, request);
    return response.getValue();
  }

  public void setGlobalConfiguration(
      String username, String password, GlobalConfiguration config, String iConfigValue) {
    String value = iConfigValue != null ? iConfigValue : "";
    SetGlobalConfigurationRequest request =
        new SetGlobalConfigurationRequest(config.getKey(), value);
    SetGlobalConfigurationResponse response = connectAndSend(null, username, password, request);
  }

  public Map<String, String> getGlobalConfigurations(String username, String password) {
    ListGlobalConfigurationsRequest request = new ListGlobalConfigurationsRequest();
    ListGlobalConfigurationsResponse response = connectAndSend(null, username, password, request);
    return response.getConfigs();
  }

  public RemoteConnectionManager getConnectionManager() {
    return connectionManager;
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    ExistsDatabaseRequest request = new ExistsDatabaseRequest(name, null);
    ExistsDatabaseResponse response = connectAndSend(name, user, password, request);
    return response.isExists();
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    DropDatabaseRequest request = new DropDatabaseRequest(name, null);
    connectAndSend(name, user, password, request);

    SharedContext ctx = sharedContexts.get(name);
    if (ctx != null) {
      ctx.close();
      sharedContexts.remove(name);
    }
    storages.remove(name);
  }

  @Override
  public void internalDrop(String database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> listDatabases(String user, String password) {
    return getDatabases(user, password).keySet();
  }

  public Map<String, String> getDatabases(String user, String password) {
    ListDatabasesRequest request = new ListDatabasesRequest();
    ListDatabasesResponse response = connectAndSend(null, user, password, request);
    return response.getDatabases();
  }

  @Override
  public void restore(
      String name,
      String user,
      String password,
      DatabaseType type,
      String path,
      YouTrackDBConfig config) {
    if (name == null || name.length() <= 0) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      LogManager.instance().error(this, message, null);
      throw new StorageException(message);
    }

    CreateDatabaseRequest request =
        new CreateDatabaseRequest(name, type.name().toLowerCase(), null, path);

    CreateDatabaseResponse response = connectAndSend(name, user, password, request);
  }

  public <T extends BinaryResponse> T connectAndSend(
      String name, String user, String password, BinaryRequest<T> request) {
    return connectAndExecute(
        name,
        user,
        password,
        session -> {
          return networkAdminOperation(
              request, session, "Error sending request:" + request.getDescription());
        });
  }

  public DatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public DatabasePoolInternal openPool(
      String name, String user, String password, YouTrackDBConfig config) {
    checkOpen();
    DatabasePoolImpl pool = new DatabasePoolImpl(this, name, user, password,
        solveConfig((YouTrackDBConfigImpl) config));
    pools.add(pool);
    return pool;
  }

  @Override
  public DatabasePoolInternal cachedPool(String database, String user, String password) {
    return cachedPool(database, user, password, null);
  }

  @Override
  public DatabasePoolInternal cachedPool(
      String database, String user, String password, YouTrackDBConfig config) {
    checkOpen();
    DatabasePoolInternal pool =
        cachedPoolFactory.get(database, user, password, solveConfig((YouTrackDBConfigImpl) config));
    pools.add(pool);
    return pool;
  }

  public void removePool(DatabasePoolInternal pool) {
    pools.remove(pool);
  }

  @Override
  public void close() {
    if (!open) {
      return;
    }
    removeShutdownHook();
    internalClose();
  }

  public void internalClose() {
    if (!open) {
      return;
    }

    if (timer != null) {
      timer.cancel();
    }

    final List<StorageRemote> storagesCopy;
    synchronized (this) {
      // SHUTDOWN ENGINES AVOID OTHER OPENS
      open = false;
      this.sharedContexts.values().forEach(SharedContext::close);
      storagesCopy = new ArrayList<>(storages.values());
    }

    for (StorageRemote stg : storagesCopy) {
      try {
        LogManager.instance().info(this, "- shutdown storage: %s ...", stg.getName());
        stg.shutdown();
      } catch (Exception e) {
        LogManager.instance().warn(this, "-- error on shutdown storage", e);
      } catch (Error e) {
        LogManager.instance().warn(this, "-- error on shutdown storage", e);
        throw e;
      }
    }
    synchronized (this) {
      this.sharedContexts.clear();
      storages.clear();

      connectionManager.close();
    }
  }

  private YouTrackDBConfigImpl solveConfig(YouTrackDBConfigImpl config) {
    if (config != null) {
      config.setParent(this.configurations);
      return config;
    } else {
      YouTrackDBConfigImpl cfg = (YouTrackDBConfigImpl) YouTrackDBConfig.defaultConfig();
      cfg.setParent(this.configurations);
      return cfg;
    }
  }

  private void checkOpen() {
    if (!open) {
      throw new DatabaseException("YouTrackDB Instance is closed");
    }
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isEmbedded() {
    return false;
  }

  @Override
  public void removeShutdownHook() {
    youTrack.removeYouTrackDB(this);
  }

  @Override
  public void loadAllDatabases() {
    // In remote does nothing
  }

  @Override
  public DatabaseSessionInternal openNoAuthenticate(String iDbUrl, String user) {
    throw new UnsupportedOperationException(
        "Open with no authentication is not supported in remote");
  }

  @Override
  public void initCustomStorage(String name, String baseUrl, String userName, String userPassword) {
    throw new UnsupportedOperationException("Custom storage is not supported in remote");
  }

  @Override
  public Collection<Storage> getStorages() {
    throw new UnsupportedOperationException("List storage is not supported in remote");
  }

  @Override
  public synchronized void forceDatabaseClose(String databaseName) {
    StorageRemote remote = storages.get(databaseName);
    if (remote != null) {
      closeStorage(remote);
    }
  }

  @Override
  public void restore(
      String name,
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      CommandOutputListener iListener) {
    throw new UnsupportedOperationException("raw restore is not supported in remote");
  }

  @Override
  public DatabaseSessionInternal openNoAuthorization(String name) {
    throw new UnsupportedOperationException(
        "impossible skip authentication and authorization in remote");
  }

  protected synchronized SharedContext getOrCreateSharedContext(StorageRemote storage) {

    SharedContext result = sharedContexts.get(storage.getName());
    if (result == null) {
      result = createSharedContext(storage);
      sharedContexts.put(storage.getName(), result);
    }
    return result;
  }

  private SharedContext createSharedContext(StorageRemote storage) {
    SharedContextRemote context = new SharedContextRemote(storage, this);
    storage.setSharedContext(context);
    return context;
  }

  public void schedule(TimerTask task, long delay, long period) {
    timer.schedule(task, delay, period);
  }

  public void scheduleOnce(TimerTask task, long delay) {
    timer.schedule(task, delay);
  }

  @Override
  public <X> Future<X> executeNoAuthorizationAsync(String database, DatabaseTask<X> task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
  }

  @Override
  public <X> X executeNoAuthorizationSync(DatabaseSessionInternal database,
      DatabaseTask<X> task) {
    throw new UnsupportedOperationException("not available in remote");
  }

  @Override
  public <X> Future<X> execute(String database, String user, DatabaseTask<X> task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
  }

  @Override
  public Future<?> execute(Runnable task) {
    return executor.submit(task);
  }

  @Override
  public <X> Future<X> execute(Callable<X> task) {
    return executor.submit(task);
  }

  public void releaseDatabase(String database, String user, String password) {
    ReleaseDatabaseRequest request = new ReleaseDatabaseRequest(database, null);
    ReleaseDatabaseResponse response = connectAndSend(database, user, password, request);
  }

  public void freezeDatabase(String database, String user, String password) {
    FreezeDatabaseRequest request = new FreezeDatabaseRequest(database, null);
    FreezeDatabaseResponse response = connectAndSend(database, user, password, request);
  }

  @Override
  public ResultSet executeServerStatementPositionalParams(String statement, String user,
      String pw,
      Object... params) {
    int recordsPerPage =
        getContextConfiguration()
            .getValueAsInteger(GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    ServerQueryRequest request =
        new ServerQueryRequest(
            "sql",
            statement,
            params,
            ServerQueryRequest.COMMAND,
            RecordSerializerNetworkV37Client.INSTANCE, recordsPerPage);

    ServerQueryResponse response = connectAndSend(null, user, pw, request);
    return new RemoteResultSet(
        null,
        response.getQueryId(),
        response.getResult(),
        response.getExecutionPlan(),
        response.getQueryStats(),
        response.isHasNextPage());
  }

  @Override
  public ResultSet executeServerStatementNamedParams(String statement, String user, String pw,
      Map<String, Object> params) {
    int recordsPerPage =
        getContextConfiguration()
            .getValueAsInteger(GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    ServerQueryRequest request =
        new ServerQueryRequest("sql",
            statement,
            params,
            ServerQueryRequest.COMMAND,
            RecordSerializerNetworkV37Client.INSTANCE, recordsPerPage);

    ServerQueryResponse response = connectAndSend(null, user, pw, request);

    return new RemoteResultSet(
        null,
        response.getQueryId(),
        response.getResult(),
        response.getExecutionPlan(),
        response.getQueryStats(),
        response.isHasNextPage());
  }

  public ContextConfiguration getContextConfiguration() {
    return configurations.getConfiguration();
  }

  public <T extends BinaryResponse> T networkAdminOperation(
      final BinaryRequest<T> request, StorageRemoteSession session, final String errorMessage) {
    return networkAdminOperation(
        (network, session1) -> {
          try {
            network.beginRequest(request.getCommand(), session1);
            request.write(null, network, session1);
          } finally {
            network.endRequest();
          }
          T response = request.createResponse();
          try {
            StorageRemote.beginResponse(null, network, session1);
            response.read(null, network, session1);
          } finally {
            network.endResponse();
          }
          return response;
        },
        errorMessage,
        session);
  }

  public <T> T networkAdminOperation(
      final StorageRemoteOperation<T> operation,
      final String errorMessage,
      StorageRemoteSession session) {

    SocketChannelBinaryAsynchClient network = null;
    ContextConfiguration config = getContextConfiguration();
    try {
      String serverUrl =
          urls.getNextAvailableServerURL(false, session, config, CONNECTION_STRATEGY.STICKY);
      do {
        try {
          network = StorageRemote.getNetwork(serverUrl, connectionManager, config);
        } catch (BaseException e) {
          serverUrl = urls.removeAndGet(serverUrl);
          if (serverUrl == null) {
            throw e;
          }
        }
      } while (network == null);

      T res = operation.execute(network, session);
      connectionManager.release(network);
      return res;
    } catch (Exception e) {
      if (network != null) {
        connectionManager.release(network);
      }
      session.closeAllSessions(connectionManager, config);
      throw BaseException.wrapException(new StorageException(errorMessage), e);
    }
  }

  private interface SessionOperation<T> {

    T execute(StorageRemoteSession session) throws IOException;
  }

  private <T> T connectAndExecute(
      String name, String user, String password, SessionOperation<T> operation) {
    checkOpen();
    StorageRemoteSession newSession = new StorageRemoteSession(-1);
    int retry = configurations.getConfiguration().getValueAsInteger(NETWORK_SOCKET_RETRY);
    while (retry > 0) {
      try {
        CredentialInterceptor ci = SecurityManager.instance().newCredentialInterceptor();

        String username;
        String foundPassword;
        String url = buildUrl(name);
        if (ci != null) {
          ci.intercept(url, user, password);
          username = ci.getUsername();
          foundPassword = ci.getPassword();
        } else {
          username = user;
          foundPassword = password;
        }
        Connect37Request request = new Connect37Request(username, foundPassword);

        networkAdminOperation(
            (network, session) -> {
              StorageRemoteNodeSession nodeSession =
                  session.getOrCreateServerSession(network.getServerURL());
              try {
                network.beginRequest(request.getCommand(), session);
                request.write(null, network, session);
              } finally {
                network.endRequest();
              }
              ConnectResponse response = request.createResponse();
              try {
                network.beginResponse(null, nodeSession.getSessionId(), true);
                response.read(null, network, session);
              } finally {
                network.endResponse();
              }
              return null;
            },
            "Cannot connect to the remote server/database '" + url + "'",
            newSession);

        return operation.execute(newSession);
      } catch (IOException | TokenSecurityException e) {
        retry--;
        if (retry == 0) {
          throw BaseException.wrapException(
              new DatabaseException(
                  "Reached maximum retry limit on admin operations, the server may be offline"),
              e);
        }
      } finally {
        newSession.closeAllSessions(connectionManager, configurations.getConfiguration());
      }
    }
    // SHOULD NEVER REACH THIS POINT
    throw new DatabaseException(
        "Reached maximum retry limit on admin operations, the server may be offline");
  }

  @Override
  public YouTrackDBConfigImpl getConfiguration() {
    return configurations;
  }

  @Override
  public SecuritySystem getSecuritySystem() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(
      String name,
      String user,
      String password,
      DatabaseType type,
      YouTrackDBConfig config,
      DatabaseTask<Void> createOps) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getConnectionUrl() {
    return "remote:" + String.join(StorageRemote.ADDRESS_SEPARATOR, this.urls.getUrls());
  }
}
