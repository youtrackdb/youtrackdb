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

package com.orientechnologies.orient.client.remote;

import static com.orientechnologies.orient.client.remote.StorageRemote.ADDRESS_SEPARATOR;
import static com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration.NETWORK_SOCKET_RETRY;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutors;
import com.jetbrains.youtrack.db.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseType;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.CredentialInterceptor;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.security.SecuritySystem;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.orientechnologies.orient.client.binary.SocketChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.StorageRemote.CONNECTION_STRATEGY;
import com.orientechnologies.orient.client.remote.db.document.SharedContextRemote;
import com.orientechnologies.orient.client.remote.db.document.DatabaseSessionRemote;
import com.orientechnologies.orient.client.remote.message.OConnect37Request;
import com.orientechnologies.orient.client.remote.message.OConnectResponse;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusResponse;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationResponse;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OListDatabasesResponse;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsResponse;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OServerInfoResponse;
import com.orientechnologies.orient.client.remote.message.OServerQueryRequest;
import com.orientechnologies.orient.client.remote.message.OServerQueryResponse;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationResponse;
import com.orientechnologies.orient.client.remote.message.RemoteResultSet;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.CachedDatabasePoolFactory;
import com.jetbrains.youtrack.db.internal.core.db.CachedDatabasePoolFactoryImpl;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePoolImpl;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePoolInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseTask;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.AuthenticationInfo;
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
  private final YouTrackDBConfig configurations;
  private final YouTrackDBManager youTrack;
  private final CachedDatabasePoolFactory cachedPoolFactory;
  protected volatile ORemoteConnectionManager connectionManager;
  private volatile boolean open = true;
  private final Timer timer;
  private final ORemoteURLs urls;
  private final ExecutorService executor;

  public YouTrackDBRemote(String[] hosts, YouTrackDBConfig configurations,
      YouTrackDBManager youTrack) {
    super();

    this.hosts = hosts;
    this.youTrack = youTrack;
    this.configurations =
        configurations != null ? configurations : YouTrackDBConfig.defaultConfig();
    timer = new Timer("Remote background operations timer", true);
    connectionManager =
        new ORemoteConnectionManager(this.configurations.getConfigurations(), timer);
    youTrack.addYouTrackDB(this);
    cachedPoolFactory = createCachedDatabasePoolFactory(this.configurations);
    urls = new ORemoteURLs(hosts, this.configurations.getConfigurations());
    int size =
        this.configurations
            .getConfigurations()
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

  protected CachedDatabasePoolFactory createCachedDatabasePoolFactory(YouTrackDBConfig config) {
    int capacity =
        config.getConfigurations().getValueAsInteger(GlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout =
        config
            .getConfigurations()
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
    YouTrackDBConfig resolvedConfig = solveConfig(config);
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

    config = solveConfig(config);

    if (name == null || name.length() <= 0 || name.contains("`")) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      LogManager.instance().error(this, message, null);
      throw new StorageException(message);
    }
    String create = String.format("CREATE DATABASE `%s` %s ", name, databaseType.name());
    Map<String, Object> parameters = new HashMap<String, Object>();
    Set<String> keys = config.getConfigurations().getContextKeys();
    if (!keys.isEmpty()) {
      List<String> entries = new ArrayList<String>();
      for (String key : keys) {
        GlobalConfiguration globalKey = GlobalConfiguration.findByKey(key);
        entries.add(String.format("\"%s\": :%s", key, globalKey.name()));
        parameters.put(globalKey.name(), config.getConfigurations().getValue(globalKey));
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
    OServerInfoRequest request = new OServerInfoRequest();
    OServerInfoResponse response = connectAndSend(null, username, password, request);
    EntityImpl res = new EntityImpl();
    res.fromJSON(response.getResult());

    return res;
  }

  public EntityImpl getClusterStatus(String username, String password) {
    ODistributedStatusRequest request = new ODistributedStatusRequest();
    ODistributedStatusResponse response = connectAndSend(null, username, password, request);

    LogManager.instance()
        .debug(this, "Cluster status %s", response.getClusterConfig().toJSON("prettyPrint"));
    return response.getClusterConfig();
  }

  public String getGlobalConfiguration(
      String username, String password, GlobalConfiguration config) {
    OGetGlobalConfigurationRequest request = new OGetGlobalConfigurationRequest(config.getKey());
    OGetGlobalConfigurationResponse response = connectAndSend(null, username, password, request);
    return response.getValue();
  }

  public void setGlobalConfiguration(
      String username, String password, GlobalConfiguration config, String iConfigValue) {
    String value = iConfigValue != null ? iConfigValue : "";
    OSetGlobalConfigurationRequest request =
        new OSetGlobalConfigurationRequest(config.getKey(), value);
    OSetGlobalConfigurationResponse response = connectAndSend(null, username, password, request);
  }

  public Map<String, String> getGlobalConfigurations(String username, String password) {
    OListGlobalConfigurationsRequest request = new OListGlobalConfigurationsRequest();
    OListGlobalConfigurationsResponse response = connectAndSend(null, username, password, request);
    return response.getConfigs();
  }

  public ORemoteConnectionManager getConnectionManager() {
    return connectionManager;
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    OExistsDatabaseRequest request = new OExistsDatabaseRequest(name, null);
    OExistsDatabaseResponse response = connectAndSend(name, user, password, request);
    return response.isExists();
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    ODropDatabaseRequest request = new ODropDatabaseRequest(name, null);
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
    OListDatabasesRequest request = new OListDatabasesRequest();
    OListDatabasesResponse response = connectAndSend(null, user, password, request);
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

    OCreateDatabaseRequest request =
        new OCreateDatabaseRequest(name, type.name().toLowerCase(), null, path);

    OCreateDatabaseResponse response = connectAndSend(name, user, password, request);
  }

  public <T extends OBinaryResponse> T connectAndSend(
      String name, String user, String password, OBinaryRequest<T> request) {
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
    DatabasePoolImpl pool = new DatabasePoolImpl(this, name, user, password, solveConfig(config));
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
        cachedPoolFactory.get(database, user, password, solveConfig(config));
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

  private YouTrackDBConfig solveConfig(YouTrackDBConfig config) {
    if (config != null) {
      config.setParent(this.configurations);
      return config;
    } else {
      YouTrackDBConfig cfg = YouTrackDBConfig.defaultConfig();
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
    OReleaseDatabaseRequest request = new OReleaseDatabaseRequest(database, null);
    OReleaseDatabaseResponse response = connectAndSend(database, user, password, request);
  }

  public void freezeDatabase(String database, String user, String password) {
    OFreezeDatabaseRequest request = new OFreezeDatabaseRequest(database, null);
    OFreezeDatabaseResponse response = connectAndSend(database, user, password, request);
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
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            statement,
            params,
            OServerQueryRequest.COMMAND,
            RecordSerializerNetworkV37Client.INSTANCE, recordsPerPage);

    OServerQueryResponse response = connectAndSend(null, user, pw, request);
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
    OServerQueryRequest request =
        new OServerQueryRequest("sql",
            statement,
            params,
            OServerQueryRequest.COMMAND,
            RecordSerializerNetworkV37Client.INSTANCE, recordsPerPage);

    OServerQueryResponse response = connectAndSend(null, user, pw, request);

    return new RemoteResultSet(
        null,
        response.getQueryId(),
        response.getResult(),
        response.getExecutionPlan(),
        response.getQueryStats(),
        response.isHasNextPage());
  }

  public ContextConfiguration getContextConfiguration() {
    return configurations.getConfigurations();
  }

  public <T extends OBinaryResponse> T networkAdminOperation(
      final OBinaryRequest<T> request, OStorageRemoteSession session, final String errorMessage) {
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
      final OStorageRemoteOperation<T> operation,
      final String errorMessage,
      OStorageRemoteSession session) {

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

    T execute(OStorageRemoteSession session) throws IOException;
  }

  private <T> T connectAndExecute(
      String name, String user, String password, SessionOperation<T> operation) {
    checkOpen();
    OStorageRemoteSession newSession = new OStorageRemoteSession(-1);
    int retry = configurations.getConfigurations().getValueAsInteger(NETWORK_SOCKET_RETRY);
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
        OConnect37Request request = new OConnect37Request(username, foundPassword);

        networkAdminOperation(
            (network, session) -> {
              OStorageRemoteNodeSession nodeSession =
                  session.getOrCreateServerSession(network.getServerURL());
              try {
                network.beginRequest(request.getCommand(), session);
                request.write(null, network, session);
              } finally {
                network.endRequest();
              }
              OConnectResponse response = request.createResponse();
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
        newSession.closeAllSessions(connectionManager, configurations.getConfigurations());
      }
    }
    // SHOULD NEVER REACH THIS POINT
    throw new DatabaseException(
        "Reached maximum retry limit on admin operations, the server may be offline");
  }

  @Override
  public YouTrackDBConfig getConfigurations() {
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
