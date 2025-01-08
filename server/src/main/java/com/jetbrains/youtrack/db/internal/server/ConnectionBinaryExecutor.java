package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.OfflineClusterException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.AddClusterRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.AddClusterResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransaction38Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransactionResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.BinaryProtocolHelper;
import com.jetbrains.youtrack.db.internal.client.remote.message.CeilingPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CeilingPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CleanOutRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CleanOutRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CloseQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CloseQueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CloseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CommandRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CommandResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit37Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit37Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit38Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.CommitRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CommitResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.Connect37Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.ConnectRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ConnectResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountRecordsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountRecordsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CountResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropClusterRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropClusterResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.DropDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ExistsDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ExistsDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransaction38Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransaction38Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransactionResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.FloorPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.FloorPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.FreezeDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.FreezeDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetClusterDataRangeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetClusterDataRangeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetGlobalConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.GetGlobalConfigurationResponse;
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
import com.jetbrains.youtrack.db.internal.client.remote.message.ListDatabasesRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListDatabasesResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListGlobalConfigurationsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ListGlobalConfigurationsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.LowerPhysicalPositionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.LowerPhysicalPositionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.Open37Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.Open37Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.OpenRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.OpenResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryNextPageRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReadRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReadRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.RecordExistsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.RecordExistsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReleaseDatabaseRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReleaseDatabaseResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReloadRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReloadRequest37;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReloadResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReloadResponse37;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReopenRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ReopenResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.RollbackTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.RollbackTransactionResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTCreateTreeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTCreateTreeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTFetchEntriesMajorRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTFetchEntriesMajorResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTFirstKeyRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTFirstKeyResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTGetRealBagSizeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTGetRealBagSizeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTGetRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SBTGetResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SendTransactionStateRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SendTransactionStateResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerInfoRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerInfoResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ServerQueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SetGlobalConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SetGlobalConfigurationResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.ShutdownRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.ShutdownResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeFunctionsRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeFunctionsResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeIndexManagerRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeIndexManagerResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeLiveQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeLiveQueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeSchemaRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeSchemaResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeSequencesRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeSequencesResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeStorageConfigurationRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.SubscribeStorageConfigurationResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.UnsubscribLiveQueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.UnsubscribeLiveQueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UnsubscribeRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UnsubscribeResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.UpdateRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UpdateRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.NullSerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseImport;
import com.jetbrains.youtrack.db.internal.core.fetch.FetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.FetchHelper;
import com.jetbrains.youtrack.db.internal.core.fetch.FetchListener;
import com.jetbrains.youtrack.db.internal.core.fetch.FetchPlan;
import com.jetbrains.youtrack.db.internal.core.fetch.remote.RemoteFetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.remote.RemoteFetchListener;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.query.live.LiveQueryHookV2;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.sql.parser.LocalResultSetLifecycleDecorator;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.RecordMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.config.ClusterBasedStorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.TreeInternal;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.EdgeBTree;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionOptimistic;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.AbstractCommandResultListener;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.AsyncCommandResultListener;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.HandshakeInfo;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.LiveCommandResultListener;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.NetworkProtocolBinary;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.SyncCommandResultListener;
import com.jetbrains.youtrack.db.internal.server.tx.FrontendTransactionOptimisticServer;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConnectionBinaryExecutor implements BinaryRequestExecutor {

  private final ClientConnection connection;
  private final YouTrackDBServer server;
  private final HandshakeInfo handshakeInfo;

  public ConnectionBinaryExecutor(ClientConnection connection, YouTrackDBServer server) {
    this(connection, server, null);
  }

  public ConnectionBinaryExecutor(
      ClientConnection connection, YouTrackDBServer server, HandshakeInfo handshakeInfo) {
    this.connection = connection;
    this.server = server;
    this.handshakeInfo = handshakeInfo;
  }

  @Override
  public ListDatabasesResponse executeListDatabases(ListDatabasesRequest request) {

    Set<String> dbs = server.listDatabases();
    String listener =
        server.getListenerByProtocol(NetworkProtocolBinary.class).getInboundAddr().toString();
    Map<String, String> toSend = new HashMap<>();
    for (String dbName : dbs) {
      toSend.put(dbName, "remote:" + listener + "/" + dbName);
    }
    return new ListDatabasesResponse(toSend);
  }

  @Override
  public BinaryResponse executeServerInfo(ServerInfoRequest request) {
    try {
      return new ServerInfoResponse(ServerInfo.getServerInfo(server));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public BinaryResponse executeDBReload(ReloadRequest request) {
    final DatabaseSessionInternal db = connection.getDatabase();
    final Collection<String> clusters = db.getClusterNames();

    String[] clusterNames = new String[clusters.size()];
    int[] clusterIds = new int[clusterNames.length];

    int counter = 0;
    for (final String name : clusters) {
      final int clusterId = db.getClusterIdByName(name);
      if (clusterId >= 0) {
        clusterNames[counter] = name;
        clusterIds[counter] = clusterId;
        counter++;
      }
    }

    if (counter < clusters.size()) {
      clusterNames = Arrays.copyOf(clusterNames, counter);
      clusterIds = Arrays.copyOf(clusterIds, counter);
    }

    return new ReloadResponse(clusterNames, clusterIds);
  }

  @Override
  public BinaryResponse executeDBReload(ReloadRequest37 request) {
    return new ReloadResponse37(connection.getDatabase().getStorage().getConfiguration());
  }

  @Override
  public BinaryResponse executeCreateDatabase(CreateDatabaseRequest request) {

    if (server.existsDatabase(request.getDatabaseName())) {
      throw new DatabaseException(
          "Database named '" + request.getDatabaseName() + "' already exists");
    }
    if (request.getBackupPath() != null && !"".equals(request.getBackupPath().trim())) {
      server.restore(request.getDatabaseName(), request.getBackupPath());
    } else {
      server.createDatabase(
          request.getDatabaseName(),
          DatabaseType.valueOf(request.getStorageMode().toUpperCase(Locale.ENGLISH)),
          null);
    }
    LogManager.instance()
        .info(
            this,
            "Created database '%s' of type '%s'",
            request.getDatabaseName(),
            request.getStorageMode());

    // TODO: it should be here an additional check for open with the right user
    connection.setDatabase(
        server
            .getDatabases()
            .openNoAuthenticate(request.getDatabaseName(),
                connection.getServerUser().getName(null)));

    return new CreateDatabaseResponse();
  }

  @Override
  public BinaryResponse executeClose(CloseRequest request) {
    server.getClientConnectionManager().disconnect(connection);
    return null;
  }

  @Override
  public BinaryResponse executeExistDatabase(ExistsDatabaseRequest request) {
    boolean result = server.existsDatabase(request.getDatabaseName());
    return new ExistsDatabaseResponse(result);
  }

  @Override
  public BinaryResponse executeDropDatabase(DropDatabaseRequest request) {

    server.dropDatabase(request.getDatabaseName());
    LogManager.instance().info(this, "Dropped database '%s'", request.getDatabaseName());
    connection.close();
    return new DropDatabaseResponse();
  }

  @Override
  public BinaryResponse executeGetSize(GetSizeRequest request) {
    var db = connection.getDatabase();
    return new GetSizeResponse(db.getStorage().getSize(db));
  }

  @Override
  public BinaryResponse executeCountRecords(CountRecordsRequest request) {
    var db = connection.getDatabase();
    return new CountRecordsResponse(db.getStorage().countRecords(db));
  }

  @Override
  public BinaryResponse executeCountCluster(CountRequest request) {
    final long count =
        connection
            .getDatabase()
            .countClusterElements(request.getClusterIds(), request.isCountTombstones());
    return new CountResponse(count);
  }

  @Override
  public BinaryResponse executeClusterDataRange(GetClusterDataRangeRequest request) {
    final long[] pos = connection.getDatabase().getClusterDataRange(request.getClusterId());
    return new GetClusterDataRangeResponse(pos);
  }

  @Override
  public BinaryResponse executeAddCluster(AddClusterRequest request) {
    final int num;
    if (request.getRequestedId() < 0) {
      num = connection.getDatabase().addCluster(request.getClusterName());
    } else {
      num = connection.getDatabase().addCluster(request.getClusterName(), request.getRequestedId());
    }

    return new AddClusterResponse(num);
  }

  @Override
  public BinaryResponse executeDropCluster(DropClusterRequest request) {
    final String clusterName = connection.getDatabase().getClusterNameById(request.getClusterId());
    if (clusterName == null) {
      throw new IllegalArgumentException(
          "Cluster "
              + request.getClusterId()
              + " does not exist anymore. Refresh the db structure or just reconnect to the"
              + " database");
    }

    boolean result = connection.getDatabase().dropCluster(clusterName);
    return new DropClusterResponse(result);
  }

  @Override
  public BinaryResponse executeGetRecordMetadata(GetRecordMetadataRequest request) {
    final RecordMetadata metadata = connection.getDatabase().getRecordMetadata(request.getRid());
    if (metadata != null) {
      return new GetRecordMetadataResponse(metadata);
    } else {
      throw new DatabaseException(
          String.format("Record metadata for RID: %s, Not found", request.getRid()));
    }
  }

  @Override
  public BinaryResponse executeReadRecord(ReadRecordRequest request) {
    final RecordId rid = request.getRid();
    final String fetchPlanString = request.getFetchPlan();
    boolean ignoreCache = false;
    ignoreCache = request.isIgnoreCache();

    boolean loadTombstones = false;
    loadTombstones = request.isLoadTumbstone();
    ReadRecordResponse response;
    if (rid.getClusterId() == 0 && rid.getClusterPosition() == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      FetchHelper.checkFetchPlanValid(fetchPlanString);

      final byte[] record =
          ((ClusterBasedStorageConfiguration)
              connection.getDatabase().getStorageInfo().getConfiguration())
              .toStream(connection.getData().protocolVersion, StandardCharsets.UTF_8);

      response = new ReadRecordResponse(Blob.RECORD_TYPE, 0, record, new HashSet<>());

    } else {
      try {
        var db = connection.getDatabase();
        final RecordAbstract record = db.load(rid);
        assert !record.isUnloaded();
        byte[] bytes = getRecordBytes(connection, record);
        final Set<RecordAbstract> recordsToSend = new HashSet<>();
        if (!fetchPlanString.isEmpty()) {
          // BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
          // PLAN
          if (record instanceof EntityImpl entity) {
            final FetchPlan fetchPlan = FetchHelper.buildFetchPlan(fetchPlanString);

            final FetchListener listener =
                new RemoteFetchListener() {
                  @Override
                  protected void sendRecord(RecordAbstract iLinked) {
                    recordsToSend.add(iLinked);
                  }
                };
            final FetchContext context = new RemoteFetchContext();
            FetchHelper.fetch(db, entity, entity, fetchPlan, listener, context, "");
          }
        }
        response =
            new ReadRecordResponse(
                RecordInternal.getRecordType(db, record), record.getVersion(), bytes,
                recordsToSend);
      } catch (RecordNotFoundException e) {
        response = new ReadRecordResponse((byte) 0, 0, null, null);
      }
    }
    return response;
  }

  @Override
  public BinaryResponse executeRecordExists(RecordExistsRequest request) {
    final RID rid = request.getRecordId();
    final boolean recordExists = connection.getDatabase().exists(rid);
    return new RecordExistsResponse(recordExists);
  }

  @Override
  public BinaryResponse executeCreateRecord(CreateRecordRequest request) {

    final Record record = request.getContent();
    RecordInternal.setIdentity(record, request.getRid());
    RecordInternal.setVersion(record, 0);
    if (record instanceof EntityImpl) {
      // Force conversion of value to class for trigger default values.
      EntityInternalUtils.autoConvertValueToClass(connection.getDatabase(), (EntityImpl) record);
    }
    connection.getDatabase().save(record);

    if (request.getMode() < 2) {
      Map<UUID, BonsaiCollectionPointer> changedIds;
      BTreeCollectionManager collectionManager =
          connection.getDatabase().getSbTreeCollectionManager();
      if (collectionManager != null) {
        changedIds = new HashMap<>(collectionManager.changedIds());
        collectionManager.clearChangedIds();
      } else {
        changedIds = new HashMap<>();
      }

      return new CreateRecordResponse(
          (RecordId) record.getIdentity(), record.getVersion(), changedIds);
    }
    return null;
  }

  @Override
  public BinaryResponse executeUpdateRecord(UpdateRecordRequest request) {

    DatabaseSessionInternal database = connection.getDatabase();
    final Record newRecord = request.getContent();
    RecordInternal.setIdentity(newRecord, request.getRid());
    RecordInternal.setVersion(newRecord, request.getVersion());

    RecordInternal.setContentChanged(newRecord, request.isUpdateContent());
    RecordInternal.getDirtyManager(database, newRecord).clearForSave();
    Record currentRecord = null;
    if (newRecord instanceof EntityImpl) {
      try {
        currentRecord = database.load(request.getRid());
      } catch (RecordNotFoundException e) {
        // MAINTAIN COHERENT THE BEHAVIOR FOR ALL THE STORAGE TYPES
        if (e.getCause() instanceof OfflineClusterException)
        //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
        {
          throw (OfflineClusterException) e.getCause();
        }
      }

      if (currentRecord == null) {
        throw new RecordNotFoundException(request.getRid());
      }

      ((EntityImpl) currentRecord).merge((EntityImpl) newRecord, false, false);
      if (request.isUpdateContent()) {
        ((EntityImpl) currentRecord).setDirty();
      }
    } else {
      currentRecord = newRecord;
    }

    RecordInternal.setVersion(currentRecord, request.getVersion());

    database.save(currentRecord);

    if (currentRecord
        .getIdentity()
        .toString()
        .equals(database.getStorageInfo().getConfiguration().getIndexMgrRecordId())) {
      // FORCE INDEX MANAGER UPDATE. THIS HAPPENS FOR DIRECT CHANGES FROM REMOTE LIKE IN GRAPH
      database.getMetadata().getIndexManagerInternal().reload(connection.getDatabase());
    }
    final int newVersion = currentRecord.getVersion();

    if (request.getMode() < 2) {
      Map<UUID, BonsaiCollectionPointer> changedIds;
      BTreeCollectionManager collectionManager =
          connection.getDatabase().getSbTreeCollectionManager();
      if (collectionManager != null) {
        changedIds = new HashMap<>(collectionManager.changedIds());
        collectionManager.clearChangedIds();
      } else {
        changedIds = new HashMap<>();
      }

      return new UpdateRecordResponse(newVersion, changedIds);
    }
    return null;
  }

  @Override
  public BinaryResponse executeHigherPosition(HigherPhysicalPositionsRequest request) {
    var db = connection.getDatabase();
    PhysicalPosition[] nextPositions =
        db
            .getStorage()
            .higherPhysicalPositions(db, request.getClusterId(), request.getClusterPosition());
    return new HigherPhysicalPositionsResponse(nextPositions);
  }

  @Override
  public BinaryResponse executeCeilingPosition(CeilingPhysicalPositionsRequest request) {
    var db = connection.getDatabase();
    final PhysicalPosition[] previousPositions =
        db.getStorage()
            .ceilingPhysicalPositions(db, request.getClusterId(), request.getPhysicalPosition());
    return new CeilingPhysicalPositionsResponse(previousPositions);
  }

  @Override
  public BinaryResponse executeLowerPosition(LowerPhysicalPositionsRequest request) {
    var db = connection.getDatabase();
    final PhysicalPosition[] previousPositions =
        db
            .getStorage()
            .lowerPhysicalPositions(db, request.getiClusterId(), request.getPhysicalPosition());
    return new LowerPhysicalPositionsResponse(previousPositions);
  }

  @Override
  public BinaryResponse executeFloorPosition(FloorPhysicalPositionsRequest request) {
    var db = connection.getDatabase();
    final PhysicalPosition[] previousPositions =
        db
            .getStorage()
            .floorPhysicalPositions(db, request.getClusterId(), request.getPhysicalPosition());
    return new FloorPhysicalPositionsResponse(previousPositions);
  }

  @Override
  public BinaryResponse executeCommand(CommandRequest request) {
    final boolean live = request.isLive();
    final boolean asynch = request.isAsynch();

    CommandRequestText command = request.getQuery();

    final Map<Object, Object> params = command.getParameters();

    if (asynch && command instanceof SQLSynchQuery) {
      // CONVERT IT IN ASYNCHRONOUS QUERY
      final SQLAsynchQuery asynchQuery = new SQLAsynchQuery(command.getText());
      asynchQuery.setFetchPlan(command.getFetchPlan());
      asynchQuery.setLimit(command.getLimit());
      asynchQuery.setTimeout(command.getTimeoutTime(), command.getTimeoutStrategy());
      asynchQuery.setUseCache(((SQLSynchQuery) command).isUseCache());
      command = asynchQuery;
    }

    connection.getData().commandDetail = command.getText();

    connection.getData().command = command;
    AbstractCommandResultListener listener = null;
    LiveCommandResultListener liveListener = null;

    CommandResultListener cmdResultListener = command.getResultListener();

    if (live) {
      liveListener = new LiveCommandResultListener(server, connection, cmdResultListener);
      listener = new SyncCommandResultListener(null);
      command.setResultListener(liveListener);
    } else {
      if (asynch) {
        listener = new AsyncCommandResultListener(connection, cmdResultListener);
        command.setResultListener(listener);
      } else {
        listener = new SyncCommandResultListener(null);
      }
    }

    final long serverTimeout =
        connection
            .getDatabase()
            .getConfiguration()
            .getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT);

    if (serverTimeout > 0 && command.getTimeoutTime() > serverTimeout)
    // FORCE THE SERVER'S TIMEOUT
    {
      command.setTimeout(serverTimeout, command.getTimeoutStrategy());
    }

    // REQUEST CAN'T MODIFY THE RESULT, SO IT'S CACHEABLE
    command.setCacheableResult(true);

    // ASSIGNED THE PARSED FETCHPLAN
    var db = connection.getDatabase();
    final CommandRequestText commandRequest = db.command(command);
    listener.setFetchPlan(commandRequest.getFetchPlan());
    CommandResponse response;
    if (asynch) {
      // In case of async it execute the request during the write of the response
      response =
          new CommandResponse(
              null, listener, false, asynch, connection.getDatabase(), command, params);
    } else {
      // SYNCHRONOUS
      final Object result;
      if (params == null) {
        result = commandRequest.execute(db);
      } else {
        result = commandRequest.execute(db, params);
      }

      // FETCHPLAN HAS TO BE ASSIGNED AGAIN, because it can be changed by SQL statement
      listener.setFetchPlan(commandRequest.getFetchPlan());
      boolean isRecordResultSet = true;
      isRecordResultSet = command.isRecordResultSet();
      response =
          new CommandResponse(
              result,
              listener,
              isRecordResultSet,
              asynch,
              connection.getDatabase(),
              command,
              params);
    }
    return response;
  }

  @Override
  public BinaryResponse executeCommit(final CommitRequest request) {
    var recordOperations = request.getOperations();
    var database = connection.getDatabase();
    var tx = database.getTransaction();

    if (!tx.isActive()) {
      throw new DatabaseException("There is no active transaction on server.");
    }
    if (tx.getId() != request.getTxId()) {
      throw new DatabaseException(
          "Invalid transaction id, expected " + tx.getId() + " but received " + request.getTxId());
    }

    if (!(tx instanceof FrontendTransactionOptimisticServer serverTransaction)) {
      throw new DatabaseException(
          "Invalid transaction type,"
              + " expected FrontendTransactionOptimisticServer but found "
              + tx.getClass().getName());
    }

    try {
      try {
        serverTransaction.mergeReceivedTransaction(recordOperations);
      } catch (final RecordNotFoundException e) {
        throw e.getCause() instanceof OfflineClusterException
            ? (OfflineClusterException) e.getCause()
            : e;
      }
      try {
        try {
          serverTransaction.commit();
        } catch (final RecordNotFoundException e) {
          throw e.getCause() instanceof OfflineClusterException
              ? (OfflineClusterException) e.getCause()
              : e;
        }
        final BTreeCollectionManager collectionManager =
            connection.getDatabase().getSbTreeCollectionManager();
        Map<UUID, BonsaiCollectionPointer> changedIds = null;
        if (collectionManager != null) {
          changedIds = collectionManager.changedIds();
        }

        return new CommitResponse(serverTransaction.getGeneratedOriginalRecordIdMap(), changedIds);
      } catch (final RuntimeException e) {
        if (serverTransaction.isActive()) {
          database.rollback(true);
        }

        final BTreeCollectionManager collectionManager =
            connection.getDatabase().getSbTreeCollectionManager();
        if (collectionManager != null) {
          collectionManager.clearChangedIds();
        }

        throw e;
      }
    } catch (final RuntimeException e) {
      // Error during TX initialization, possibly index constraints violation.
      if (serverTransaction.isActive()) {
        database.rollback(true);
      }
      throw e;
    }
  }

  @Override
  public BinaryResponse executeGetGlobalConfiguration(GetGlobalConfigurationRequest request) {
    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(request.getKey());
    String cfgValue = cfg != null ? cfg.isHidden() ? "<hidden>" : cfg.getValueAsString() : "";
    return new GetGlobalConfigurationResponse(cfgValue);
  }

  @Override
  public BinaryResponse executeListGlobalConfigurations(ListGlobalConfigurationsRequest request) {
    Map<String, String> configs = new HashMap<>();
    for (GlobalConfiguration cfg : GlobalConfiguration.values()) {
      String key;
      try {
        key = cfg.getKey();
      } catch (Exception e) {
        key = "?";
      }

      String value;
      if (cfg.isHidden()) {
        value = "<hidden>";
      } else {
        try {
          ContextConfiguration config =
              connection.getProtocol().getServer().getContextConfiguration();
          value = config.getValueAsString(cfg) != null ? config.getValueAsString(cfg) : "";
        } catch (Exception e) {
          value = "";
        }
      }
      configs.put(key, value);
    }
    return new ListGlobalConfigurationsResponse(configs);
  }

  @Override
  public BinaryResponse executeFreezeDatabase(FreezeDatabaseRequest request) {
    DatabaseSessionInternal database =
        server
            .getDatabases()
            .openNoAuthenticate(request.getName(), connection.getServerUser().getName(null));
    connection.setDatabase(database);

    LogManager.instance().info(this, "Freezing database '%s'", connection.getDatabase().getURL());

    connection.getDatabase().freeze(true);
    return new FreezeDatabaseResponse();
  }

  @Override
  public BinaryResponse executeReleaseDatabase(ReleaseDatabaseRequest request) {
    DatabaseSessionInternal database =
        server
            .getDatabases()
            .openNoAuthenticate(request.getName(), connection.getServerUser().getName(null));

    connection.setDatabase(database);

    LogManager.instance().info(this, "Realising database '%s'", connection.getDatabase().getURL());

    connection.getDatabase().release();
    return new ReleaseDatabaseResponse();
  }

  @Override
  public BinaryResponse executeCleanOutRecord(CleanOutRecordRequest request) {
    connection.getDatabase().cleanOutRecord(request.getRecordId(), request.getRecordVersion());

    if (request.getMode() < 2) {
      return new CleanOutRecordResponse(true);
    }
    return null;
  }

  @Override
  public BinaryResponse executeSBTreeCreate(SBTCreateTreeRequest request) {
    BonsaiCollectionPointer collectionPointer = null;
    try {
      final DatabaseSessionInternal database = connection.getDatabase();
      final AbstractPaginatedStorage storage = (AbstractPaginatedStorage) database.getStorage();
      final AtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
      collectionPointer =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  connection
                      .getDatabase()
                      .getSbTreeCollectionManager()
                      .createSBTree(request.getClusterId(), atomicOperation, null));
    } catch (IOException e) {
      throw BaseException.wrapException(new DatabaseException("Error during ridbag creation"), e);
    }

    return new SBTCreateTreeResponse(collectionPointer);
  }

  @Override
  public BinaryResponse executeSBTGet(SBTGetRequest request) {
    final BTreeCollectionManager bTreeCollectionManager =
        connection.getDatabase().getSbTreeCollectionManager();
    final EdgeBTree<RID, Integer> tree =
        bTreeCollectionManager.loadSBTree(request.getCollectionPointer());
    try {
      var key = tree.getKeySerializer().deserialize(request.getKeyStream(), 0);
      Integer result = tree.get(key);
      final BinarySerializer<? super Integer> valueSerializer;
      if (result == null) {
        valueSerializer = NullSerializer.INSTANCE;
      } else {
        valueSerializer = tree.getValueSerializer();
      }

      byte[] stream = new byte[ByteSerializer.BYTE_SIZE + valueSerializer.getObjectSize(result)];
      ByteSerializer.INSTANCE.serialize(valueSerializer.getId(), stream, 0);
      valueSerializer.serialize(result, stream, ByteSerializer.BYTE_SIZE);
      return new SBTGetResponse(stream);
    } finally {
      bTreeCollectionManager.releaseSBTree(request.getCollectionPointer());
    }
  }

  @Override
  public BinaryResponse executeSBTFirstKey(SBTFirstKeyRequest request) {

    final BTreeCollectionManager bTreeCollectionManager =
        connection.getDatabase().getSbTreeCollectionManager();
    final EdgeBTree<RID, Integer> tree =
        bTreeCollectionManager.loadSBTree(request.getCollectionPointer());
    byte[] stream;
    try {

      Identifiable result = tree.firstKey();
      final BinarySerializer keySerializer;
      if (result == null) {
        keySerializer = NullSerializer.INSTANCE;
      } else {
        keySerializer = tree.getKeySerializer();
      }

      stream = new byte[ByteSerializer.BYTE_SIZE + keySerializer.getObjectSize(result)];
      ByteSerializer.INSTANCE.serialize(keySerializer.getId(), stream, 0);
      keySerializer.serialize(result, stream, ByteSerializer.BYTE_SIZE);
      return new SBTFirstKeyResponse(stream);
    } finally {
      bTreeCollectionManager.releaseSBTree(request.getCollectionPointer());
    }
  }

  @Override
  public BinaryResponse executeSBTFetchEntriesMajor(
      @SuppressWarnings("rawtypes") SBTFetchEntriesMajorRequest request) {

    final BTreeCollectionManager bTreeCollectionManager =
        connection.getDatabase().getSbTreeCollectionManager();
    final EdgeBTree<RID, Integer> tree =
        bTreeCollectionManager.loadSBTree(request.getPointer());
    try {
      final BinarySerializer<RID> keySerializer = tree.getKeySerializer();
      RID key = keySerializer.deserialize(request.getKeyStream(), 0);

      final BinarySerializer<Integer> valueSerializer = tree.getValueSerializer();

      TreeInternal.AccumulativeListener<RID, Integer> listener =
          new TreeInternal.AccumulativeListener<>(request.getPageSize());
      tree.loadEntriesMajor(key, request.isInclusive(), true, listener);
      List<Entry<RID, Integer>> result = listener.getResult();
      return new SBTFetchEntriesMajorResponse<>(keySerializer, valueSerializer, result);
    } finally {
      bTreeCollectionManager.releaseSBTree(request.getPointer());
    }
  }

  @Override
  public BinaryResponse executeSBTGetRealSize(SBTGetRealBagSizeRequest request) {
    final BTreeCollectionManager bTreeCollectionManager =
        connection.getDatabase().getSbTreeCollectionManager();
    final EdgeBTree<RID, Integer> tree =
        bTreeCollectionManager.loadSBTree(request.getCollectionPointer());
    try {
      int realSize = tree.getRealBagSize(request.getChanges());
      return new SBTGetRealBagSizeResponse(realSize);
    } finally {
      bTreeCollectionManager.releaseSBTree(request.getCollectionPointer());
    }
  }

  @Override
  public BinaryResponse executeIncrementalBackup(IncrementalBackupRequest request) {
    String fileName = connection.getDatabase()
        .incrementalBackup(Path.of(request.getBackupDirectory()));
    return new IncrementalBackupResponse(fileName);
  }

  @Override
  public BinaryResponse executeImport(ImportRequest request) {
    List<String> result = new ArrayList<>();
    LogManager.instance().info(this, "Starting database import");
    DatabaseImport imp;
    try {
      imp =
          new DatabaseImport(
              connection.getDatabase(),
              request.getImporPath(),
              iText -> {
                LogManager.instance().debug(ConnectionBinaryExecutor.this, iText);
                if (iText != null) {
                  result.add(iText);
                }
              });
      imp.setOptions(request.getOptions());
      imp.importDatabase();
      imp.close();
      new File(request.getImporPath()).delete();

    } catch (IOException e) {
      throw BaseException.wrapException(new DatabaseException("error on import"), e);
    }
    return new ImportResponse(result);
  }

  @Override
  public BinaryResponse executeConnect(ConnectRequest request) {
    BinaryProtocolHelper.checkProtocolVersion(this, request.getProtocolVersion());
    if (request.getProtocolVersion() > 36) {
      throw new ConfigurationException(
          "You can use connect as first operation only for protocol  < 37 please use handshake for"
              + " protocol >= 37");
    }
    connection.getData().driverName = request.getDriverName();
    connection.getData().driverVersion = request.getDriverVersion();
    connection.getData().protocolVersion = request.getProtocolVersion();
    connection.getData().clientId = request.getClientId();
    connection.getData().setSerializationImpl(request.getRecordFormat());

    connection.setTokenBased(request.isTokenBased());
    connection.getData().supportsLegacyPushMessages = request.isSupportPush();
    connection.getData().collectStats = request.isCollectStats();

    if (!request.isTokenBased()
        && !GlobalConfiguration.NETWORK_BINARY_ALLOW_NO_TOKEN.getValueAsBoolean()) {
      LogManager.instance()
          .warn(
              this,
              "Session open with token flag false is not supported anymore please use token based"
                  + " sessions");
      throw new ConfigurationException(
          "Session open with token flag false is not supported anymore please use token based"
              + " sessions");
    }

    connection.setServerUser(
        server.authenticateUser(request.getUsername(), request.getPassword(), "server.connect"));

    if (connection.getServerUser() == null) {
      throw new SecurityAccessException(
          "Wrong user/password to [connect] to the remote YouTrackDB Server instance");
    }
    byte[] token = null;
    if (connection.getData().protocolVersion > ChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      connection.getData().serverUsername = connection.getServerUser().getName(null);
      connection.getData().serverUser = true;

      if (Boolean.TRUE.equals(connection.getTokenBased())) {
        token = server.getTokenHandler().getSignedBinaryToken(null, null, connection.getData());
      } else {
        token = CommonConst.EMPTY_BYTE_ARRAY;
      }
    }

    return new ConnectResponse(connection.getId(), token);
  }

  @Override
  public BinaryResponse executeConnect37(Connect37Request request) {
    connection.getData().driverName = handshakeInfo.getDriverName();
    connection.getData().driverVersion = handshakeInfo.getDriverVersion();
    connection.getData().protocolVersion = handshakeInfo.getProtocolVersion();
    connection.getData().setSerializer(handshakeInfo.getSerializer());

    connection.setTokenBased(true);
    connection.getData().supportsLegacyPushMessages = false;
    connection.getData().collectStats = true;

    connection.setServerUser(
        server.authenticateUser(request.getUsername(), request.getPassword(), "server.connect"));

    if (connection.getServerUser() == null) {
      throw new SecurityAccessException(
          "Wrong user/password to [connect] to the remote YouTrackDB Server instance");
    }

    byte[] token = null;
    if (connection.getData().protocolVersion > ChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      connection.getData().serverUsername = connection.getServerUser().getName(null);
      connection.getData().serverUser = true;

      if (Boolean.TRUE.equals(connection.getTokenBased())) {
        token = server.getTokenHandler().getSignedBinaryToken(null, null, connection.getData());
      } else {
        token = CommonConst.EMPTY_BYTE_ARRAY;
      }
    }

    return new ConnectResponse(connection.getId(), token);
  }

  @Override
  public BinaryResponse executeDatabaseOpen(OpenRequest request) {
    BinaryProtocolHelper.checkProtocolVersion(this, request.getProtocolVersion());
    if (request.getProtocolVersion() > 36) {
      throw new ConfigurationException(
          "You can use open as first operation only for protocol  < 37 please use handshake for"
              + " protocol >= 37");
    }
    connection.getData().driverName = request.getDriverName();
    connection.getData().driverVersion = request.getDriverVersion();
    connection.getData().protocolVersion = request.getProtocolVersion();
    connection.getData().clientId = request.getClientId();
    connection.getData().setSerializationImpl(request.getRecordFormat());
    if (!request.isUseToken()
        && !GlobalConfiguration.NETWORK_BINARY_ALLOW_NO_TOKEN.getValueAsBoolean()) {
      LogManager.instance()
          .warn(
              this,
              "Session open with token flag false is not supported anymore please use token based"
                  + " sessions");
      throw new ConfigurationException(
          "Session open with token flag false is not supported anymore please use token based"
              + " sessions");
    }
    connection.setTokenBased(request.isUseToken());
    connection.getData().supportsLegacyPushMessages = request.isSupportsPush();
    connection.getData().collectStats = request.isCollectStats();

    try {
      connection.setDatabase(
          server.openDatabase(
              request.getDatabaseName(),
              request.getUserName(),
              request.getUserPassword(),
              connection.getData()));
    } catch (BaseException e) {
      server.getClientConnectionManager().disconnect(connection);
      throw e;
    }

    byte[] token = null;

    if (Boolean.TRUE.equals(connection.getTokenBased())) {
      token =
          server
              .getTokenHandler()
              .getSignedBinaryToken(
                  connection.getDatabase(),
                  connection.getDatabase().geCurrentUser(),
                  connection.getData());
      // TODO: do not use the parse split getSignedBinaryToken in two methods.
      server.getClientConnectionManager().connect(connection.getProtocol(), connection, token);
    }

    DatabaseSessionInternal db = connection.getDatabase();
    final Collection<String> clusters = db.getClusterNames();
    final byte[] tokenToSend;
    if (Boolean.TRUE.equals(connection.getTokenBased())) {
      tokenToSend = token;
    } else {
      tokenToSend = CommonConst.EMPTY_BYTE_ARRAY;
    }

    byte[] distriConf = null;

    String[] clusterNames = new String[clusters.size()];
    int[] clusterIds = new int[clusters.size()];

    int counter = 0;
    for (String name : clusters) {
      final int clusterId = db.getClusterIdByName(name);
      if (clusterId >= 0) {
        clusterNames[counter] = name;
        clusterIds[counter] = clusterId;
        counter++;
      }
    }

    if (counter < clusters.size()) {
      clusterNames = Arrays.copyOf(clusterNames, counter);
      clusterIds = Arrays.copyOf(clusterIds, counter);
    }

    return new OpenResponse(
        connection.getId(),
        tokenToSend,
        clusterIds,
        clusterNames,
        distriConf,
        YouTrackDBConstants.getVersion());
  }

  @Override
  public BinaryResponse executeDatabaseOpen37(Open37Request request) {
    connection.setTokenBased(true);
    connection.getData().supportsLegacyPushMessages = false;
    connection.getData().collectStats = true;
    connection.getData().driverName = handshakeInfo.getDriverName();
    connection.getData().driverVersion = handshakeInfo.getDriverVersion();
    connection.getData().protocolVersion = handshakeInfo.getProtocolVersion();
    connection.getData().setSerializer(handshakeInfo.getSerializer());
    try {
      connection.setDatabase(
          server.openDatabase(
              request.getDatabaseName(),
              request.getUserName(),
              request.getUserPassword(),
              connection.getData()));
    } catch (BaseException e) {
      server.getClientConnectionManager().disconnect(connection);
      throw e;
    }

    byte[] token = null;

    token =
        server
            .getTokenHandler()
            .getSignedBinaryToken(
                connection.getDatabase(), connection.getDatabase().geCurrentUser(),
                connection.getData());
    // TODO: do not use the parse split getSignedBinaryToken in two methods.
    server.getClientConnectionManager().connect(connection.getProtocol(), connection, token);

    return new Open37Response(connection.getId(), token);
  }

  @Override
  public BinaryResponse executeShutdown(ShutdownRequest request) {

    LogManager.instance().info(this, "Received shutdown command from the remote client ");

    final String user = request.getRootUser();
    final String passwd = request.getRootPassword();

    if (server.authenticate(user, passwd, "server.shutdown")) {
      LogManager.instance()
          .info(this, "Remote client authenticated. Starting shutdown of server...");

      runShutdownInNonDaemonThread();

      return new ShutdownResponse();
    }

    LogManager.instance()
        .error(this, "Authentication error of remote client: shutdown is aborted.", null);

    throw new SecurityAccessException("Invalid user/password to shutdown the server");
  }

  private void runShutdownInNonDaemonThread() {
    Thread shutdownThread =
        new Thread("YouTrackDB server shutdown thread") {
          public void run() {
            server.shutdown();
          }
        };
    shutdownThread.setDaemon(false);
    shutdownThread.start();
  }

  @Override
  public BinaryResponse executeReopen(ReopenRequest request) {
    return new ReopenResponse(connection.getId());
  }

  @Override
  public BinaryResponse executeSetGlobalConfig(SetGlobalConfigurationRequest request) {

    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(request.getKey());

    if (cfg != null) {
      cfg.setValue(request.getValue());
      if (!cfg.isChangeableAtRuntime()) {
        throw new ConfigurationException(
            "Property '"
                + request.getKey()
                + "' cannot be changed at runtime. Change the setting at startup");
      }
    } else {
      throw new ConfigurationException(
          "Property '" + request.getKey() + "' was not found in global configuration");
    }

    return new SetGlobalConfigurationResponse();
  }

  public static byte[] getRecordBytes(ClientConnection connection,
      final RecordAbstract iRecord) {
    var db = connection.getDatabase();
    final byte[] stream;
    String name = connection.getData().getSerializationImpl();
    if (RecordInternal.getRecordType(db, iRecord) == EntityImpl.RECORD_TYPE) {
      ((EntityImpl) iRecord).deserializeFields();
      RecordSerializer ser = RecordSerializerFactory.instance().getFormat(name);
      stream = ser.toStream(db, iRecord);
    } else {
      stream = iRecord.toStream();
    }

    return stream;
  }

  @Override
  public BinaryResponse executeServerQuery(ServerQueryRequest request) {
    YouTrackDB youTrackDB = server.getContext();

    ResultSet rs;

    if (request.isNamedParams()) {
      rs = youTrackDB.execute(request.getStatement(), request.getNamedParameters());
    } else {
      rs = youTrackDB.execute(request.getStatement(), request.getPositionalParameters());
    }

    // copy the result-set to make sure that the execution is successful
    List<Result> rsCopy = rs.stream().collect(Collectors.toList());

    return new ServerQueryResponse(
        ((LocalResultSetLifecycleDecorator) rs).getQueryId(),
        false,
        rsCopy,
        rs.getExecutionPlan(),
        false,
        rs.getQueryStats(),
        false);
  }

  @Override
  public BinaryResponse executeQuery(QueryRequest request) {
    DatabaseSessionInternal database = connection.getDatabase();
    QueryMetadataUpdateListener metadataListener = new QueryMetadataUpdateListener();
    database.getSharedContext().registerListener(metadataListener);
    if (database.getTransaction().isActive()) {
      ((FrontendTransactionOptimistic) database.getTransaction()).resetChangesTracking();
    }
    ResultSet rs;
    if (QueryRequest.QUERY == request.getOperationType()) {
      // TODO Assert is sql.
      if (request.isNamedParams()) {
        rs = database.query(request.getStatement(), request.getNamedParameters(database));
      } else {
        rs = database.query(request.getStatement(), request.getPositionalParameters(database));
      }
    } else {
      if (QueryRequest.COMMAND == request.getOperationType()) {
        if (request.isNamedParams()) {
          rs = database.command(request.getStatement(), request.getNamedParameters(database));
        } else {
          rs = database.command(request.getStatement(), request.getPositionalParameters(database));
        }
      } else {
        if (request.isNamedParams()) {
          rs =
              database.execute(
                  request.getLanguage(), request.getStatement(),
                  request.getNamedParameters(database));
        } else {
          rs =
              database.execute(
                  request.getLanguage(), request.getStatement(),
                  request.getPositionalParameters(database));
        }
      }
    }

    // copy the result-set to make sure that the execution is successful
    Stream<Result> stream = rs.stream();
    if (database
        .getActiveQueries()
        .containsKey(((LocalResultSetLifecycleDecorator) rs).getQueryId())) {
      stream = stream.limit(request.getRecordsPerPage());
    }
    List<Result> rsCopy = stream.collect(Collectors.toList());

    boolean hasNext = rs.hasNext();
    boolean txChanges = false;
    if (database.getTransaction().isActive()) {
      txChanges = ((FrontendTransactionOptimistic) database.getTransaction()).isChanged();
    }
    database.getSharedContext().unregisterListener(metadataListener);

    return new QueryResponse(
        ((LocalResultSetLifecycleDecorator) rs).getQueryId(),
        txChanges,
        rsCopy,
        rs.getExecutionPlan(),
        hasNext,
        rs.getQueryStats(),
        metadataListener.isUpdated());
  }

  @Override
  public BinaryResponse closeQuery(CloseQueryRequest oQueryRequest) {
    String queryId = oQueryRequest.getQueryId();
    DatabaseSessionInternal db = connection.getDatabase();
    ResultSet query = db.getActiveQuery(queryId);
    if (query != null) {
      query.close();
    }
    return new CloseQueryResponse();
  }

  @Override
  public BinaryResponse executeQueryNextPage(QueryNextPageRequest request) {
    DatabaseSessionInternal database = connection.getDatabase();
    YouTrackDBInternal orientDB = database.getSharedContext().getYouTrackDB();
    LocalResultSetLifecycleDecorator rs =
        (LocalResultSetLifecycleDecorator) database.getActiveQuery(request.getQueryId());

    if (rs == null) {
      throw new DatabaseException(
          String.format(
              "No query with id '%s' found probably expired session", request.getQueryId()));
    }

    try {
      orientDB.startCommand(Optional.empty());
      // copy the result-set to make sure that the execution is successful
      List<Result> rsCopy = new ArrayList<>(request.getRecordsPerPage());
      int i = 0;
      // if it's InternalResultSet it means that it's a Command, not a Query, so the result has to
      // be
      // sent as it is, not streamed
      while (rs.hasNext() && (rs.isDetached() || i < request.getRecordsPerPage())) {
        rsCopy.add(rs.next());
        i++;
      }
      boolean hasNext = rs.hasNext();
      return new QueryResponse(
          rs.getQueryId(),
          false,
          rsCopy,
          rs.getExecutionPlan(),
          hasNext,
          rs.getQueryStats(),
          false);
    } finally {
      orientDB.endCommand();
    }
  }

  @Override
  public BinaryResponse executeBeginTransaction(BeginTransactionRequest request) {
    var database = connection.getDatabase();
    var tx = database.getTransaction();

    var recordOperations = request.getOperations();
    if (tx.isActive()) {
      if (!(tx instanceof FrontendTransactionOptimisticServer serverTransaction)) {
        throw new DatabaseException("Non-server based transaction is active");
      }
      if (tx.getId() != request.getTxId()) {
        throw new DatabaseException(
            "Transaction id mismatch, expected " + tx.getId() + " but got " + request.getTxId());
      }

      try {
        serverTransaction.mergeReceivedTransaction(recordOperations);
      } catch (final RecordNotFoundException e) {
        throw e.getCause() instanceof OfflineClusterException
            ? (OfflineClusterException) e.getCause()
            : e;
      }

      return new BeginTransactionResponse(
          tx.getId(), serverTransaction.getGeneratedOriginalRecordIdMap());
    }

    database.begin(new FrontendTransactionOptimisticServer(database, request.getTxId()));
    var serverTransaction = (FrontendTransactionOptimisticServer) database.getTransaction();

    try {
      serverTransaction.mergeReceivedTransaction(recordOperations);
    } catch (final RecordNotFoundException e) {
      throw e.getCause() instanceof OfflineClusterException
          ? (OfflineClusterException) e.getCause()
          : e;
    }

    return new BeginTransactionResponse(
        tx.getId(), serverTransaction.getGeneratedOriginalRecordIdMap());
  }

  @Override
  public BinaryResponse executeBeginTransaction38(BeginTransaction38Request request) {
    var database = connection.getDatabase();
    var recordOperations = request.getOperations();

    var tx = database.getTransaction();

    if (tx.isActive()) {
      throw new DatabaseException("Transaction is already started on server");
    }

    var serverTransaction =
        doExecuteBeginTransaction(request.getTxId(), database, recordOperations);
    return new BeginTransactionResponse(
        tx.getId(), serverTransaction.getGeneratedOriginalRecordIdMap());
  }

  private static FrontendTransactionOptimisticServer doExecuteBeginTransaction(
      long txId, DatabaseSessionInternal database,
      List<RecordOperationRequest> recordOperations) {
    assert database.activeTxCount() == 0;

    database.begin(new FrontendTransactionOptimisticServer(database, txId));
    var serverTransaction = (FrontendTransactionOptimisticServer) database.getTransaction();

    try {
      serverTransaction.mergeReceivedTransaction(recordOperations);
    } catch (final RecordNotFoundException e) {
      throw e.getCause() instanceof OfflineClusterException
          ? (OfflineClusterException) e.getCause()
          : e;
    }

    return serverTransaction;
  }

  @Override
  public BinaryResponse executeSendTransactionState(SendTransactionStateRequest request) {
    var database = connection.getDatabase();
    var recordOperations = request.getOperations();

    var tx = database.getTransaction();

    if (!tx.isActive()) {
      throw new DatabaseException(
          "Transaction with id " + request.getTxId() + " is not active on server.");
    }

    if (!(tx instanceof FrontendTransactionOptimisticServer serverTransaction)) {
      throw new DatabaseException(
          "Invalid transaction type,"
              + " expected FrontendTransactionOptimisticServer but found "
              + tx.getClass().getName());
    }

    try {
      serverTransaction.mergeReceivedTransaction(recordOperations);
    } catch (final RecordNotFoundException e) {
      throw e.getCause() instanceof OfflineClusterException
          ? (OfflineClusterException) e.getCause()
          : e;
    }

    return new SendTransactionStateResponse(
        tx.getId(), serverTransaction.getGeneratedOriginalRecordIdMap());
  }

  @Override
  public BinaryResponse executeCommit38(Commit38Request request) {
    var recordOperations = request.getOperations();

    var database = connection.getDatabase();
    var tx = database.getTransaction();

    var started = tx.isActive();
    if (!started) {
      //case when transaction was sent during commit.
      tx = doExecuteBeginTransaction(request.getTxId(), database, recordOperations);
    }

    if (tx.getId() != request.getTxId()) {
      throw new DatabaseException(
          "Invalid transaction id, expected " + tx.getId() + " but received " + request.getTxId());
    }

    if (!(tx instanceof FrontendTransactionOptimisticServer serverTransaction)) {
      throw new DatabaseException(
          "Invalid transaction type,"
              + " expected FrontendTransactionOptimisticServer but found "
              + tx.getClass().getName());
    }

    try {
      try {
        if (started) {
          serverTransaction.mergeReceivedTransaction(recordOperations);
        }
      } catch (final RecordNotFoundException e) {
        throw e.getCause() instanceof OfflineClusterException
            ? (OfflineClusterException) e.getCause()
            : e;
      }

      if (serverTransaction.getTxStartCounter() != 1) {
        throw new DatabaseException("Transaction can be started only once on server");
      }

      try {
        try {
          database.commit();
        } catch (final RecordNotFoundException e) {
          throw e.getCause() instanceof OfflineClusterException
              ? (OfflineClusterException) e.getCause()
              : e;
        }
        final BTreeCollectionManager collectionManager =
            connection.getDatabase().getSbTreeCollectionManager();
        Map<UUID, BonsaiCollectionPointer> changedIds = null;

        if (collectionManager != null) {
          changedIds = collectionManager.changedIds();
        }

        return new Commit37Response(serverTransaction.getGeneratedOriginalRecordIdMap(),
            changedIds);
      } catch (final RuntimeException e) {
        if (serverTransaction.isActive()) {
          database.rollback(true);
        }

        final BTreeCollectionManager collectionManager =
            connection.getDatabase().getSbTreeCollectionManager();
        if (collectionManager != null) {
          collectionManager.clearChangedIds();
        }

        throw e;
      }
    } catch (final RuntimeException e) {
      // Error during TX initialization, possibly index constraints violation.
      if (serverTransaction.isActive()) {
        database.rollback(true);
      }
      throw e;
    }
  }

  @Override
  public BinaryResponse executeCommit37(Commit37Request request) {
    var recordOperations = request.getOperations();
    var database = connection.getDatabase();
    var tx = database.getTransaction();

    if (!tx.isActive()) {
      tx = doExecuteBeginTransaction(request.getTxId(), database, recordOperations);
    }

    if (tx.getId() != request.getTxId()) {
      throw new DatabaseException(
          "Invalid transaction id, expected " + tx.getId() + " but received " + request.getTxId());
    }

    if (!(tx instanceof FrontendTransactionOptimisticServer serverTransaction)) {
      throw new DatabaseException(
          "Invalid transaction type,"
              + " expected FrontendTransactionOptimisticServer but found "
              + tx.getClass().getName());
    }

    if (serverTransaction.getTxStartCounter() != 1) {
      throw new DatabaseException("Transaction can be started only once on server");
    }

    try {
      try {
        serverTransaction.mergeReceivedTransaction(recordOperations);
      } catch (final RecordNotFoundException e) {
        throw e.getCause() instanceof OfflineClusterException
            ? (OfflineClusterException) e.getCause()
            : e;
      }
      try {
        try {
          database.commit();
        } catch (final RecordNotFoundException e) {
          throw e.getCause() instanceof OfflineClusterException
              ? (OfflineClusterException) e.getCause()
              : e;
        }
        final BTreeCollectionManager collectionManager =
            connection.getDatabase().getSbTreeCollectionManager();
        Map<UUID, BonsaiCollectionPointer> changedIds = null;

        if (collectionManager != null) {
          changedIds = collectionManager.changedIds();
        }

        return new Commit37Response(serverTransaction.getGeneratedOriginalRecordIdMap(),
            changedIds);
      } catch (final RuntimeException e) {
        if (serverTransaction.isActive()) {
          database.rollback(true);
        }

        final BTreeCollectionManager collectionManager =
            connection.getDatabase().getSbTreeCollectionManager();
        if (collectionManager != null) {
          collectionManager.clearChangedIds();
        }

        throw e;
      }
    } catch (final RuntimeException e) {
      // Error during TX initialization, possibly index constraints violation.
      if (serverTransaction.isActive()) {
        database.rollback(true);
      }
      throw e;
    }
  }

  @Override
  public BinaryResponse executeFetchTransaction(FetchTransactionRequest request) {
    DatabaseSessionInternal database = connection.getDatabase();
    if (!database.getTransaction().isActive()) {
      throw new DatabaseException("No Transaction Active");
    }

    FrontendTransactionOptimistic tx = (FrontendTransactionOptimistic) database.getTransaction();
    if (tx.getId() != request.getTxId()) {
      throw new DatabaseException(
          "Invalid transaction id, expected " + tx.getId() + " but received " + request.getTxId());
    }

    return new FetchTransactionResponse(database,
        tx.getId(),
        tx.getRecordOperations(),
        tx.getGeneratedOriginalRecordIdMap());
  }

  @Override
  public BinaryResponse executeFetchTransaction38(FetchTransaction38Request request) {
    DatabaseSessionInternal database = connection.getDatabase();
    if (!database.getTransaction().isActive()) {
      throw new DatabaseException("No Transaction Active");
    }
    FrontendTransactionOptimistic tx = (FrontendTransactionOptimistic) database.getTransaction();
    if (tx.getId() != request.getTxId()) {
      throw new DatabaseException(
          "Invalid transaction id, expected " + tx.getId() + " but received " + request.getTxId());
    }

    return new FetchTransaction38Response(database,
        tx.getId(),
        tx.getRecordOperations(),
        Collections.emptyMap(),
        tx.getGeneratedOriginalRecordIdMap(),
        database);
  }

  @Override
  public BinaryResponse executeRollback(RollbackTransactionRequest request) {
    DatabaseSessionInternal database = connection.getDatabase();
    if (database.getTransaction().isActive()) {
      database.rollback(true);
    }
    return new RollbackTransactionResponse();
  }

  @Override
  public BinaryResponse executeSubscribe(SubscribeRequest request) {
    return new SubscribeResponse(request.getPushRequest().execute(this));
  }

  @Override
  public BinaryResponse executeUnsubscribe(UnsubscribeRequest request) {
    return new UnsubscribeResponse(request.getUnsubscribeRequest().execute(this));
  }

  @Override
  public BinaryResponse executeSubscribeStorageConfiguration(
      SubscribeStorageConfigurationRequest request) {
    PushManager manager = server.getPushManager();
    manager.subscribeStorageConfiguration(
        connection.getDatabase(), (NetworkProtocolBinary) connection.getProtocol());
    return new SubscribeStorageConfigurationResponse();
  }

  @Override
  public BinaryResponse executeSubscribeSchema(SubscribeSchemaRequest request) {
    PushManager manager = server.getPushManager();
    manager.subscribeSchema(
        connection.getDatabase(), (NetworkProtocolBinary) connection.getProtocol());
    return new SubscribeSchemaResponse();
  }

  @Override
  public BinaryResponse executeSubscribeIndexManager(SubscribeIndexManagerRequest request) {
    PushManager manager = server.getPushManager();
    manager.subscribeIndexManager(
        connection.getDatabase(), (NetworkProtocolBinary) connection.getProtocol());
    return new SubscribeIndexManagerResponse();
  }

  @Override
  public BinaryResponse executeSubscribeFunctions(SubscribeFunctionsRequest request) {
    PushManager manager = server.getPushManager();
    manager.subscribeFunctions(
        connection.getDatabase(), (NetworkProtocolBinary) connection.getProtocol());
    return new SubscribeFunctionsResponse();
  }

  @Override
  public BinaryResponse executeSubscribeSequences(SubscribeSequencesRequest request) {
    PushManager manager = server.getPushManager();
    manager.subscribeSequences(
        connection.getDatabase(), (NetworkProtocolBinary) connection.getProtocol());
    return new SubscribeSequencesResponse();
  }

  @Override
  public BinaryResponse executeUnsubscribeLiveQuery(UnsubscribeLiveQueryRequest request) {
    DatabaseSessionInternal database = connection.getDatabase();
    LiveQueryHookV2.unsubscribe(request.getMonitorId(), database);
    return new UnsubscribLiveQueryResponse();
  }

  @Override
  public BinaryResponse executeSubscribeLiveQuery(SubscribeLiveQueryRequest request) {
    NetworkProtocolBinary protocol = (NetworkProtocolBinary) connection.getProtocol();
    ServerLiveQueryResultListener listener =
        new ServerLiveQueryResultListener(protocol, connection.getDatabase().getSharedContext());
    LiveQueryMonitor monitor =
        connection.getDatabase().live(request.getQuery(), listener, request.getParams());
    listener.setMonitorId(monitor.getMonitorId());
    return new SubscribeLiveQueryResponse(monitor.getMonitorId());
  }
}
