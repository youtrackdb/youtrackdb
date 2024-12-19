package com.jetbrains.youtrack.db.internal.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.BeginTransactionResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit37Request;
import com.jetbrains.youtrack.db.internal.client.remote.message.Commit37Response;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.CreateRecordResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.FetchTransactionResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.QueryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.message.RebeginTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.RollbackTransactionRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UpdateRecordRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.UpdateRecordResponse;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.server.network.protocol.NetworkProtocolData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 *
 */
public class ConnectionExecutorTransactionTest {

  @Mock
  private YouTrackDBServer server;
  @Mock
  private ClientConnection connection;

  private YouTrackDB youTrackDb;
  private DatabaseSessionInternal db;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    youTrackDb = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        ConnectionExecutorTransactionTest.class.getSimpleName());
    db =
        (DatabaseSessionInternal)
            youTrackDb.open(
                ConnectionExecutorTransactionTest.class.getSimpleName(), "admin", "admin");
    db.createClass("test");
    NetworkProtocolData protocolData = new NetworkProtocolData();
    protocolData.setSerializer(RecordSerializerNetworkFactory.INSTANCE.current());
    Mockito.when(connection.getDatabase()).thenReturn(db);
    Mockito.when(connection.getData()).thenReturn(protocolData);
  }

  @After
  public void after() {
    db.close();
    youTrackDb.drop(ConnectionExecutorTransactionTest.class.getSimpleName());
    youTrackDb.close();
  }

  @Test
  public void testExecutionBeginTransaction() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db);
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);
    // TODO:Define properly what is the txId
    // assertEquals(((BeginTransactionResponse) response).getTxId(), request.getTxId());
  }

  @Test
  public void testExecutionBeginCommitTransaction() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db);
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));

    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    Commit37Request commit = new Commit37Request(db, 10, false, true, null);
    BinaryResponse commitResponse = commit.execute(executor);
    assertFalse(db.getTransaction().isActive());
    assertTrue(commitResponse instanceof Commit37Response);

    assertEquals(1, ((Commit37Response) commitResponse).getUpdatedRids().size());
  }

  @Test
  public void testExecutionReplaceCommitTransaction() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db);
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    rec.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    EntityImpl record1 = new EntityImpl(db, new RecordId(3, -3));
    record1.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record1, RecordOperation.CREATED));

    Commit37Request commit = new Commit37Request(db, 10, true, true, operations
    );
    BinaryResponse commitResponse = commit.execute(executor);
    assertFalse(db.getTransaction().isActive());
    assertTrue(commitResponse instanceof Commit37Response);
    assertEquals(2, ((Commit37Response) commitResponse).getUpdatedRids().size());
  }

  @Test
  public void testExecutionRebeginTransaction() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db);
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    rec.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));

    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    EntityImpl record1 = new EntityImpl(db, new RecordId(3, -3));
    record1.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record1, RecordOperation.CREATED));

    RebeginTransactionRequest rebegin =
        new RebeginTransactionRequest(db, 10, true, operations, new HashMap<>());
    BinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof BeginTransactionResponse);
    assertTrue(db.getTransaction().isActive());
    assertEquals(2, db.getTransaction().getEntryCount());
  }

  @Test
  public void testExecutionRebeginCommitTransaction() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db);
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    rec.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));

    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    EntityImpl record1 = new EntityImpl(db, new RecordId(3, -3));
    record1.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record1, RecordOperation.CREATED));

    RebeginTransactionRequest rebegin =
        new RebeginTransactionRequest(db, 10, true, operations, new HashMap<>());
    BinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof BeginTransactionResponse);
    assertTrue(db.getTransaction().isActive());
    assertEquals(2, db.getTransaction().getEntryCount());

    EntityImpl record2 = new EntityImpl(db, new RecordId(3, -4));
    record2.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record2, RecordOperation.CREATED));

    Commit37Request commit = new Commit37Request(db, 10, true, true, operations
    );
    BinaryResponse commitResponse = commit.execute(executor);
    assertFalse(db.getTransaction().isActive());
    assertTrue(commitResponse instanceof Commit37Response);
    assertEquals(3, ((Commit37Response) commitResponse).getUpdatedRids().size());
  }

  @Test
  public void testExecutionQueryChangesTracking() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db, "test");
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    QueryRequest query =
        new QueryRequest(db,
            "sql",
            "update test set name='bla'",
            new HashMap<>(),
            QueryRequest.COMMAND,
            RecordSerializerNetworkFactory.INSTANCE.current(), 20);
    QueryResponse queryResponse = (QueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());
  }

  @Test
  public void testBeginChangeFetchTransaction() {

    db.begin();
    db.save(db.newEntity("test"));
    db.commit();

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db, "test");
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    QueryRequest query =
        new QueryRequest(db,
            "sql",
            "update test set name='bla'",
            new HashMap<>(),
            QueryRequest.COMMAND,
            RecordSerializerNetworkFactory.INSTANCE.current(), 20);
    QueryResponse queryResponse = (QueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());

    FetchTransactionRequest fetchRequest = new FetchTransactionRequest(10);

    FetchTransactionResponse response1 =
        (FetchTransactionResponse) fetchRequest.execute(executor);

    assertEquals(2, response1.getOperations().size());
  }

  @Test
  public void testBeginRollbackTransaction() {
    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl(db, "test");
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(db.getTransaction().isActive());

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, true, true, operations);
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    RollbackTransactionRequest rollback = new RollbackTransactionRequest(10);
    BinaryResponse resposne = rollback.execute(executor);
    assertFalse(db.getTransaction().isActive());
  }

  @Test
  public void testEmptyBeginCommitTransaction() {

    db.begin();
    EntityImpl rec = db.save(((EntityImpl) db.newEntity("test")).field("name", "foo"));
    db.commit();

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);
    BeginTransactionRequest request = new BeginTransactionRequest(db, 10, false, true, null
    );
    BinaryResponse response = request.execute(executor);
    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    CreateRecordRequest createRecordRequest =
        new CreateRecordRequest(
            new EntityImpl(db, "test"), new RecordId(-1, -1), EntityImpl.RECORD_TYPE);
    BinaryResponse createResponse = createRecordRequest.execute(executor);
    assertTrue(createResponse instanceof CreateRecordResponse);

    rec = db.load(rec.getIdentity());
    rec.setProperty("name", "bar");
    UpdateRecordRequest updateRecordRequest =
        new UpdateRecordRequest(
            rec.getIdentity(), rec, rec.getVersion(), true, EntityImpl.RECORD_TYPE);
    BinaryResponse updateResponse = updateRecordRequest.execute(executor);
    assertTrue(updateResponse instanceof UpdateRecordResponse);

    Commit37Request commit = new Commit37Request(db, 10, false, true, null
    );
    BinaryResponse commitResponse = commit.execute(executor);
    assertFalse(db.getTransaction().isActive());
    assertTrue(commitResponse instanceof Commit37Response);
    assertEquals(1, ((Commit37Response) commitResponse).getUpdatedRids().size());
    assertEquals(2, db.countClass("test"));
  }

  @Test
  public void testBeginSQLInsertCommitTransaction() {

    ConnectionBinaryExecutor executor = new ConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();

    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 10, false, true, operations);
    BinaryResponse response = request.execute(executor);

    assertTrue(db.getTransaction().isActive());
    assertTrue(response instanceof BeginTransactionResponse);

    List<Result> results =
        db.command("insert into test set name = 'update'").stream()
            .collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    assertTrue(results.get(0).getEntity().get().getIdentity().isTemporary());

    Commit37Request commit = new Commit37Request(db, 10, false, true, null
    );
    BinaryResponse commitResponse = commit.execute(executor);
    assertFalse(db.getTransaction().isActive());
    assertTrue(commitResponse instanceof Commit37Response);

    assertEquals(1, ((Commit37Response) commitResponse).getUpdatedRids().size());

    assertTrue(((Commit37Response) commitResponse).getUpdatedRids().get(0).first().isTemporary());

    assertEquals(1, db.countClass("test"));

    ResultSet query = db.query("select from test where name = 'update'");

    results = query.stream().collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    query.close();
  }
}
