package com.orientechnologies.orient.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OCommit37Request;
import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordResponse;
import com.orientechnologies.orient.client.remote.message.OFetchTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OFetchTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OQueryRequest;
import com.orientechnologies.orient.client.remote.message.OQueryResponse;
import com.orientechnologies.orient.client.remote.message.ORebeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordResponse;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
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
public class OConnectionExecutorTransactionTest {

  @Mock
  private OServer server;
  @Mock
  private OClientConnection connection;

  private YouTrackDB youTrackDb;
  private DatabaseSessionInternal database;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OConnectionExecutorTransactionTest.class.getSimpleName());
    database =
        (DatabaseSessionInternal)
            youTrackDb.open(
                OConnectionExecutorTransactionTest.class.getSimpleName(), "admin", "admin");
    database.createClass("test");
    ONetworkProtocolData protocolData = new ONetworkProtocolData();
    protocolData.setSerializer(RecordSerializerNetworkFactory.INSTANCE.current());
    Mockito.when(connection.getDatabase()).thenReturn(database);
    Mockito.when(connection.getData()).thenReturn(protocolData);
  }

  @After
  public void after() {
    database.close();
    youTrackDb.drop(OConnectionExecutorTransactionTest.class.getSimpleName());
    youTrackDb.close();
  }

  @Test
  public void testExecutionBeginTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl();
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);
    // TODO:Define properly what is the txId
    // assertEquals(((OBeginTransactionResponse) response).getTxId(), request.getTxId());
  }

  @Test
  public void testExecutionBeginCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl();
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OCommit37Request commit = new OCommit37Request(database, 10, false, true, null, null);
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(1, ((OCommit37Response) commitResponse).getUpdatedRids().size());
  }

  @Test
  public void testExecutionReplaceCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl();
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    rec.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    EntityImpl record1 = new EntityImpl(new RecordId(3, -3));
    record1.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record1, RecordOperation.CREATED));

    OCommit37Request commit = new OCommit37Request(database, 10, true, true, operations,
        new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(2, ((OCommit37Response) commitResponse).getUpdatedRids().size());
  }

  @Test
  public void testExecutionRebeginTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl();
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    rec.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    EntityImpl record1 = new EntityImpl(new RecordId(3, -3));
    record1.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record1, RecordOperation.CREATED));

    ORebeginTransactionRequest rebegin =
        new ORebeginTransactionRequest(database, 10, true, operations, new HashMap<>());
    OBinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof OBeginTransactionResponse);
    assertTrue(database.getTransaction().isActive());
    assertEquals(2, database.getTransaction().getEntryCount());
  }

  @Test
  public void testExecutionRebeginCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl();
    RecordInternal.setIdentity(rec, new RecordId(3, -2));
    rec.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    EntityImpl record1 = new EntityImpl(new RecordId(3, -3));
    record1.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record1, RecordOperation.CREATED));

    ORebeginTransactionRequest rebegin =
        new ORebeginTransactionRequest(database, 10, true, operations, new HashMap<>());
    OBinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof OBeginTransactionResponse);
    assertTrue(database.getTransaction().isActive());
    assertEquals(2, database.getTransaction().getEntryCount());

    EntityImpl record2 = new EntityImpl(new RecordId(3, -4));
    record2.setInternalStatus(RecordElement.STATUS.LOADED);
    operations.add(new RecordOperation(record2, RecordOperation.CREATED));

    OCommit37Request commit = new OCommit37Request(database, 10, true, true, operations,
        new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(3, ((OCommit37Response) commitResponse).getUpdatedRids().size());
  }

  @Test
  public void testExecutionQueryChangesTracking() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl("test");
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OQueryRequest query =
        new OQueryRequest(database,
            "sql",
            "update test set name='bla'",
            new HashMap<>(),
            OQueryRequest.COMMAND,
            RecordSerializerNetworkFactory.INSTANCE.current(), 20);
    OQueryResponse queryResponse = (OQueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());
  }

  @Test
  public void testBeginChangeFetchTransaction() {

    database.begin();
    database.save(new EntityImpl("test"));
    database.commit();

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl("test");
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OQueryRequest query =
        new OQueryRequest(database,
            "sql",
            "update test set name='bla'",
            new HashMap<>(),
            OQueryRequest.COMMAND,
            RecordSerializerNetworkFactory.INSTANCE.current(), 20);
    OQueryResponse queryResponse = (OQueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());

    OFetchTransactionRequest fetchRequest = new OFetchTransactionRequest(10);

    OFetchTransactionResponse response1 =
        (OFetchTransactionResponse) fetchRequest.execute(executor);

    assertEquals(2, response1.getOperations().size());
  }

  @Test
  public void testBeginRollbackTransaction() {
    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();
    EntityImpl rec = new EntityImpl("test");
    operations.add(new RecordOperation(rec, RecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ORollbackTransactionRequest rollback = new ORollbackTransactionRequest(10);
    OBinaryResponse resposne = rollback.execute(executor);
    assertFalse(database.getTransaction().isActive());
  }

  @Test
  public void testEmptyBeginCommitTransaction() {

    database.begin();
    EntityImpl rec = database.save(new EntityImpl("test").field("name", "foo"));
    database.commit();

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);
    OBeginTransactionRequest request = new OBeginTransactionRequest(database, 10, false, true, null,
        null);
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OCreateRecordRequest createRecordRequest =
        new OCreateRecordRequest(
            new EntityImpl("test"), new RecordId(-1, -1), EntityImpl.RECORD_TYPE);
    OBinaryResponse createResponse = createRecordRequest.execute(executor);
    assertTrue(createResponse instanceof OCreateRecordResponse);

    rec = database.load(rec.getIdentity());
    rec.setProperty("name", "bar");
    OUpdateRecordRequest updateRecordRequest =
        new OUpdateRecordRequest(
            (RecordId) rec.getIdentity(), rec, rec.getVersion(), true, EntityImpl.RECORD_TYPE);
    OBinaryResponse updateResponse = updateRecordRequest.execute(executor);
    assertTrue(updateResponse instanceof OUpdateRecordResponse);

    OCommit37Request commit = new OCommit37Request(database, 10, false, true, null,
        new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(1, ((OCommit37Response) commitResponse).getUpdatedRids().size());
    assertEquals(2, database.countClass("test"));
  }

  @Test
  public void testBeginSQLInsertCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<RecordOperation> operations = new ArrayList<>();

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, false, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    List<Result> results =
        database.command("insert into test set name = 'update'").stream()
            .collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    assertTrue(results.get(0).getEntity().get().getIdentity().isTemporary());

    OCommit37Request commit = new OCommit37Request(database, 10, false, true, null,
        new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(1, ((OCommit37Response) commitResponse).getUpdatedRids().size());

    assertTrue(((OCommit37Response) commitResponse).getUpdatedRids().get(0).first().isTemporary());

    assertEquals(1, database.countClass("test"));

    ResultSet query = database.query("select from test where name = 'update'");

    results = query.stream().collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    query.close();
  }
}
