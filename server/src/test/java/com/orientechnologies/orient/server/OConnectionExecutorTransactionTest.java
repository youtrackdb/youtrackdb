package com.orientechnologies.orient.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
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
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
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
  private YTDatabaseSessionInternal database;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OConnectionExecutorTransactionTest.class.getSimpleName());
    database =
        (YTDatabaseSessionInternal)
            youTrackDb.open(
                OConnectionExecutorTransactionTest.class.getSimpleName(), "admin", "admin");
    database.createClass("test");
    ONetworkProtocolData protocolData = new ONetworkProtocolData();
    protocolData.setSerializer(ORecordSerializerNetworkFactory.INSTANCE.current());
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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument();
    ORecordInternal.setIdentity(rec, new YTRecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument();
    ORecordInternal.setIdentity(rec, new YTRecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument();
    ORecordInternal.setIdentity(rec, new YTRecordId(3, -2));
    rec.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    YTDocument record1 = new YTDocument(new YTRecordId(3, -3));
    record1.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument();
    ORecordInternal.setIdentity(rec, new YTRecordId(3, -2));
    rec.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    YTDocument record1 = new YTDocument(new YTRecordId(3, -3));
    record1.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument();
    ORecordInternal.setIdentity(rec, new YTRecordId(3, -2));
    rec.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    YTDocument record1 = new YTDocument(new YTRecordId(3, -3));
    record1.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    ORebeginTransactionRequest rebegin =
        new ORebeginTransactionRequest(database, 10, true, operations, new HashMap<>());
    OBinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof OBeginTransactionResponse);
    assertTrue(database.getTransaction().isActive());
    assertEquals(2, database.getTransaction().getEntryCount());

    YTDocument record2 = new YTDocument(new YTRecordId(3, -4));
    record2.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record2, ORecordOperation.CREATED));

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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
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
            ORecordSerializerNetworkFactory.INSTANCE.current(), 20);
    OQueryResponse queryResponse = (OQueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());
  }

  @Test
  public void testBeginChangeFetchTransaction() {

    database.begin();
    database.save(new YTDocument("test"));
    database.commit();

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
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
            ORecordSerializerNetworkFactory.INSTANCE.current(), 20);
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

    List<ORecordOperation> operations = new ArrayList<>();
    YTDocument rec = new YTDocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
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
    YTDocument rec = database.save(new YTDocument("test").field("name", "foo"));
    database.commit();

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);
    OBeginTransactionRequest request = new OBeginTransactionRequest(database, 10, false, true, null,
        null);
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OCreateRecordRequest createRecordRequest =
        new OCreateRecordRequest(
            new YTDocument("test"), new YTRecordId(-1, -1), YTDocument.RECORD_TYPE);
    OBinaryResponse createResponse = createRecordRequest.execute(executor);
    assertTrue(createResponse instanceof OCreateRecordResponse);

    rec = database.load(rec.getIdentity());
    rec.setProperty("name", "bar");
    OUpdateRecordRequest updateRecordRequest =
        new OUpdateRecordRequest(
            (YTRecordId) rec.getIdentity(), rec, rec.getVersion(), true, YTDocument.RECORD_TYPE);
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

    List<ORecordOperation> operations = new ArrayList<>();

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(database, 10, false, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    List<YTResult> results =
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

    YTResultSet query = database.query("select from test where name = 'update'");

    results = query.stream().collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    query.close();
  }
}
