package com.jetbrains.youtrack.db.internal.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.RidBagBucketPointer;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;

public class RemoteTransactionMessagesTest extends DbTestBase {

  @Test
  public void testBeginTransactionEmptyWriteRead() throws IOException {
    var channel = new MockChannel();
    var request = new BeginTransactionRequest(session, 0, false,
        true, null);
    request.write(session, channel, null);
    channel.close();
    var readRequest = new BeginTransactionRequest();
    readRequest.read(session, channel, 0, null);
    assertFalse(readRequest.isHasContent());
  }

  @Test
  public void testBeginTransactionWriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(session), RecordOperation.CREATED));
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();

    var channel = new MockChannel();
    var request =
        new BeginTransactionRequest(session, 0, true, true, operations);
    request.write(session, channel, null);

    channel.close();

    var readRequest = new BeginTransactionRequest();
    readRequest.read(session, channel, 0, RecordSerializerNetworkFactory.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(1, readRequest.getOperations().size());
    assertEquals(0, readRequest.getTxId());
  }

  @Test
  public void testFullCommitTransactionWriteRead() throws IOException {
    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(session), RecordOperation.CREATED));
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();

    var channel = new MockChannel();
    var request = new Commit37Request(session, 0, true, true, operations);
    request.write(session, channel, null);

    channel.close();

    var readRequest = new Commit37Request();
    readRequest.read(session, channel, 0, RecordSerializerNetworkFactory.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(1, readRequest.getOperations().size());
    assertEquals(0, readRequest.getTxId());
  }

  @Test
  public void testCommitResponseTransactionWriteRead() throws IOException {

    var channel = new MockChannel();

    Map<UUID, BonsaiCollectionPointer> changes = new HashMap<>();
    var val = UUID.randomUUID();
    changes.put(val, new BonsaiCollectionPointer(10, new RidBagBucketPointer(30, 40)));
    var updatedRids = new HashMap<RecordId, RecordId>();

    updatedRids.put(new RecordId(10, 20), new RecordId(10, 30));
    updatedRids.put(new RecordId(10, 21), new RecordId(10, 31));

    var response = new Commit37Response(updatedRids, changes);
    response.write(session, channel, 0, null);
    channel.close();

    var readResponse = new Commit37Response();
    readResponse.read(session, channel, null);

    assertEquals(2, readResponse.getUpdatedRids().size());

    assertEquals(new RecordId(10, 30), readResponse.getUpdatedRids().get(0).first());
    assertEquals(new RecordId(10, 20), readResponse.getUpdatedRids().get(0).second());

    assertEquals(new RecordId(10, 31), readResponse.getUpdatedRids().get(1).first());
    assertEquals(new RecordId(10, 21), readResponse.getUpdatedRids().get(1).second());

    assertEquals(1, readResponse.getCollectionChanges().size());
    assertNotNull(readResponse.getCollectionChanges().get(val));
    assertEquals(10, readResponse.getCollectionChanges().get(val).getFileId());
    assertEquals(30, readResponse.getCollectionChanges().get(val).getRootPointer().getPageIndex());
    assertEquals(40, readResponse.getCollectionChanges().get(val).getRootPointer().getPageOffset());
  }

  @Test
  public void testEmptyCommitTransactionWriteRead() throws IOException {

    var channel = new MockChannel();
    var request = new Commit37Request(session, 0, false, true, null);
    request.write(session, channel, null);

    channel.close();

    var readRequest = new Commit37Request();
    readRequest.read(session, channel, 0, RecordSerializerNetworkFactory.current());
    assertTrue(readRequest.isUsingLog());
    assertNull(readRequest.getOperations());
    assertEquals(0, readRequest.getTxId());
  }

  @Test
  public void testTransactionFetchResponseWriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(session), RecordOperation.CREATED));
    var docOne = new EntityImpl(session);
    RecordInternal.setIdentity(docOne, new RecordId(10, 2));

    var docTwo = new EntityImpl(session);
    RecordInternal.setIdentity(docTwo, new RecordId(10, 1));

    operations.add(
        new RecordOperation(docOne, RecordOperation.UPDATED));
    operations.add(
        new RecordOperation(docTwo, RecordOperation.DELETED));

    var channel = new MockChannel();
    var response =
        new FetchTransactionResponse(session, 10, operations, new HashMap<>());
    response.write(session, channel, 0, RecordSerializerNetworkV37.INSTANCE);

    channel.close();

    var readResponse = new FetchTransactionResponse();
    readResponse.read(session, channel, null);

    assertEquals(3, readResponse.getOperations().size());
    assertEquals(RecordOperation.CREATED, readResponse.getOperations().get(0).getType());
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(RecordOperation.UPDATED, readResponse.getOperations().get(1).getType());
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(RecordOperation.DELETED, readResponse.getOperations().get(2).getType());
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(10, readResponse.getTxId());
  }

  @Test
  @Ignore
  public void testTransactionFetchResponse38WriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(session), RecordOperation.CREATED));
    operations.add(
        new RecordOperation(new EntityImpl(session, new RecordId(10, 2)), RecordOperation.UPDATED));
    operations.add(
        new RecordOperation(new EntityImpl(session, new RecordId(10, 1)), RecordOperation.DELETED));
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();
    var channel = new MockChannel();
    var response =
        new FetchTransaction38Response(session, 10, operations, changes, new HashMap<>(), null);
    response.write(session, channel, 0, RecordSerializerNetworkV37.INSTANCE);

    channel.close();

    var readResponse = new FetchTransaction38Response();
    readResponse.read(session, channel, null);

    assertEquals(3, readResponse.getOperations().size());
    assertEquals(RecordOperation.CREATED, readResponse.getOperations().get(0).getType());
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(RecordOperation.UPDATED, readResponse.getOperations().get(1).getType());
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(RecordOperation.DELETED, readResponse.getOperations().get(2).getType());
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(10, readResponse.getTxId());
  }

  @Test
  public void testTransactionClearIndexFetchResponseWriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();

    var channel = new MockChannel();
    var response =
        new FetchTransactionResponse(session, 10, operations, new HashMap<>());
    response.write(session, channel, 0, RecordSerializerNetworkV37.INSTANCE);

    channel.close();

    var readResponse =
        new FetchTransactionResponse(session, 10, operations, new HashMap<>());
    readResponse.read(session, channel, null);

    assertEquals(10, readResponse.getTxId());
  }
}
