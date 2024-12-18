package com.jetbrains.youtrack.db.internal.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.RidBagBucketPointer;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges.OPERATION;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChangesPerKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;

public class RemoteTransactionMessagesTest extends DbTestBase {

  @Test
  public void testBeginTransactionEmptyWriteRead() throws IOException {
    MockChannel channel = new MockChannel();
    BeginTransactionRequest request = new BeginTransactionRequest(db, 0, false,
        true, null, null);
    request.write(db, channel, null);
    channel.close();
    BeginTransactionRequest readRequest = new BeginTransactionRequest();
    readRequest.read(db, channel, 0, null);
    assertFalse(readRequest.isHasContent());
  }

  @Test
  public void testBeginTransactionWriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(db), RecordOperation.CREATED));
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();
    FrontendTransactionIndexChanges change = new FrontendTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(DefaultComparator.INSTANCE);
    FrontendTransactionIndexChangesPerKey keyChange = new FrontendTransactionIndexChangesPerKey(
        "key");
    keyChange.add(new RecordId(1, 2), OPERATION.PUT);
    keyChange.add(new RecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    BeginTransactionRequest request =
        new BeginTransactionRequest(db, 0, true, true, operations, changes);
    request.write(db, channel, null);

    channel.close();

    BeginTransactionRequest readRequest = new BeginTransactionRequest();
    readRequest.read(db, channel, 0, RecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(1, readRequest.getOperations().size());
    assertEquals(0, readRequest.getTxId());
    assertEquals(1, readRequest.getIndexChanges().size());
    assertEquals("some", readRequest.getIndexChanges().get(0).getName());
    FrontendTransactionIndexChanges val = readRequest.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    FrontendTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new RecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new RecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  public void testFullCommitTransactionWriteRead() throws IOException {
    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(db), RecordOperation.CREATED));
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();
    FrontendTransactionIndexChanges change = new FrontendTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(DefaultComparator.INSTANCE);
    FrontendTransactionIndexChangesPerKey keyChange = new FrontendTransactionIndexChangesPerKey(
        "key");
    keyChange.add(new RecordId(1, 2), OPERATION.PUT);
    keyChange.add(new RecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    Commit37Request request = new Commit37Request(db, 0, true, true, operations, changes);
    request.write(db, channel, null);

    channel.close();

    Commit37Request readRequest = new Commit37Request();
    readRequest.read(db, channel, 0, RecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(1, readRequest.getOperations().size());
    assertEquals(0, readRequest.getTxId());
    assertEquals(1, readRequest.getIndexChanges().size());
    assertEquals("some", readRequest.getIndexChanges().get(0).getName());
    FrontendTransactionIndexChanges val = readRequest.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    FrontendTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new RecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new RecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  public void testCommitResponseTransactionWriteRead() throws IOException {

    MockChannel channel = new MockChannel();

    Map<UUID, BonsaiCollectionPointer> changes = new HashMap<>();
    UUID val = UUID.randomUUID();
    changes.put(val, new BonsaiCollectionPointer(10, new RidBagBucketPointer(30, 40)));
    var updatedRids = new HashMap<RecordId, RecordId>();

    updatedRids.put(new RecordId(10, 20), new RecordId(10, 30));
    updatedRids.put(new RecordId(10, 21), new RecordId(10, 31));

    Commit37Response response = new Commit37Response(updatedRids, changes);
    response.write(db, channel, 0, null);
    channel.close();

    Commit37Response readResponse = new Commit37Response();
    readResponse.read(db, channel, null);

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

    MockChannel channel = new MockChannel();
    Commit37Request request = new Commit37Request(db, 0, false, true, null, null);
    request.write(db, channel, null);

    channel.close();

    Commit37Request readRequest = new Commit37Request();
    readRequest.read(db, channel, 0, RecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertNull(readRequest.getOperations());
    assertEquals(0, readRequest.getTxId());
    assertNull(readRequest.getIndexChanges());
  }

  @Test
  public void testTransactionFetchResponseWriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(db), RecordOperation.CREATED));
    var docOne = new EntityImpl(db);
    RecordInternal.setIdentity(docOne, new RecordId(10, 2));

    var docTwo = new EntityImpl(db);
    RecordInternal.setIdentity(docTwo, new RecordId(10, 1));

    operations.add(
        new RecordOperation(docOne, RecordOperation.UPDATED));
    operations.add(
        new RecordOperation(docTwo, RecordOperation.DELETED));

    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();
    FrontendTransactionIndexChanges change = new FrontendTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(DefaultComparator.INSTANCE);
    FrontendTransactionIndexChangesPerKey keyChange = new FrontendTransactionIndexChangesPerKey(
        "key");
    keyChange.add(new RecordId(1, 2), OPERATION.PUT);
    keyChange.add(new RecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    FetchTransactionResponse response =
        new FetchTransactionResponse(db, 10, operations, changes, new HashMap<>());
    response.write(db, channel, 0, RecordSerializerNetworkV37.INSTANCE);

    channel.close();

    FetchTransactionResponse readResponse = new FetchTransactionResponse();
    readResponse.read(db, channel, null);

    assertEquals(3, readResponse.getOperations().size());
    assertEquals(RecordOperation.CREATED, readResponse.getOperations().get(0).getType());
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(RecordOperation.UPDATED, readResponse.getOperations().get(1).getType());
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(RecordOperation.DELETED, readResponse.getOperations().get(2).getType());
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(10, readResponse.getTxId());
    assertEquals(1, readResponse.getIndexChanges().size());
    assertEquals("some", readResponse.getIndexChanges().get(0).getName());
    FrontendTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    FrontendTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new RecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new RecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  @Ignore
  public void testTransactionFetchResponse38WriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    operations.add(new RecordOperation(new EntityImpl(db), RecordOperation.CREATED));
    operations.add(
        new RecordOperation(new EntityImpl(db, new RecordId(10, 2)), RecordOperation.UPDATED));
    operations.add(
        new RecordOperation(new EntityImpl(db, new RecordId(10, 1)), RecordOperation.DELETED));
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();
    FrontendTransactionIndexChanges change = new FrontendTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(DefaultComparator.INSTANCE);
    FrontendTransactionIndexChangesPerKey keyChange = new FrontendTransactionIndexChangesPerKey(
        "key");
    keyChange.add(new RecordId(1, 2), OPERATION.PUT);
    keyChange.add(new RecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    FetchTransaction38Response response =
        new FetchTransaction38Response(db, 10, operations, changes, new HashMap<>(), null);
    response.write(db, channel, 0, RecordSerializerNetworkV37.INSTANCE);

    channel.close();

    FetchTransaction38Response readResponse = new FetchTransaction38Response();
    readResponse.read(db, channel, null);

    assertEquals(3, readResponse.getOperations().size());
    assertEquals(RecordOperation.CREATED, readResponse.getOperations().get(0).getType());
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(RecordOperation.UPDATED, readResponse.getOperations().get(1).getType());
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(RecordOperation.DELETED, readResponse.getOperations().get(2).getType());
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(10, readResponse.getTxId());
    assertEquals(1, readResponse.getIndexChanges().size());
    assertEquals("some", readResponse.getIndexChanges().get(0).getName());
    FrontendTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    FrontendTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new RecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new RecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  public void testTransactionClearIndexFetchResponseWriteRead() throws IOException {

    List<RecordOperation> operations = new ArrayList<>();
    Map<String, FrontendTransactionIndexChanges> changes = new HashMap<>();
    FrontendTransactionIndexChanges change = new FrontendTransactionIndexChanges();
    change.cleared = true;
    change.changesPerKey = new TreeMap<>(DefaultComparator.INSTANCE);
    FrontendTransactionIndexChangesPerKey keyChange = new FrontendTransactionIndexChangesPerKey(
        "key");
    keyChange.add(new RecordId(1, 2), OPERATION.PUT);
    keyChange.add(new RecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    FetchTransactionResponse response =
        new FetchTransactionResponse(db, 10, operations, changes, new HashMap<>());
    response.write(db, channel, 0, RecordSerializerNetworkV37.INSTANCE);

    channel.close();

    FetchTransactionResponse readResponse =
        new FetchTransactionResponse(db, 10, operations, changes, new HashMap<>());
    readResponse.read(db, channel, null);

    assertEquals(10, readResponse.getTxId());
    assertEquals(1, readResponse.getIndexChanges().size());
    assertEquals("some", readResponse.getIndexChanges().get(0).getName());
    FrontendTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertTrue(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    FrontendTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new RecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new RecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }
}
