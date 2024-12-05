package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.comparator.ODefaultComparator;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges.OPERATION;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChangesPerKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;

public class ORemoteTransactionMessagesTest extends DBTestBase {

  @Test
  public void testBeginTransactionEmptyWriteRead() throws IOException {
    MockChannel channel = new MockChannel();
    OBeginTransactionRequest request = new OBeginTransactionRequest(db, 0, false,
        true, null, null);
    request.write(db, channel, null);
    channel.close();
    OBeginTransactionRequest readRequest = new OBeginTransactionRequest();
    readRequest.read(db, channel, 0, null);
    assertFalse(readRequest.isHasContent());
  }

  @Test
  public void testBeginTransactionWriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new EntityImpl(), ORecordOperation.CREATED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new YTRecordId(1, 2), OPERATION.PUT);
    keyChange.add(new YTRecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OBeginTransactionRequest request =
        new OBeginTransactionRequest(db, 0, true, true, operations, changes);
    request.write(db, channel, null);

    channel.close();

    OBeginTransactionRequest readRequest = new OBeginTransactionRequest();
    readRequest.read(db, channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(1, readRequest.getOperations().size());
    assertEquals(0, readRequest.getTxId());
    assertEquals(1, readRequest.getIndexChanges().size());
    assertEquals("some", readRequest.getIndexChanges().get(0).getName());
    OTransactionIndexChanges val = readRequest.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new YTRecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new YTRecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  public void testFullCommitTransactionWriteRead() throws IOException {
    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new EntityImpl(), ORecordOperation.CREATED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new YTRecordId(1, 2), OPERATION.PUT);
    keyChange.add(new YTRecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OCommit37Request request = new OCommit37Request(db, 0, true, true, operations, changes);
    request.write(db, channel, null);

    channel.close();

    OCommit37Request readRequest = new OCommit37Request();
    readRequest.read(db, channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(1, readRequest.getOperations().size());
    assertEquals(0, readRequest.getTxId());
    assertEquals(1, readRequest.getIndexChanges().size());
    assertEquals("some", readRequest.getIndexChanges().get(0).getName());
    OTransactionIndexChanges val = readRequest.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new YTRecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new YTRecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  public void testCommitResponseTransactionWriteRead() throws IOException {

    MockChannel channel = new MockChannel();

    Map<UUID, OBonsaiCollectionPointer> changes = new HashMap<>();
    UUID val = UUID.randomUUID();
    changes.put(val, new OBonsaiCollectionPointer(10, new OBonsaiBucketPointer(30, 40)));
    var updatedRids = new HashMap<YTRID, YTRID>();

    updatedRids.put(new YTRecordId(10, 20), new YTRecordId(10, 30));
    updatedRids.put(new YTRecordId(10, 21), new YTRecordId(10, 31));

    OCommit37Response response = new OCommit37Response(updatedRids, changes);
    response.write(db, channel, 0, null);
    channel.close();

    OCommit37Response readResponse = new OCommit37Response();
    readResponse.read(db, channel, null);

    assertEquals(2, readResponse.getUpdatedRids().size());

    assertEquals(new YTRecordId(10, 30), readResponse.getUpdatedRids().get(0).first());
    assertEquals(new YTRecordId(10, 20), readResponse.getUpdatedRids().get(0).second());

    assertEquals(new YTRecordId(10, 31), readResponse.getUpdatedRids().get(1).first());
    assertEquals(new YTRecordId(10, 21), readResponse.getUpdatedRids().get(1).second());

    assertEquals(1, readResponse.getCollectionChanges().size());
    assertNotNull(readResponse.getCollectionChanges().get(val));
    assertEquals(10, readResponse.getCollectionChanges().get(val).getFileId());
    assertEquals(30, readResponse.getCollectionChanges().get(val).getRootPointer().getPageIndex());
    assertEquals(40, readResponse.getCollectionChanges().get(val).getRootPointer().getPageOffset());
  }

  @Test
  public void testEmptyCommitTransactionWriteRead() throws IOException {

    MockChannel channel = new MockChannel();
    OCommit37Request request = new OCommit37Request(db, 0, false, true, null, null);
    request.write(db, channel, null);

    channel.close();

    OCommit37Request readRequest = new OCommit37Request();
    readRequest.read(db, channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertNull(readRequest.getOperations());
    assertEquals(0, readRequest.getTxId());
    assertNull(readRequest.getIndexChanges());
  }

  @Test
  public void testTransactionFetchResponseWriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new EntityImpl(), ORecordOperation.CREATED));
    var docOne = new EntityImpl();
    ORecordInternal.setIdentity(docOne, new YTRecordId(10, 2));

    var docTwo = new EntityImpl();
    ORecordInternal.setIdentity(docTwo, new YTRecordId(10, 1));

    operations.add(
        new ORecordOperation(docOne, ORecordOperation.UPDATED));
    operations.add(
        new ORecordOperation(docTwo, ORecordOperation.DELETED));

    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new YTRecordId(1, 2), OPERATION.PUT);
    keyChange.add(new YTRecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OFetchTransactionResponse response =
        new OFetchTransactionResponse(db, 10, operations, changes, new HashMap<>());
    response.write(db, channel, 0, ORecordSerializerNetworkV37.INSTANCE);

    channel.close();

    OFetchTransactionResponse readResponse = new OFetchTransactionResponse();
    readResponse.read(db, channel, null);

    assertEquals(3, readResponse.getOperations().size());
    assertEquals(ORecordOperation.CREATED, readResponse.getOperations().get(0).getType());
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(ORecordOperation.UPDATED, readResponse.getOperations().get(1).getType());
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(ORecordOperation.DELETED, readResponse.getOperations().get(2).getType());
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(10, readResponse.getTxId());
    assertEquals(1, readResponse.getIndexChanges().size());
    assertEquals("some", readResponse.getIndexChanges().get(0).getName());
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new YTRecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new YTRecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  @Ignore
  public void testTransactionFetchResponse38WriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new EntityImpl(), ORecordOperation.CREATED));
    operations.add(
        new ORecordOperation(new EntityImpl(new YTRecordId(10, 2)), ORecordOperation.UPDATED));
    operations.add(
        new ORecordOperation(new EntityImpl(new YTRecordId(10, 1)), ORecordOperation.DELETED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new YTRecordId(1, 2), OPERATION.PUT);
    keyChange.add(new YTRecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OFetchTransaction38Response response =
        new OFetchTransaction38Response(db, 10, operations, changes, new HashMap<>(), null);
    response.write(db, channel, 0, ORecordSerializerNetworkV37.INSTANCE);

    channel.close();

    OFetchTransaction38Response readResponse = new OFetchTransaction38Response();
    readResponse.read(db, channel, null);

    assertEquals(3, readResponse.getOperations().size());
    assertEquals(ORecordOperation.CREATED, readResponse.getOperations().get(0).getType());
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(ORecordOperation.UPDATED, readResponse.getOperations().get(1).getType());
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(ORecordOperation.DELETED, readResponse.getOperations().get(2).getType());
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(10, readResponse.getTxId());
    assertEquals(1, readResponse.getIndexChanges().size());
    assertEquals("some", readResponse.getIndexChanges().get(0).getName());
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertFalse(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new YTRecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new YTRecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }

  @Test
  public void testTransactionClearIndexFetchResponseWriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = true;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new YTRecordId(1, 2), OPERATION.PUT);
    keyChange.add(new YTRecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OFetchTransactionResponse response =
        new OFetchTransactionResponse(db, 10, operations, changes, new HashMap<>());
    response.write(db, channel, 0, ORecordSerializerNetworkV37.INSTANCE);

    channel.close();

    OFetchTransactionResponse readResponse =
        new OFetchTransactionResponse(db, 10, operations, changes, new HashMap<>());
    readResponse.read(db, channel, null);

    assertEquals(10, readResponse.getTxId());
    assertEquals(1, readResponse.getIndexChanges().size());
    assertEquals("some", readResponse.getIndexChanges().get(0).getName());
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertTrue(val.cleared);
    assertEquals(1, val.changesPerKey.size());
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals("key", entryChange.key);
    assertEquals(2, entryChange.size());
    assertEquals(new YTRecordId(1, 2), entryChange.getEntriesAsList().get(0).getValue());
    assertEquals(OPERATION.PUT, entryChange.getEntriesAsList().get(0).getOperation());
    assertEquals(new YTRecordId(2, 2), entryChange.getEntriesAsList().get(1).getValue());
    assertEquals(OPERATION.REMOVE, entryChange.getEntriesAsList().get(1).getOperation());
  }
}
