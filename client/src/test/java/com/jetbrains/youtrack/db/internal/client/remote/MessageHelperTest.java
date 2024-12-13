package com.jetbrains.youtrack.db.internal.client.remote;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.client.remote.message.MessageHelper;
import com.jetbrains.youtrack.db.internal.client.remote.message.MockChannel;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.DirtyManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MessageHelperTest {

  @Test
  public void testOIdentifiable() throws IOException {

    YouTrackDB youTrackDB = new YouTrackDBImpl("embedded",
        YouTrackDBConfig.defaultConfig());

    youTrackDB.execute(
        "create database testOIdentifiable memory users (admin identified by 'admin' role admin)");

    DatabaseSessionInternal db =
        (DatabaseSessionInternal) youTrackDB.open("testOIdentifiable", "admin", "admin");
    int id = db.getClusterIdByName("V");
    try {
      MockChannel channel = new MockChannel();
      EntityImpl doc = new EntityImpl();
      RidBag bags = new RidBag(db);
      bags.add(new RecordId(id, 0));
      doc.field("bag", bags);

      EntityInternalUtils.fillClassNameIfNeeded(doc, "Test");
      RecordInternal.setIdentity(doc, new RecordId(id, 1));
      RecordInternal.setVersion(doc, 1);

      MessageHelper.writeIdentifiable(null,
          channel, doc, RecordSerializerNetworkFactory.INSTANCE.current());
      channel.close();

      EntityImpl newDoc =
          (EntityImpl)
              MessageHelper.readIdentifiable(db,
                  channel, RecordSerializerNetworkFactory.INSTANCE.current());

      assertThat(newDoc.getClassName()).isEqualTo("Test");
      assertThat((RidBag) newDoc.field("bag")).hasSize(1);

      DirtyManager dirtyManager = RecordInternal.getDirtyManager(newDoc);
      assertThat(dirtyManager.getNewRecords()).isNull();

    } finally {
      db.close();
      youTrackDB.close();
    }
  }

  @Test
  public void testReadWriteTransactionEntry() {
    RecordOperationRequest request = new RecordOperationRequest();

    request.setType(RecordOperation.UPDATED);
    request.setRecordType(RecordOperation.UPDATED);
    request.setId(new RecordId(25, 50));
    request.setRecord(new byte[]{10, 20, 30});
    request.setVersion(100);
    request.setContentChanged(true);

    ByteArrayOutputStream outArray = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outArray);

    try {
      MessageHelper.writeTransactionEntry(out, request);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(outArray.toByteArray()));

    try {
      RecordOperationRequest result = MessageHelper.readTransactionEntry(in);
      Assert.assertEquals(request.getType(), result.getType());
      Assert.assertEquals(request.getRecordType(), result.getRecordType());
      Assert.assertEquals(request.getType(), result.getType());
      Assert.assertEquals(request.getId(), result.getId());
      Assert.assertArrayEquals(request.getRecord(), result.getRecord());
      Assert.assertEquals(request.getVersion(), result.getVersion());
      Assert.assertEquals(request.isContentChanged(), result.isContentChanged());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
