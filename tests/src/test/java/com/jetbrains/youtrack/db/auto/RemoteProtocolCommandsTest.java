package com.jetbrains.youtrack.db.auto;

import static org.testng.AssertJUnit.assertTrue;

import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.StorageOperationResult;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 *
 */
@Test
public class RemoteProtocolCommandsTest extends BaseDBTest {

  private static final String serverPort = System.getProperty("youtrackdb.server.port", "2424");

  @Parameters(value = "remote")
  public RemoteProtocolCommandsTest(boolean remote) {
    super(remote);
  }

  @Test(enabled = false)
  public void testConnect() throws Exception {
    final ServerAdmin admin =
        new ServerAdmin("remote:localhost:" + serverPort)
            .connect("root", SERVER_PASSWORD);
    admin.close();
  }

  @Test
  public void testListDatabasesMemoryDB() throws Exception {
    final ServerAdmin admin =
        new ServerAdmin("remote:localhost")
            .connect("root", SERVER_PASSWORD);
    try {
      final Random random = new Random();

      final String plocalDatabaseName = "plocalTestListDatabasesMemoryDB" + random.nextInt();
      admin.createDatabase(plocalDatabaseName, "graph", "plocal");

      final String memoryDatabaseName = "memoryTestListDatabasesMemoryDB" + random.nextInt();
      admin.createDatabase(memoryDatabaseName, "graph", "memory");

      final Map<String, String> list = admin.listDatabases();

      Assert.assertTrue(list.containsKey(plocalDatabaseName), "Check plocal db is in list");
      Assert.assertTrue(list.containsKey(memoryDatabaseName), "Check memory db is in list");
    } finally {
      admin.close();
    }
  }

  @Test(enabled = false)
  // This is not supported anymore direct record operations are removed from the storage, only tx is
  // available
  public void testRawCreateWithoutIDTest() {
    SchemaClass clazz = this.database.getMetadata().getSchema().createClass("RidCreationTestClass");
    AbstractPaginatedStorage storage = (AbstractPaginatedStorage) this.database.getStorage();
    EntityImpl doc = new EntityImpl("RidCreationTestClass");
    doc.field("test", "test");
    RecordId bad = new RecordId(-1, -1);
    StorageOperationResult<PhysicalPosition> res =
        storage.createRecord(bad, doc.toStream(), doc.getVersion(), EntityImpl.RECORD_TYPE, null);

    // assertTrue(" the cluster is not valid", bad.clusterId >= 0);
    String ids = "";
    for (int aId : clazz.getClusterIds()) {
      ids += aId;
    }

    assertTrue(
        " returned id:" + bad.getClusterId() + " shoud be one of:" + ids,
        Arrays.binarySearch(clazz.getClusterIds(), bad.getClusterId()) >= 0);
  }
}
