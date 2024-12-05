package com.orientechnologies.orient.test.database.auto;

import static org.testng.AssertJUnit.assertTrue;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.storage.OPhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.OStorageOperationResult;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
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
public class RemoteProtocolCommandsTest extends DocumentDBBaseTest {

  private static final String serverPort = System.getProperty("orient.server.port", "2424");

  @Parameters(value = "remote")
  public RemoteProtocolCommandsTest(boolean remote) {
    super(remote);
  }

  @Test(enabled = false)
  public void testConnect() throws Exception {
    final OServerAdmin admin =
        new OServerAdmin("remote:localhost:" + serverPort)
            .connect("root", SERVER_PASSWORD);
    admin.close();
  }

  @Test
  public void testListDatabasesMemoryDB() throws Exception {
    final OServerAdmin admin =
        new OServerAdmin("remote:localhost")
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
    YTClass clazz = this.database.getMetadata().getSchema().createClass("RidCreationTestClass");
    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) this.database.getStorage();
    EntityImpl doc = new EntityImpl("RidCreationTestClass");
    doc.field("test", "test");
    YTRecordId bad = new YTRecordId(-1, -1);
    OStorageOperationResult<OPhysicalPosition> res =
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
