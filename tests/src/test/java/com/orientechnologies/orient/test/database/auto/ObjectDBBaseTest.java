package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.object.db.OObjectDatabaseTxInternal;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public class ObjectDBBaseTest extends BaseTest<OObjectDatabaseTxInternal> {
  public ObjectDBBaseTest() {}

  @Parameters(value = "remote")
  public ObjectDBBaseTest(boolean remote) {
    super(remote);
  }

  public ObjectDBBaseTest(boolean remote, String prefix) {
    super(remote, prefix);
  }

  @Override
  protected OObjectDatabaseTxInternal createSessionInstance(
      OrientDB orientDB, String dbName, String user, String password) {
    var session = orientDB.open(dbName, "admin", "admin");
    return new OObjectDatabaseTxInternal((ODatabaseSessionInternal) session);
  }
}
