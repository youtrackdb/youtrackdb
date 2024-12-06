package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TruncateClusterTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public TruncateClusterTest(boolean remote) {
    super(remote);
  }

  public void testSimpleCluster() {
    final String clusterName = "TruncateCluster";

    final int clusterId = database.addCluster(clusterName);
    final EntityImpl document = new EntityImpl();

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClusterElements(clusterId), 1);

    database.truncateCluster(clusterName);

    Assert.assertEquals(database.countClusterElements(clusterId), 0);

    database.dropCluster(clusterId);
  }

  public void testClusterWithIndex() {
    final String clusterName = "TruncateClusterWithIndex";
    final int clusterId = database.addCluster(clusterName);

    final String className = "TruncateClusterClass";
    final Schema schema = database.getMetadata().getSchema();

    final SchemaClass clazz = schema.createClass(className);
    clazz.addClusterId(database, clusterId);

    clazz.createProperty(database, "value", PropertyType.STRING);
    clazz.createIndex(database, "TruncateClusterIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    final EntityImpl document = new EntityImpl();
    document.field("value", "val");

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClass(className), 1);
    Assert.assertEquals(database.countClusterElements(clusterId), 1);

    ResultSet indexQuery = database.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(indexQuery.stream().count(), 1);

    database.truncateCluster(clusterName);

    Assert.assertEquals(database.countClass(className), 0);
    Assert.assertEquals(database.countClusterElements(clusterId), 0);

    indexQuery = database.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(indexQuery.stream().count(), 0);
  }

  public void testSimpleClusterIsAbsent() {
    final String clusterName = "TruncateClusterIsAbsent";
    final int clusterId = database.addCluster(clusterName);

    final EntityImpl document = new EntityImpl();

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    try {
      database.truncateCluster("Wrong" + clusterName);

      Assert.fail();
    } catch (BaseException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    database.dropCluster(clusterId);
  }

  public void testClusterInClassIsAbsent() {
    final String clusterName = "TruncateClusterInClassIsAbsent";

    final int clusterId = database.addCluster(clusterName);

    final String className = "TruncateClusterIsAbsentClass";
    final Schema schema = database.getMetadata().getSchema();

    final SchemaClass clazz = schema.createClass(className);
    clazz.addClusterId(database, clusterId);

    final EntityImpl document = new EntityImpl();

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    Assert.assertEquals(database.countClass(className), 1);

    try {
      clazz.truncateCluster(database, "Wrong" + clusterName);
      Assert.fail();
    } catch (BaseException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    Assert.assertEquals(database.countClass(className), 1);

    database.dropCluster(clusterId);
  }
}
