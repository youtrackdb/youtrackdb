package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TruncateClusterTest extends BaseDBTest {

  @Parameters(value = "remote")
  public TruncateClusterTest(boolean remote) {
    super(remote);
  }

  public void testSimpleCluster() {
    final String clusterName = "TruncateCluster";

    final int clusterId = db.addCluster(clusterName);
    final EntityImpl document = ((EntityImpl) db.newEntity());

    db.begin();
    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClusterElements(clusterId), 1);

    db.truncateCluster(clusterName);

    Assert.assertEquals(db.countClusterElements(clusterId), 0);

    db.dropCluster(clusterId);
  }

  public void testClusterWithIndex() {
    final String clusterName = "TruncateClusterWithIndex";
    final int clusterId = db.addCluster(clusterName);

    final String className = "TruncateClusterClass";
    final Schema schema = db.getMetadata().getSchema();

    final SchemaClass clazz = schema.createClass(className);
    clazz.addClusterId(db, clusterId);

    clazz.createProperty(db, "value", PropertyType.STRING);
    clazz.createIndex(db, "TruncateClusterIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    final EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("value", "val");

    db.begin();
    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClass(className), 1);
    Assert.assertEquals(db.countClusterElements(clusterId), 1);

    ResultSet indexQuery = db.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(indexQuery.stream().count(), 1);

    db.truncateCluster(clusterName);

    Assert.assertEquals(db.countClass(className), 0);
    Assert.assertEquals(db.countClusterElements(clusterId), 0);

    indexQuery = db.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(indexQuery.stream().count(), 0);
  }

  public void testSimpleClusterIsAbsent() {
    final String clusterName = "TruncateClusterIsAbsent";
    final int clusterId = db.addCluster(clusterName);

    final EntityImpl document = ((EntityImpl) db.newEntity());

    db.begin();
    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClusterElements(clusterId), 1);
    try {
      db.truncateCluster("Wrong" + clusterName);

      Assert.fail();
    } catch (BaseException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(db.countClusterElements(clusterId), 1);
    db.dropCluster(clusterId);
  }

  public void testClusterInClassIsAbsent() {
    final String clusterName = "TruncateClusterInClassIsAbsent";

    final int clusterId = db.addCluster(clusterName);

    final String className = "TruncateClusterIsAbsentClass";
    final Schema schema = db.getMetadata().getSchema();

    final SchemaClassInternal clazz = (SchemaClassInternal) schema.createClass(className);
    clazz.addClusterId(db, clusterId);

    final EntityImpl document = ((EntityImpl) db.newEntity());

    db.begin();
    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClusterElements(clusterId), 1);
    Assert.assertEquals(db.countClass(className), 1);

    try {
      clazz.truncateCluster(db, "Wrong" + clusterName);
      Assert.fail();
    } catch (BaseException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(db.countClusterElements(clusterId), 1);
    Assert.assertEquals(db.countClass(className), 1);

    db.dropCluster(clusterId);
  }
}
