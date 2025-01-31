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
    final var clusterName = "TruncateCluster";

    final var clusterId = db.addCluster(clusterName);
    final var document = ((EntityImpl) db.newEntity());

    db.begin();
    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClusterElements(clusterId), 1);

    db.truncateCluster(clusterName);

    Assert.assertEquals(db.countClusterElements(clusterId), 0);

    db.dropCluster(clusterId);
  }

  public void testClusterWithIndex() {
    final var clusterName = "TruncateClusterWithIndex";
    final var clusterId = db.addCluster(clusterName);

    final var className = "TruncateClusterClass";
    final Schema schema = db.getMetadata().getSchema();

    final var clazz = schema.createClass(className);
    clazz.addClusterId(db, clusterId);

    clazz.createProperty(db, "value", PropertyType.STRING);
    clazz.createIndex(db, "TruncateClusterIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    final var document = ((EntityImpl) db.newEntity());
    document.field("value", "val");

    db.begin();
    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClass(className), 1);
    Assert.assertEquals(db.countClusterElements(clusterId), 1);

    var indexQuery = db.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(indexQuery.stream().count(), 1);

    db.truncateCluster(clusterName);

    Assert.assertEquals(db.countClass(className), 0);
    Assert.assertEquals(db.countClusterElements(clusterId), 0);

    indexQuery = db.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(indexQuery.stream().count(), 0);
  }

  public void testSimpleClusterIsAbsent() {
    final var clusterName = "TruncateClusterIsAbsent";
    final var clusterId = db.addCluster(clusterName);

    final var document = ((EntityImpl) db.newEntity());

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
    final var clusterName = "TruncateClusterInClassIsAbsent";

    final var clusterId = db.addCluster(clusterName);

    final var className = "TruncateClusterIsAbsentClass";
    final Schema schema = db.getMetadata().getSchema();

    final var clazz = (SchemaClassInternal) schema.createClass(className);
    clazz.addClusterId(db, clusterId);

    final var document = ((EntityImpl) db.newEntity());

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
