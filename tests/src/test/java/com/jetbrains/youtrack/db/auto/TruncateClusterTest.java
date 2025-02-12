package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.BaseException;
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

    final var clusterId = session.addCluster(clusterName);
    final var document = ((EntityImpl) session.newEntity());

    session.begin();
    document.save(clusterName);
    session.commit();

    Assert.assertEquals(session.countClusterElements(clusterId), 1);

    session.truncateCluster(clusterName);

    Assert.assertEquals(session.countClusterElements(clusterId), 0);

    session.dropCluster(clusterId);
  }

  public void testClusterWithIndex() {
    final var clusterName = "TruncateClusterWithIndex";
    final var clusterId = session.addCluster(clusterName);

    final var className = "TruncateClusterClass";
    final Schema schema = session.getMetadata().getSchema();

    final var clazz = schema.createClass(className);
    clazz.addClusterId(session, clusterId);

    clazz.createProperty(session, "value", PropertyType.STRING);
    clazz.createIndex(session, "TruncateClusterIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    final var document = ((EntityImpl) session.newEntity());
    document.field("value", "val");

    session.begin();
    document.save(clusterName);
    session.commit();

    Assert.assertEquals(session.countClass(className), 1);
    Assert.assertEquals(session.countClusterElements(clusterId), 1);

    var indexQuery = session.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(indexQuery.stream().count(), 1);

    session.truncateCluster(clusterName);

    Assert.assertEquals(session.countClass(className), 0);
    Assert.assertEquals(session.countClusterElements(clusterId), 0);

    indexQuery = session.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(indexQuery.stream().count(), 0);
  }

  public void testSimpleClusterIsAbsent() {
    final var clusterName = "TruncateClusterIsAbsent";
    final var clusterId = session.addCluster(clusterName);

    final var document = ((EntityImpl) session.newEntity());

    session.begin();
    document.save(clusterName);
    session.commit();

    Assert.assertEquals(session.countClusterElements(clusterId), 1);
    try {
      session.truncateCluster("Wrong" + clusterName);

      Assert.fail();
    } catch (BaseException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(session.countClusterElements(clusterId), 1);
    session.dropCluster(clusterId);
  }

  public void testClusterInClassIsAbsent() {
    final var clusterName = "TruncateClusterInClassIsAbsent";

    final var clusterId = session.addCluster(clusterName);

    final var className = "TruncateClusterIsAbsentClass";
    final Schema schema = session.getMetadata().getSchema();

    final var clazz = (SchemaClassInternal) schema.createClass(className);
    clazz.addClusterId(session, clusterId);

    final var document = ((EntityImpl) session.newEntity());

    session.begin();
    document.save(clusterName);
    session.commit();

    Assert.assertEquals(session.countClusterElements(clusterId), 1);
    Assert.assertEquals(session.countClass(className), 1);

    try {
      clazz.truncateCluster(session, "Wrong" + clusterName);
      Assert.fail();
    } catch (BaseException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(session.countClusterElements(clusterId), 1);
    Assert.assertEquals(session.countClass(className), 1);

    session.dropCluster(clusterId);
  }
}
