package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
    final YTDocument document = new YTDocument();

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
    final YTSchema schema = database.getMetadata().getSchema();

    final YTClass clazz = schema.createClass(className);
    clazz.addClusterId(database, clusterId);

    clazz.createProperty(database, "value", YTType.STRING);
    clazz.createIndex(database, "TruncateClusterIndex", YTClass.INDEX_TYPE.UNIQUE, "value");

    final YTDocument document = new YTDocument();
    document.field("value", "val");

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClass(className), 1);
    Assert.assertEquals(database.countClusterElements(clusterId), 1);

    OResultSet indexQuery = database.query("select from TruncateClusterClass where value='val'");
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

    final YTDocument document = new YTDocument();

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    try {
      database.truncateCluster("Wrong" + clusterName);

      Assert.fail();
    } catch (OException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    database.dropCluster(clusterId);
  }

  public void testClusterInClassIsAbsent() {
    final String clusterName = "TruncateClusterInClassIsAbsent";

    final int clusterId = database.addCluster(clusterName);

    final String className = "TruncateClusterIsAbsentClass";
    final YTSchema schema = database.getMetadata().getSchema();

    final YTClass clazz = schema.createClass(className);
    clazz.addClusterId(database, clusterId);

    final YTDocument document = new YTDocument();

    database.begin();
    document.save(clusterName);
    database.commit();

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    Assert.assertEquals(database.countClass(className), 1);

    try {
      clazz.truncateCluster(database, "Wrong" + clusterName);
      Assert.fail();
    } catch (OException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    Assert.assertEquals(database.countClass(className), 1);

    database.dropCluster(clusterId);
  }
}
