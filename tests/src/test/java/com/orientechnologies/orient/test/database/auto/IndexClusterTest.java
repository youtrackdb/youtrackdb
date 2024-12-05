package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class IndexClusterTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public IndexClusterTest(boolean remote) {
    super(remote);
  }

  @Test
  public void indexAfterRebuildShouldIncludeAllClusters() {
    // given
    YTSchema schema = database.getMetadata().getSchema();
    String className = "IndexClusterTest";

    YTClass oclass = schema.createClass(className);
    oclass.createProperty(database, "key", YTType.STRING);
    oclass.createProperty(database, "value", YTType.INTEGER);
    oclass.createIndex(database, className + "index1", YTClass.INDEX_TYPE.NOTUNIQUE, "key");

    database.begin();
    database.<EntityImpl>newInstance(className).field("key", "a").field("value", 1).save();
    database.commit();

    int clId = database.addCluster(className + "secondCluster");
    oclass.addClusterId(database, clId);

    database.begin();
    database
        .<EntityImpl>newInstance(className)
        .field("key", "a")
        .field("value", 2)
        .save(className + "secondCluster");
    database.commit();

    // when
    database.command("rebuild index " + className + "index1").close();
    assertEquals(
        database.query("select from " + className + " where key = 'a'").stream().count(), 2);
  }
}
