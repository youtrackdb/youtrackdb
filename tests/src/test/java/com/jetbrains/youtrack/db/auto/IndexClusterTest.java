package com.jetbrains.youtrack.db.auto;

import static org.testng.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class IndexClusterTest extends BaseDBTest {

  @Parameters(value = "remote")
  public IndexClusterTest(boolean remote) {
    super(remote);
  }

  @Test
  public void indexAfterRebuildShouldIncludeAllClusters() {
    // given
    Schema schema = db.getMetadata().getSchema();
    var className = "IndexClusterTest";

    var oclass = schema.createClass(className);
    oclass.createProperty(db, "key", PropertyType.STRING);
    oclass.createProperty(db, "value", PropertyType.INTEGER);
    oclass.createIndex(db, className + "index1", SchemaClass.INDEX_TYPE.NOTUNIQUE, "key");

    db.begin();
    db.<EntityImpl>newInstance(className).field("key", "a").field("value", 1).save();
    db.commit();

    var clId = db.addCluster(className + "secondCluster");
    oclass.addClusterId(db, clId);

    db.begin();
    db
        .<EntityImpl>newInstance(className)
        .field("key", "a")
        .field("value", 2)
        .save(className + "secondCluster");
    db.commit();

    // when
    db.command("rebuild index " + className + "index1").close();
    assertEquals(
        db.query("select from " + className + " where key = 'a'").stream().count(), 2);
  }
}
