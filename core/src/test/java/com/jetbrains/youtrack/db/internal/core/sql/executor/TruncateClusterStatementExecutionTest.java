package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TruncateClusterStatementExecutionTest extends DbTestBase {

  @Test
  public void testClusterWithIndex() {
    final String clusterName = "TruncateClusterWithIndex";
    final int clusterId = db.addCluster(clusterName);

    final String className = "TruncateClusterClass";
    final Schema schema = db.getMetadata().getSchema();

    final SchemaClass clazz = schema.createClass(className);
    clazz.addClusterId(db, clusterId);

    clazz.createProperty(db, "value", PropertyType.STRING);
    clazz.createIndex(db, "TruncateClusterIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    db.begin();
    final EntityImpl document = new EntityImpl();
    document.field("value", "val");

    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClass(className), 1);
    Assert.assertEquals(db.countClusterElements(clusterId), 1);

    ResultSet indexQuery = db.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(toList(indexQuery).size(), 1);
    indexQuery.close();

    db.command("truncate cluster " + clusterName);

    Assert.assertEquals(db.countClass(className), 0);
    Assert.assertEquals(db.countClusterElements(clusterId), 0);

    indexQuery = db.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(toList(indexQuery).size(), 0);
    indexQuery.close();
  }

  private List<Result> toList(ResultSet input) {
    List<Result> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }
}
