package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
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
    final EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("value", "val");

    document.save(clusterName);
    db.commit();

    Assert.assertEquals(1, db.countClass(className));
    Assert.assertEquals(1, db.countClusterElements(clusterId));

    ResultSet indexQuery = db.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(1, toList(indexQuery).size());
    indexQuery.close();

    db.command("truncate cluster " + clusterName);

    Assert.assertEquals(0, db.countClass(className));
    Assert.assertEquals(0, db.countClusterElements(clusterId));

    indexQuery = db.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(0, toList(indexQuery).size());
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
