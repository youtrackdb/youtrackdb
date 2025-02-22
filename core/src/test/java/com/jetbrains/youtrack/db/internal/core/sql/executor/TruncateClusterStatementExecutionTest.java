package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
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
    final var clusterName = "TruncateClusterWithIndex";
    final var clusterId = session.addCluster(clusterName);

    final var className = "TruncateClusterClass";
    final Schema schema = session.getMetadata().getSchema();

    final var clazz = schema.createClass(className);
    clazz.addClusterId(session, clusterId);

    clazz.createProperty(session, "value", PropertyType.STRING);
    clazz.createIndex(session, "TruncateClusterIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    session.begin();
    final var document = ((EntityImpl) session.newEntity(className));
    document.field("value", "val");

    session.commit();

    Assert.assertEquals(1, session.countClass(className));
    Assert.assertEquals(1, session.countClusterElements(clusterId));

    var indexQuery = session.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(1, toList(indexQuery).size());
    indexQuery.close();

    session.command("truncate cluster " + clusterName);

    Assert.assertEquals(0, session.countClass(className));
    Assert.assertEquals(0, session.countClusterElements(clusterId));

    indexQuery = session.query("select from TruncateClusterClass where value='val'");

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
