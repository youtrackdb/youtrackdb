package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OTruncateClusterStatementExecutionTest extends DBTestBase {

  @Test
  public void testClusterWithIndex() {
    final String clusterName = "TruncateClusterWithIndex";
    final int clusterId = db.addCluster(clusterName);

    final String className = "TruncateClusterClass";
    final YTSchema schema = db.getMetadata().getSchema();

    final YTClass clazz = schema.createClass(className);
    clazz.addClusterId(db, clusterId);

    clazz.createProperty(db, "value", YTType.STRING);
    clazz.createIndex(db, "TruncateClusterIndex", YTClass.INDEX_TYPE.UNIQUE, "value");

    db.begin();
    final YTDocument document = new YTDocument();
    document.field("value", "val");

    document.save(clusterName);
    db.commit();

    Assert.assertEquals(db.countClass(className), 1);
    Assert.assertEquals(db.countClusterElements(clusterId), 1);

    YTResultSet indexQuery = db.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(toList(indexQuery).size(), 1);
    indexQuery.close();

    db.command("truncate cluster " + clusterName);

    Assert.assertEquals(db.countClass(className), 0);
    Assert.assertEquals(db.countClusterElements(clusterId), 0);

    indexQuery = db.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(toList(indexQuery).size(), 0);
    indexQuery.close();
  }

  private List<YTResult> toList(YTResultSet input) {
    List<YTResult> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }
}
