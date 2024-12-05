package com.jetbrains.youtrack.db.internal.core.db.graph;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.storage.YTRecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestGraphOperations extends DBTestBase {

  @Test
  public void testEdgeUniqueConstraint() {

    db.createVertexClass("TestVertex");

    YTClass testLabel = db.createEdgeClass("TestLabel");

    YTProperty key = testLabel.createProperty(db, "key", YTType.STRING);

    key.createIndex(db, YTClass.INDEX_TYPE.UNIQUE);

    db.begin();
    Vertex vertex = db.newVertex("TestVertex");

    Vertex vertex1 = db.newVertex("TestVertex");

    Edge edge = vertex.addEdge(vertex1, "TestLabel");

    edge.setProperty("key", "unique");
    db.save(vertex);
    db.commit();

    try {
      db.begin();
      edge = db.bindToSession(vertex).addEdge(db.bindToSession(vertex1), "TestLabel");
      edge.setProperty("key", "unique");
      db.save(edge);
      db.commit();
      Assert.fail("It should not be inserted  a duplicated edge");
    } catch (YTRecordDuplicatedException e) {

    }

    db.begin();
    edge = db.bindToSession(vertex).addEdge(db.bindToSession(vertex1), "TestLabel");
    edge.setProperty("key", "notunique");
    db.save(edge);
    db.commit();
  }
}
