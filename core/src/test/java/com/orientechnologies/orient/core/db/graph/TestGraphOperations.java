package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;
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
    YTVertex vertex = db.newVertex("TestVertex");

    YTVertex vertex1 = db.newVertex("TestVertex");

    YTEdge edge = vertex.addEdge(vertex1, "TestLabel");

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
