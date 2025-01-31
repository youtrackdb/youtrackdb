package com.jetbrains.youtrack.db.internal.core.db.graph;

import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestGraphOperations extends DbTestBase {

  @Test
  public void testEdgeUniqueConstraint() {

    db.createVertexClass("TestVertex");

    SchemaClass testLabel = db.createEdgeClass("TestLabel");

    SchemaProperty key = testLabel.createProperty(db, "key", PropertyType.STRING);

    key.createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);

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
    } catch (RecordDuplicatedException e) {

    }

    db.begin();
    edge = db.bindToSession(vertex).addEdge(db.bindToSession(vertex1), "TestLabel");
    edge.setProperty("key", "notunique");
    db.save(edge);
    db.commit();
  }
}
