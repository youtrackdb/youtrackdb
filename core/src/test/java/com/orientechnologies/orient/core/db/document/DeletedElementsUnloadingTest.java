package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import org.junit.Assert;
import org.junit.Test;

public class DeletedElementsUnloadingTest extends BaseMemoryDatabase {
  @Test
  public void linkedElementIsUnloadedDuringDeletion() {
    var elementOne = db.newElement();
    var elementTwo = db.newElement();

    elementOne.setProperty("linked", elementTwo, OType.LINK);
    elementOne.save();

    elementTwo = elementOne.getProperty("linked");
    Assert.assertNotNull(elementTwo);

    elementTwo.delete();

    Assert.assertNull(elementOne.getProperty("linked"));
  }

  @Test
  public void invalidateNewlyAddedLink() {
    var elementOne = db.newElement();
    var elementTwo = db.newElement();

    elementOne.setProperty("linked", elementTwo, OType.LINK);

    elementTwo = elementOne.getProperty("linked");
    Assert.assertNotNull(elementTwo);

    elementTwo.delete();

    Assert.assertNull(elementOne.getProperty("linked"));
  }

  @Test
  public void doubleReplacePropertyTest() {
    var elementOne = db.newElement();
    var elementTwo = db.newElement();

    elementOne.setProperty("linked", elementTwo, OType.LINK);
    elementOne.save();

    elementOne.getProperty("linked");
    elementOne.setProperty("linked", "test");

    elementOne.setProperty("linked", elementTwo, OType.LINK);

    elementTwo = elementOne.getProperty("linked");

    Assert.assertNotNull(elementTwo);
    elementTwo.delete();

    Assert.assertNull(elementOne.getProperty("linked"));
  }

  @Test
  public void doubleReplacePropertyNotSaveTest() {
    var elementOne = db.newElement();
    var elementTwo = db.newElement();

    elementOne.setProperty("linked", elementTwo, OType.LINK);

    elementOne.getProperty("linked");
    elementOne.setProperty("linked", "test");

    elementOne.setProperty("linked", elementTwo, OType.LINK);

    elementTwo = elementOne.getProperty("linked");

    Assert.assertNotNull(elementTwo);
    elementTwo.delete();

    Assert.assertNull(elementOne.getProperty("linked"));
  }

  @Test
  public void deleteEdgesTest() {
    var vertexOne = db.newVertex();

    var vertexTwo = db.newVertex();
    var vertexThree = db.newVertex();
    var vertexFour = db.newVertex();

    db.createEdgeClass("linked");

    vertexOne.addEdge(vertexTwo, "linked");
    vertexOne.addEdge(vertexThree, "linked");
    vertexOne.addEdge(vertexFour, "linked");

    vertexOne.save();

    vertexOne.getEdges(ODirection.OUT, "linked").forEach(edge -> edge.getTo().delete());
    var edges = vertexOne.getEdges(ODirection.OUT, "linked");
    Assert.assertFalse(edges.iterator().hasNext());
  }
}
