package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.YTSchemaException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 *
 */
public class AlterSuperclassTest extends DBTestBase {

  @Test
  public void testSamePropertyCheck() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ParentClass");
    classA.setAbstract(db, true);
    YTProperty property = classA.createProperty(db, "RevNumberNine", YTType.INTEGER);
    YTClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), List.of(classA));
    YTClass classChild2 = schema.createClass("ChildClass2", classChild);
    assertEquals(classChild2.getSuperClasses(), List.of(classChild));
    classChild2.setSuperClasses(db, List.of(classA));
    assertEquals(classChild2.getSuperClasses(), List.of(classA));
  }

  @Test(expected = YTSchemaException.class)
  public void testPropertyNameConflict() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ParentClass");
    classA.setAbstract(db, true);
    YTProperty property = classA.createProperty(db, "RevNumberNine", YTType.INTEGER);
    YTClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), List.of(classA));
    YTClass classChild2 = schema.createClass("ChildClass2");
    classChild2.createProperty(db, "RevNumberNine", YTType.STRING);
    classChild2.setSuperClasses(db, List.of(classChild));
  }

  @Test(expected = YTSchemaException.class)
  public void testHasAlreadySuperclass() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ParentClass");
    YTClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Collections.singletonList(classA));
    classChild.addSuperClass(db, classA);
  }

  @Test(expected = YTSchemaException.class)
  public void testSetDuplicateSuperclasses() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ParentClass");
    YTClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Collections.singletonList(classA));
    classChild.setSuperClasses(db, Arrays.asList(classA, classA));
  }

  /**
   * This tests fixes a problem created in Issue #5586. It should not throw
   * ArrayIndexOutOfBoundsException
   */
  @Test
  public void testBrokenDbAlteringSuperClass() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("BaseClass");
    YTClass classChild = schema.createClass("ChildClass1", classA);
    YTClass classChild2 = schema.createClass("ChildClass2", classA);

    classChild2.setSuperClass(db, classChild);

    schema.dropClass("ChildClass2");
  }
}
