package com.orientechnologies.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTSchemaException;
import org.junit.Assert;
import org.junit.Test;

public class AlterPropertyTest extends DBTestBase {

  @Test
  public void testPropertyRenaming() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestPropertyRenaming");
    YTProperty property = classA.createProperty(db, "propertyOld", YTType.STRING);
    assertEquals(property, classA.getProperty("propertyOld"));
    assertNull(classA.getProperty("propertyNew"));
    property.setName(db, "propertyNew");
    assertNull(classA.getProperty("propertyOld"));
    assertEquals(property, classA.getProperty("propertyNew"));
  }

  @Test
  public void testPropertyRenamingReload() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestPropertyRenaming");
    YTProperty property = classA.createProperty(db, "propertyOld", YTType.STRING);
    assertEquals(property, classA.getProperty("propertyOld"));
    assertNull(classA.getProperty("propertyNew"));
    property.setName(db, "propertyNew");
    classA = schema.getClass("TestPropertyRenaming");
    assertNull(classA.getProperty("propertyOld"));
    assertEquals(property, classA.getProperty("propertyNew"));
  }

  @Test
  public void testLinkedMapPropertyLinkedType() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestMapProperty");
    try {
      classA.createProperty(db, "propertyMap", YTType.LINKMAP, YTType.STRING);
      fail("create linkmap property should not allow linked type");
    } catch (YTSchemaException e) {

    }

    YTProperty prop = classA.getProperty("propertyMap");
    assertNull(prop);
  }

  @Test
  public void testLinkedMapPropertyLinkedClass() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestMapProperty");
    YTClass classLinked = schema.createClass("LinkedClass");
    try {
      classA.createProperty(db, "propertyString", YTType.STRING, classLinked);
      fail("create linkmap property should not allow linked type");
    } catch (YTSchemaException e) {

    }

    YTProperty prop = classA.getProperty("propertyString");
    assertNull(prop);
  }

  @Test
  public void testRemoveLinkedClass() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestRemoveLinkedClass");
    YTClass classLinked = schema.createClass("LinkedClass");
    YTProperty prop = classA.createProperty(db, "propertyLink", YTType.LINK, classLinked);
    assertNotNull(prop.getLinkedClass());
    prop.setLinkedClass(db, null);
    assertNull(prop.getLinkedClass());
  }

  @Test
  public void testRemoveLinkedClassSQL() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestRemoveLinkedClass");
    YTClass classLinked = schema.createClass("LinkedClass");
    YTProperty prop = classA.createProperty(db, "propertyLink", YTType.LINK, classLinked);
    assertNotNull(prop.getLinkedClass());
    db.command("alter property TestRemoveLinkedClass.propertyLink linkedclass null").close();
    assertNull(prop.getLinkedClass());
  }

  @Test
  public void testMax() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("TestWrongMax");
    YTProperty prop = classA.createProperty(db, "dates", YTType.EMBEDDEDLIST, YTType.DATE);

    db.command("alter property TestWrongMax.dates max 2016-05-25").close();

    try {
      db.command("alter property TestWrongMax.dates max '2016-05-25'").close();
      Assert.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testAlterPropertyWithDot() {

    YTSchema schema = db.getMetadata().getSchema();
    db.command("create class testAlterPropertyWithDot").close();
    db.command("create property testAlterPropertyWithDot.`a.b` STRING").close();
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty("a.b"));
    db.command("alter property testAlterPropertyWithDot.`a.b` name c").close();
    Assert.assertNull(schema.getClass("testAlterPropertyWithDot").getProperty("a.b"));
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty("c"));
  }

  @Test
  public void testAlterCustomAttributeInProperty() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("TestCreateCustomAttributeClass");
    YTProperty property = oClass.createProperty(db, "property", YTType.STRING);

    property.setCustom(db, "customAttribute", "value1");
    assertEquals("value1", property.getCustom("customAttribute"));

    property.setCustom(db, "custom.attribute", "value2");
    assertEquals("value2", property.getCustom("custom.attribute"));
  }
}
