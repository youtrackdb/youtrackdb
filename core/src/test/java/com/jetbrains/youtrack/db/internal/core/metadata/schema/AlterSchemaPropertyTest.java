package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AlterSchemaPropertyTest extends DbTestBase {

  @Test
  public void testPropertyRenaming() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestPropertyRenaming");
    var property = classA.createProperty(session, "propertyOld", PropertyType.STRING);
    assertEquals(property, classA.getProperty(session, "propertyOld"));
    assertNull(classA.getProperty(session, "propertyNew"));
    property.setName(session, "propertyNew");
    assertNull(classA.getProperty(session, "propertyOld"));
    assertEquals(property, classA.getProperty(session, "propertyNew"));
  }

  @Test
  public void testPropertyRenamingReload() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestPropertyRenaming");
    var property = classA.createProperty(session, "propertyOld", PropertyType.STRING);
    assertEquals(property, classA.getProperty(session, "propertyOld"));
    assertNull(classA.getProperty(session, "propertyNew"));
    property.setName(session, "propertyNew");
    classA = schema.getClass("TestPropertyRenaming");
    assertNull(classA.getProperty(session, "propertyOld"));
    assertEquals(property, classA.getProperty(session, "propertyNew"));
  }

  @Test
  public void testLinkedMapPropertyLinkedType() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestMapProperty");
    try {
      classA.createProperty(session, "propertyMap", PropertyType.LINKMAP, PropertyType.STRING);
      fail("create linkmap property should not allow linked type");
    } catch (SchemaException e) {

    }

    var prop = classA.getProperty(session, "propertyMap");
    assertNull(prop);
  }

  @Test
  public void testLinkedMapPropertyLinkedClass() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestMapProperty");
    var classLinked = schema.createClass("LinkedClass");
    try {
      classA.createProperty(session, "propertyString", PropertyType.STRING, classLinked);
      fail("create linkmap property should not allow linked type");
    } catch (SchemaException e) {

    }

    var prop = classA.getProperty(session, "propertyString");
    assertNull(prop);
  }

  @Test
  public void testRemoveLinkedClass() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestRemoveLinkedClass");
    var classLinked = schema.createClass("LinkedClass");
    var prop = classA.createProperty(session, "propertyLink", PropertyType.LINK, classLinked);
    assertNotNull(prop.getLinkedClass(session));
    prop.setLinkedClass(session, null);
    assertNull(prop.getLinkedClass(session));
  }

  @Test
  public void testRemoveLinkedClassSQL() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestRemoveLinkedClass");
    var classLinked = schema.createClass("LinkedClass");
    var prop = classA.createProperty(session, "propertyLink", PropertyType.LINK, classLinked);
    assertNotNull(prop.getLinkedClass(session));
    session.command("alter property TestRemoveLinkedClass.propertyLink linkedclass null").close();
    assertNull(prop.getLinkedClass(session));
  }

  @Test
  public void testMax() {
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("TestWrongMax");
    var prop = classA.createProperty(session, "dates", PropertyType.EMBEDDEDLIST,
        PropertyType.DATE);

    session.command("alter property TestWrongMax.dates max 2016-05-25").close();

    try {
      session.command("alter property TestWrongMax.dates max '2016-05-25'").close();
      Assert.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testAlterPropertyWithDot() {

    Schema schema = session.getMetadata().getSchema();
    session.command("create class testAlterPropertyWithDot").close();
    session.command("create property testAlterPropertyWithDot.`a.b` STRING").close();
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty(session, "a.b"));
    session.command("alter property testAlterPropertyWithDot.`a.b` name c").close();
    Assert.assertNull(schema.getClass("testAlterPropertyWithDot").getProperty(session, "a.b"));
    Assert.assertNotNull(schema.getClass("testAlterPropertyWithDot").getProperty(session, "c"));
  }

  @Test
  public void testAlterCustomAttributeInProperty() {
    Schema schema = session.getMetadata().getSchema();
    var oClass = schema.createClass("TestCreateCustomAttributeClass");
    var property = oClass.createProperty(session, "property", PropertyType.STRING);

    property.setCustom(session, "customAttribute", "value1");
    assertEquals("value1", property.getCustom(session, "customAttribute"));

    property.setCustom(session, "custom.attribute", "value2");
    assertEquals("value2", property.getCustom(session, "custom.attribute"));
  }
}
