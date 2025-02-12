package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

/**
 *
 */
public class CreateSchemaPropertyStatementExecutionTest extends DbTestBase {

  private static final String PROP_NAME = "name";

  private static final String PROP_DIVISION = "division";

  private static final String PROP_OFFICERS = "officers";

  private static final String PROP_ID = "id";

  @Test
  public void testBasicCreateProperty() throws Exception {
    session.command("CREATE class testBasicCreateProperty").close();
    session.command("CREATE property testBasicCreateProperty.name STRING").close();

    var companyClass = session.getMetadata().getSchema().getClass("testBasicCreateProperty");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testBasicCreateProperty.name", nameProperty.getFullName(session));
    assertEquals(PropertyType.STRING, nameProperty.getType(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testBasicUnsafeCreateProperty() throws Exception {
    session.command("CREATE class testBasicUnsafeCreateProperty").close();
    session.command("CREATE property testBasicUnsafeCreateProperty.name STRING UNSAFE").close();

    var companyClass = session.getMetadata().getSchema()
        .getClass("testBasicUnsafeCreateProperty");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testBasicUnsafeCreateProperty.name", nameProperty.getFullName(session));
    assertEquals(PropertyType.STRING, nameProperty.getType(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreatePropertyWithLinkedClass() throws Exception {
    session.command("CREATE class testCreatePropertyWithLinkedClass_1").close();
    session.command("CREATE class testCreatePropertyWithLinkedClass_2").close();
    session.command(
            "CREATE property testCreatePropertyWithLinkedClass_2.division LINK"
                + " testCreatePropertyWithLinkedClass_1")
        .close();

    var companyClass =
        session.getMetadata().getSchema().getClass("testCreatePropertyWithLinkedClass_2");
    var nameProperty = companyClass.getProperty(session, PROP_DIVISION);

    assertEquals(PROP_DIVISION, nameProperty.getName(session));
    assertEquals("testCreatePropertyWithLinkedClass_2.division", nameProperty.getFullName(session));
    assertEquals(PropertyType.LINK, nameProperty.getType(session));
    assertEquals("testCreatePropertyWithLinkedClass_1",
        nameProperty.getLinkedClass(session).getName(
            session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreatePropertyWithEmbeddedType() throws Exception {
    session.command("CREATE Class testCreatePropertyWithEmbeddedType").close();
    session.command(
            "CREATE Property testCreatePropertyWithEmbeddedType.officers EMBEDDEDLIST STRING")
        .close();

    var companyClass =
        session.getMetadata().getSchema().getClass("testCreatePropertyWithEmbeddedType");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals("testCreatePropertyWithEmbeddedType.officers", nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateMandatoryProperty() throws Exception {
    session.command("CREATE class testCreateMandatoryProperty").close();
    session.command("CREATE property testCreateMandatoryProperty.name STRING (MANDATORY)").close();

    var companyClass = session.getMetadata().getSchema().getClass("testCreateMandatoryProperty");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testCreateMandatoryProperty.name", nameProperty.getFullName(session));
    assertTrue(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateNotNullProperty() throws Exception {
    session.command("CREATE class testCreateNotNullProperty").close();
    session.command("CREATE property testCreateNotNullProperty.name STRING (NOTNULL)").close();

    var companyClass = session.getMetadata().getSchema().getClass("testCreateNotNullProperty");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testCreateNotNullProperty.name", nameProperty.getFullName(session));
    assertFalse(nameProperty.isMandatory(session));
    assertTrue(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateReadOnlyProperty() throws Exception {
    session.command("CREATE class testCreateReadOnlyProperty").close();
    session.command("CREATE property testCreateReadOnlyProperty.name STRING (READONLY)").close();

    var companyClass = session.getMetadata().getSchema().getClass("testCreateReadOnlyProperty");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testCreateReadOnlyProperty.name", nameProperty.getFullName(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertTrue(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateReadOnlyFalseProperty() throws Exception {
    session.command("CREATE class testCreateReadOnlyFalseProperty").close();
    session.command("CREATE property testCreateReadOnlyFalseProperty.name STRING (READONLY false)")
        .close();

    var companyClass = session.getMetadata().getSchema()
        .getClass("testCreateReadOnlyFalseProperty");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testCreateReadOnlyFalseProperty.name", nameProperty.getFullName(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() throws Exception {
    session.command("CREATE Class testCreateMandatoryPropertyWithEmbeddedType").close();
    session.command(
            "CREATE Property testCreateMandatoryPropertyWithEmbeddedType.officers EMBEDDEDLIST"
                + " STRING (MANDATORY)")
        .close();

    var companyClass =
        session.getMetadata().getSchema().getClass("testCreateMandatoryPropertyWithEmbeddedType");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals(
        "testCreateMandatoryPropertyWithEmbeddedType.officers", nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
    assertTrue(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() throws Exception {
    session.command("CREATE Class testCreateUnsafePropertyWithEmbeddedType").close();
    session.command(
            "CREATE Property testCreateUnsafePropertyWithEmbeddedType.officers EMBEDDEDLIST STRING"
                + " UNSAFE")
        .close();

    var companyClass =
        session.getMetadata().getSchema().getClass("testCreateUnsafePropertyWithEmbeddedType");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals("testCreateUnsafePropertyWithEmbeddedType.officers",
        nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
  }

  @Test
  public void testComplexCreateProperty() throws Exception {
    session.command("CREATE Class testComplexCreateProperty").close();
    session.command(
            "CREATE Property testComplexCreateProperty.officers EMBEDDEDLIST STRING (MANDATORY,"
                + " READONLY, NOTNULL) UNSAFE")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("testComplexCreateProperty");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals("testComplexCreateProperty.officers", nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
    assertTrue(nameProperty.isMandatory(session));
    assertTrue(nameProperty.isNotNull(session));
    assertTrue(nameProperty.isReadonly(session));
  }

  @Test
  public void testLinkedTypeDefaultAndMinMaxUnsafeProperty() throws Exception {
    session.command("CREATE CLASS testLinkedTypeDefaultAndMinMaxUnsafeProperty").close();
    session.command(
            "CREATE PROPERTY testLinkedTypeDefaultAndMinMaxUnsafeProperty.id EMBEDDEDLIST Integer"
                + " (DEFAULT 5, MIN 1, MAX 10) UNSAFE")
        .close();

    var companyClass =
        session.getMetadata().getSchema().getClass("testLinkedTypeDefaultAndMinMaxUnsafeProperty");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals("testLinkedTypeDefaultAndMinMaxUnsafeProperty.id",
        idProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, idProperty.getType(session));
    assertEquals(PropertyType.INTEGER, idProperty.getLinkedType(session));
    assertFalse(idProperty.isMandatory(session));
    assertFalse(idProperty.isNotNull(session));
    assertFalse(idProperty.isReadonly(session));
    assertEquals("5", idProperty.getDefaultValue(session));
    assertEquals("1", idProperty.getMin(session));
    assertEquals("10", idProperty.getMax(session));
  }

  @Test
  public void testDefaultAndMinMaxUnsafeProperty() throws Exception {
    session.command("CREATE CLASS testDefaultAndMinMaxUnsafeProperty").close();
    session.command(
            "CREATE PROPERTY testDefaultAndMinMaxUnsafeProperty.id INTEGER (DEFAULT 5, MIN 1, MAX"
                + " 10) UNSAFE")
        .close();

    var companyClass =
        session.getMetadata().getSchema().getClass("testDefaultAndMinMaxUnsafeProperty");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals("testDefaultAndMinMaxUnsafeProperty.id", idProperty.getFullName(session));
    assertEquals(PropertyType.INTEGER, idProperty.getType(session));
    assertNull(idProperty.getLinkedType(session));
    assertFalse(idProperty.isMandatory(session));
    assertFalse(idProperty.isNotNull(session));
    assertFalse(idProperty.isReadonly(session));
    assertEquals("5", idProperty.getDefaultValue(session));
    assertEquals("1", idProperty.getMin(session));
    assertEquals("10", idProperty.getMax(session));
  }

  @Test
  public void testExtraSpaces() throws Exception {
    session.command("CREATE CLASS testExtraSpaces").close();
    session.command(
            "CREATE PROPERTY testExtraSpaces.id INTEGER  ( DEFAULT  5 ,  MANDATORY  )  UNSAFE ")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("testExtraSpaces");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals("testExtraSpaces.id", idProperty.getFullName(session));
    assertEquals(PropertyType.INTEGER, idProperty.getType(session));
    assertNull(idProperty.getLinkedType(session));
    assertTrue(idProperty.isMandatory(session));
    assertEquals("5", idProperty.getDefaultValue(session));
  }

  @Test(expected = CommandExecutionException.class)
  public void testInvalidAttributeName() throws Exception {
    session.command("CREATE CLASS CommandExecutionException").close();
    session.command(
            "CREATE PROPERTY CommandExecutionException.id INTEGER (MANDATORY, INVALID, NOTNULL) "
                + " UNSAFE")
        .close();
  }

  @Test(expected = CommandExecutionException.class)
  public void testMissingAttributeValue() throws Exception {
    session.command("CREATE CLASS testMissingAttributeValue").close();
    session.command("CREATE PROPERTY testMissingAttributeValue.id INTEGER (DEFAULT)  UNSAFE")
        .close();
  }

  @Test
  public void testMandatoryAsLinkedName() throws Exception {
    session.command("CREATE CLASS testMandatoryAsLinkedName").close();
    session.command("CREATE CLASS testMandatoryAsLinkedName_2").close();
    session.command(
            "CREATE PROPERTY testMandatoryAsLinkedName.id EMBEDDEDLIST testMandatoryAsLinkedName_2"
                + " UNSAFE")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("testMandatoryAsLinkedName");
    var mandatoryClass = session.getMetadata().getSchema()
        .getClass("testMandatoryAsLinkedName_2");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals("testMandatoryAsLinkedName.id", idProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, idProperty.getType(session));
    assertEquals(idProperty.getLinkedClass(session), mandatoryClass);
    assertFalse(idProperty.isMandatory(session));
  }

  @Test
  public void testIfNotExists() throws Exception {
    session.command("CREATE class testIfNotExists").close();
    session.command("CREATE property testIfNotExists.name if not exists STRING").close();

    var clazz = session.getMetadata().getSchema().getClass("testIfNotExists");
    var nameProperty = clazz.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testIfNotExists.name", nameProperty.getFullName(session));
    assertEquals(PropertyType.STRING, nameProperty.getType(session));

    session.command("CREATE property testIfNotExists.name if not exists STRING").close();

    clazz = session.getMetadata().getSchema().getClass("testIfNotExists");
    nameProperty = clazz.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals("testIfNotExists.name", nameProperty.getFullName(session));
    assertEquals(PropertyType.STRING, nameProperty.getType(session));
  }
}
