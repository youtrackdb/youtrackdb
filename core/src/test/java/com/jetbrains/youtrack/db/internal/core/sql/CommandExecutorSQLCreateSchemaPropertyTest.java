/*
 *
 *  *  Copyright YouTrackDB
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.sql;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import org.junit.Test;

/**
 *
 */
public class CommandExecutorSQLCreateSchemaPropertyTest extends BaseMemoryInternalDatabase {

  private static final String PROP_NAME = "name";
  private static final String PROP_FULL_NAME = "company.name";
  private static final String PROP_DIVISION = "division";
  private static final String PROP_FULL_DIVISION = "company.division";
  private static final String PROP_OFFICERS = "officers";
  private static final String PROP_FULL_OFFICERS = "company.officers";
  private static final String PROP_ID = "id";
  private static final String PROP_FULL_ID = "company.id";

  @Test
  public void testBasicCreateProperty() throws Exception {

    session.command("CREATE class company").close();
    session.command("CREATE property company.name STRING").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals(PROP_FULL_NAME, nameProperty.getFullName(session));
    assertEquals(PropertyType.STRING, nameProperty.getType(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testBasicUnsafeCreateProperty() throws Exception {

    session.command("CREATE class company").close();
    session.command("CREATE property company.name STRING UNSAFE").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals(PROP_FULL_NAME, nameProperty.getFullName(session));
    assertEquals(PropertyType.STRING, nameProperty.getType(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreatePropertyWithLinkedClass() throws Exception {
    session.command("CREATE class division").close();
    session.command("CREATE class company").close();
    session.command("CREATE property company.division LINK division").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_DIVISION);

    assertEquals(PROP_DIVISION, nameProperty.getName(session));
    assertEquals(PROP_FULL_DIVISION, nameProperty.getFullName(session));
    assertEquals(PropertyType.LINK, nameProperty.getType(session));
    assertEquals("division", nameProperty.getLinkedClass(session).getName(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreatePropertyWithEmbeddedType() throws Exception {

    session.command("CREATE Class company").close();
    session.command("CREATE Property company.officers EMBEDDEDLIST STRING").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals(PROP_FULL_OFFICERS, nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateMandatoryProperty() throws Exception {
    session.command("CREATE class company").close();
    session.command("CREATE property company.name STRING (MANDATORY)").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals(PROP_FULL_NAME, nameProperty.getFullName(session));
    assertTrue(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateNotNullProperty() {
    session.command("CREATE class company").close();
    session.command("CREATE property company.name STRING (NOTNULL)").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals(PROP_FULL_NAME, nameProperty.getFullName(session));
    assertFalse(nameProperty.isMandatory(session));
    assertTrue(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateReadOnlyProperty() {
    session.command("CREATE class company").close();
    session.command("CREATE property company.name STRING (READONLY)").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals(PROP_FULL_NAME, nameProperty.getFullName(session));
    assertFalse(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertTrue(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateReadOnlyFalseProperty() {
    session.command("CREATE class company").close();
    session.command("CREATE property company.name STRING (READONLY false)").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_NAME);

    assertEquals(PROP_NAME, nameProperty.getName(session));
    assertEquals(PROP_FULL_NAME, nameProperty.getFullName(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() {

    session.command("CREATE Class company").close();
    session.command("CREATE Property company.officers EMBEDDEDLIST STRING (MANDATORY)").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals(PROP_FULL_OFFICERS, nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
    assertTrue(nameProperty.isMandatory(session));
    assertFalse(nameProperty.isNotNull(session));
    assertFalse(nameProperty.isReadonly(session));
  }

  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() {

    session.command("CREATE Class company").close();
    session.command("CREATE Property company.officers EMBEDDEDLIST STRING UNSAFE").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals(PROP_FULL_OFFICERS, nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
  }

  @Test
  public void testComplexCreateProperty() throws Exception {

    session.command("CREATE Class company").close();
    session.command(
            "CREATE Property company.officers EMBEDDEDLIST STRING (MANDATORY, READONLY, NOTNULL)"
                + " UNSAFE")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var nameProperty = companyClass.getProperty(session, PROP_OFFICERS);

    assertEquals(PROP_OFFICERS, nameProperty.getName(session));
    assertEquals(PROP_FULL_OFFICERS, nameProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, nameProperty.getType(session));
    assertEquals(PropertyType.STRING, nameProperty.getLinkedType(session));
    assertTrue(nameProperty.isMandatory(session));
    assertTrue(nameProperty.isNotNull(session));
    assertTrue(nameProperty.isReadonly(session));
  }

  @Test
  public void testLinkedTypeDefaultAndMinMaxUnsafeProperty() {
    session.command("CREATE CLASS company").close();
    session.command(
            "CREATE PROPERTY company.id EMBEDDEDLIST Integer (DEFAULT 5, MIN 1, MAX 10) UNSAFE")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals(PROP_FULL_ID, idProperty.getFullName(session));
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
  public void testDefaultAndMinMaxUnsafeProperty() {
    session.command("CREATE CLASS company").close();
    session.command("CREATE PROPERTY company.id INTEGER (DEFAULT 5, MIN 1, MAX 10) UNSAFE").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals(PROP_FULL_ID, idProperty.getFullName(session));
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
    session.command("CREATE CLASS company").close();
    session.command("CREATE PROPERTY company.id INTEGER  ( DEFAULT  5 ,  MANDATORY  )  UNSAFE ")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals(PROP_FULL_ID, idProperty.getFullName(session));
    assertEquals(PropertyType.INTEGER, idProperty.getType(session));
    assertNull(idProperty.getLinkedType(session));
    assertTrue(idProperty.isMandatory(session));
    assertEquals("5", idProperty.getDefaultValue(session));
  }

  @Test
  public void testNonStrict() throws Exception {

    session.getStorage().setProperty(SQLStatement.CUSTOM_STRICT_SQL, "false");

    session.command("CREATE CLASS company").close();
    session.command(
            "CREATE PROPERTY company.id INTEGER (MANDATORY, NOTNULL false, READONLY true, MAX 10,"
                + " MIN 4, DEFAULT 6)  UNSAFE")
        .close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals(PROP_FULL_ID, idProperty.getFullName(session));
    assertEquals(PropertyType.INTEGER, idProperty.getType(session));
    assertNull(idProperty.getLinkedType(session));
    assertTrue(idProperty.isMandatory(session));
    assertFalse(idProperty.isNotNull(session));
    assertTrue(idProperty.isReadonly(session));
    assertEquals("4", idProperty.getMin(session));
    assertEquals("10", idProperty.getMax(session));
    assertEquals("6", idProperty.getDefaultValue(session));
  }

  @Test(expected = CommandExecutionException.class)
  public void testInvalidAttributeName() throws Exception {
    session.command("CREATE CLASS company").close();
    session.command("CREATE PROPERTY company.id INTEGER (MANDATORY, INVALID, NOTNULL)  UNSAFE")
        .close();
  }

  @Test(expected = CommandExecutionException.class)
  public void testMissingAttributeValue() throws Exception {

    session.command("CREATE CLASS company").close();
    session.command("CREATE PROPERTY company.id INTEGER (DEFAULT)  UNSAFE").close();
  }

  @Test(expected = CommandSQLParsingException.class)
  public void tooManyAttributeParts() throws Exception {

    session.command("CREATE CLASS company").close();
    session.command("CREATE PROPERTY company.id INTEGER (DEFAULT 5 10)  UNSAFE").close();
  }

  @Test
  public void testMandatoryAsLinkedName() throws Exception {
    session.command("CREATE CLASS company").close();
    session.command("CREATE CLASS Mandatory").close();
    session.command("CREATE PROPERTY company.id EMBEDDEDLIST Mandatory UNSAFE").close();

    var companyClass = session.getMetadata().getSchema().getClass("company");
    var mandatoryClass = session.getMetadata().getSchema().getClass("Mandatory");
    var idProperty = companyClass.getProperty(session, PROP_ID);

    assertEquals(PROP_ID, idProperty.getName(session));
    assertEquals(PROP_FULL_ID, idProperty.getFullName(session));
    assertEquals(PropertyType.EMBEDDEDLIST, idProperty.getType(session));
    assertEquals(idProperty.getLinkedClass(session), mandatoryClass);
    assertFalse(idProperty.isMandatory(session));
  }

  @Test
  public void testIfNotExists() {
    session.command("CREATE class testIfNotExists").close();
    session.command("CREATE property testIfNotExists.name if not exists STRING").close();

    var companyClass = session.getMetadata().getSchema().getClass("testIfNotExists");
    var property = companyClass.getProperty(session, "name");
    assertEquals(PROP_NAME, property.getName(session));

    session.command("CREATE property testIfNotExists.name if not exists STRING").close();

    companyClass = session.getMetadata().getSchema().getClass("testIfNotExists");
    property = companyClass.getProperty(session, "name");
    assertEquals(PROP_NAME, property.getName(session));
  }
}
