/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CRUDInheritanceTest extends BaseDBTest {

  @Parameters(value = "remote")
  public CRUDInheritanceTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    createCompanyClass();
  }

  @Test
  public void create() {
    session.command("delete from Company").close();

    generateCompanyData();
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    Assert.assertEquals(session.countClass("Company"), TOT_COMPANY_RECORDS);
  }

  @Test(dependsOnMethods = "testCreate")
  public void queryByBaseType() {
    var resultSet = executeQuery("select from Company where name.length() > 0");

    Assert.assertFalse(resultSet.isEmpty());
    Assert.assertEquals(resultSet.size(), TOT_COMPANY_RECORDS);

    var companyRecords = 0;
    Entity account;
    for (var entries : resultSet) {
      account = entries.asEntity();

      if ("Company".equals(account.getClassName())) {
        companyRecords++;
      }

      Assert.assertNotSame(account.<String>getProperty("name").length(), 0);
    }

    Assert.assertEquals(companyRecords, TOT_COMPANY_RECORDS);
  }

  @Test(dependsOnMethods = "queryByBaseType")
  public void queryPerSuperType() {
    var resultSet = executeQuery("select * from Company where name.length() > 0");

    Assert.assertEquals(resultSet.size(), TOT_COMPANY_RECORDS);

    Entity account;
    for (var entries : resultSet) {
      account = entries.asEntity();
      Assert.assertNotSame(account.<String>getProperty("name").length(), 0);
    }
  }

  @Test(dependsOnMethods = {"queryPerSuperType", "testCreate"})
  public void deleteFirst() {
    // DELETE ALL THE RECORD IN THE CLUSTER
    session.begin();
    var companyClusterIterator = session.browseClass("Company");
    for (Entity obj : companyClusterIterator) {
      if (obj.<Integer>getProperty("id") == 1) {
        session.delete(obj);
        break;
      }
    }
    session.commit();

    Assert.assertEquals(session.countClass("Company"), TOT_COMPANY_RECORDS - 1);
  }

  @Test(dependsOnMethods = "deleteFirst")
  public void testSuperclassInheritanceCreation() {
    session.close();
    session = createSessionInstance();

    createInheritanceTestClass();

    var abstractClass =
        session.getMetadata().getSchema().getClass("InheritanceTestAbstractClass");
    var baseClass = session.getMetadata().getSchema().getClass("InheritanceTestBaseClass");
    var testClass = session.getMetadata().getSchema().getClass("InheritanceTestClass");

    Assert.assertTrue(baseClass.getSuperClasses(session).contains(abstractClass));
    Assert.assertTrue(testClass.getSuperClasses(session).contains(baseClass));
  }

  @Test
  public void testIdFieldInheritanceFirstSubClass() {
    createInheritanceTestClass();

    var a = session.newInstance("InheritanceTestBaseClass");
    var b = session.newInstance("InheritanceTestClass");

    session.begin();
    session.save(a);
    session.save(b);
    session.commit();

    var resultSet = executeQuery("select from InheritanceTestBaseClass");
    Assert.assertEquals(resultSet.size(), 2);
  }

  @Test
  public void testKeywordClass() {
    var klass = session.getMetadata().getSchema().createClass("Not");

    var klass1 = session.getMetadata().getSchema().createClass("Extends_Not", klass);
    Assert.assertEquals(klass1.getSuperClasses(session).size(), 1, 1);
    Assert.assertEquals(klass1.getSuperClasses(session).getFirst().getName(session), "Not");
  }

  @Test
  public void testSchemaGeneration() {
    var schema = session.getMetadata().getSchema();
    var testSchemaClass = schema.createClass("JavaTestSchemaGeneration");
    var childClass = schema.createClass("TestSchemaGenerationChild");

    testSchemaClass.createProperty(session, "text", PropertyType.STRING);
    testSchemaClass.createProperty(session, "enumeration", PropertyType.STRING);
    testSchemaClass.createProperty(session, "numberSimple", PropertyType.INTEGER);
    testSchemaClass.createProperty(session, "longSimple", PropertyType.LONG);
    testSchemaClass.createProperty(session, "doubleSimple", PropertyType.DOUBLE);
    testSchemaClass.createProperty(session, "floatSimple", PropertyType.FLOAT);
    testSchemaClass.createProperty(session, "byteSimple", PropertyType.BYTE);
    testSchemaClass.createProperty(session, "flagSimple", PropertyType.BOOLEAN);
    testSchemaClass.createProperty(session, "dateField", PropertyType.DATETIME);

    testSchemaClass.createProperty(session, "stringListMap", PropertyType.EMBEDDEDMAP,
        PropertyType.EMBEDDEDLIST);
    testSchemaClass.createProperty(session, "enumList", PropertyType.EMBEDDEDLIST,
        PropertyType.STRING);
    testSchemaClass.createProperty(session, "enumSet", PropertyType.EMBEDDEDSET,
        PropertyType.STRING);
    testSchemaClass.createProperty(session, "stringSet", PropertyType.EMBEDDEDSET,
        PropertyType.STRING);
    testSchemaClass.createProperty(session, "stringMap", PropertyType.EMBEDDEDMAP,
        PropertyType.STRING);

    testSchemaClass.createProperty(session, "list", PropertyType.LINKLIST, childClass);
    testSchemaClass.createProperty(session, "set", PropertyType.LINKSET, childClass);
    testSchemaClass.createProperty(session, "children", PropertyType.LINKMAP, childClass);
    testSchemaClass.createProperty(session, "child", PropertyType.LINK, childClass);

    testSchemaClass.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET, childClass);
    testSchemaClass.createProperty(session, "embeddedChildren", PropertyType.EMBEDDEDMAP,
        childClass);
    testSchemaClass.createProperty(session, "embeddedChild", PropertyType.EMBEDDED, childClass);
    testSchemaClass.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST, childClass);

    // Test simple types
    checkProperty(session, testSchemaClass, "text", PropertyType.STRING);
    checkProperty(session, testSchemaClass, "enumeration", PropertyType.STRING);
    checkProperty(session, testSchemaClass, "numberSimple", PropertyType.INTEGER);
    checkProperty(session, testSchemaClass, "longSimple", PropertyType.LONG);
    checkProperty(session, testSchemaClass, "doubleSimple", PropertyType.DOUBLE);
    checkProperty(session, testSchemaClass, "floatSimple", PropertyType.FLOAT);
    checkProperty(session, testSchemaClass, "byteSimple", PropertyType.BYTE);
    checkProperty(session, testSchemaClass, "flagSimple", PropertyType.BOOLEAN);
    checkProperty(session, testSchemaClass, "dateField", PropertyType.DATETIME);

    // Test complex types
    checkProperty(session, testSchemaClass, "stringListMap",
        PropertyType.EMBEDDEDMAP, PropertyType.EMBEDDEDLIST);
    checkProperty(session, testSchemaClass, "enumList", PropertyType.EMBEDDEDLIST,
        PropertyType.STRING);
    checkProperty(session, testSchemaClass, "enumSet", PropertyType.EMBEDDEDSET,
        PropertyType.STRING);
    checkProperty(session, testSchemaClass, "stringSet", PropertyType.EMBEDDEDSET,
        PropertyType.STRING);
    checkProperty(session, testSchemaClass, "stringMap", PropertyType.EMBEDDEDMAP,
        PropertyType.STRING);

    // Test linked types
    checkProperty(session, testSchemaClass, "list", PropertyType.LINKLIST, childClass);
    checkProperty(session, testSchemaClass, "set", PropertyType.LINKSET, childClass);
    checkProperty(session, testSchemaClass, "children", PropertyType.LINKMAP, childClass);
    checkProperty(session, testSchemaClass, "child", PropertyType.LINK, childClass);

    // Test embedded types
    checkProperty(session, testSchemaClass, "embeddedSet", PropertyType.EMBEDDEDSET, childClass);
    checkProperty(session, testSchemaClass, "embeddedChildren", PropertyType.EMBEDDEDMAP,
        childClass);
    checkProperty(session, testSchemaClass, "embeddedChild", PropertyType.EMBEDDED, childClass);
    checkProperty(session, testSchemaClass, "embeddedList", PropertyType.EMBEDDEDLIST, childClass);
  }

  protected static void checkProperty(DatabaseSessionInternal session, SchemaClass iClass,
      String iPropertyName,
      PropertyType iType) {
    var prop = iClass.getProperty(session, iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(session), iType);
  }

  protected static void checkProperty(
      DatabaseSessionInternal session, SchemaClass iClass, String iPropertyName, PropertyType iType,
      SchemaClass iLinkedClass) {
    var prop = iClass.getProperty(session, iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(session), iType);
    Assert.assertEquals(prop.getLinkedClass(session), iLinkedClass);
  }

  protected static void checkProperty(
      DatabaseSessionInternal session, SchemaClass iClass, String iPropertyName, PropertyType iType,
      PropertyType iLinkedType) {
    var prop = iClass.getProperty(session, iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(session), iType);
    Assert.assertEquals(prop.getLinkedType(session), iLinkedType);
  }
}
