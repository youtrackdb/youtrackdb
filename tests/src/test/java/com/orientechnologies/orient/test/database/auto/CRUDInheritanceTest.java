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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CRUDInheritanceTest extends DocumentDBBaseTest {

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
    database.command("delete from Company").close();

    generateCompanyData();
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    Assert.assertEquals(database.countClass("Company"), TOT_COMPANY_RECORDS);
  }

  @Test(dependsOnMethods = "testCreate")
  public void queryByBaseType() {
    final List<ODocument> result = executeQuery("select from Company where name.length() > 0");

    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.size(), TOT_COMPANY_RECORDS);

    int companyRecords = 0;
    ODocument account;
    for (ODocument entries : result) {
      account = entries;

      if ("Company".equals(account.getClassName())) {
        companyRecords++;
      }

      Assert.assertNotSame(account.<String>getProperty("name").length(), 0);
    }

    Assert.assertEquals(companyRecords, TOT_COMPANY_RECORDS);
  }

  @Test(dependsOnMethods = "queryByBaseType")
  public void queryPerSuperType() {
    final List<ODocument> result = executeQuery("select * from Company where name.length() > 0");

    Assert.assertEquals(result.size(), TOT_COMPANY_RECORDS);

    OElement account;
    for (ODocument entries : result) {
      account = entries;
      Assert.assertNotSame(account.<String>getProperty("name").length(), 0);
    }
  }

  @Test(dependsOnMethods = {"queryPerSuperType", "testCreate"})
  public void deleteFirst() {
    // DELETE ALL THE RECORD IN THE CLUSTER
    database.begin();
    var companyClusterIterator = database.browseClass("Company");
    for (OElement obj : companyClusterIterator) {
      if (obj.<Integer>getProperty("id") == 1) {
        database.delete(obj);
        break;
      }
    }
    database.commit();

    Assert.assertEquals(database.countClass("Company"), TOT_COMPANY_RECORDS - 1);
  }

  @Test(dependsOnMethods = "deleteFirst")
  public void testSuperclassInheritanceCreation() {
    database.close();
    database = createSessionInstance();

    createInheritanceTestClass();

    OClass abstractClass =
        database.getMetadata().getSchema().getClass("InheritanceTestAbstractClass");
    OClass baseClass = database.getMetadata().getSchema().getClass("InheritanceTestBaseClass");
    OClass testClass = database.getMetadata().getSchema().getClass("InheritanceTestClass");

    Assert.assertTrue(baseClass.getSuperClasses().contains(abstractClass));
    Assert.assertTrue(testClass.getSuperClasses().contains(baseClass));
  }

  @Test
  public void testIdFieldInheritanceFirstSubClass() {
    createInheritanceTestClass();

    OElement a = database.newInstance("InheritanceTestBaseClass");
    OElement b = database.newInstance("InheritanceTestClass");

    database.begin();
    database.save(a);
    database.save(b);
    database.commit();

    final List<ODocument> result1 = executeQuery("select from InheritanceTestBaseClass");
    Assert.assertEquals(2, result1.size());
  }

  @Test
  public void testKeywordClass() {
    OClass klass = database.getMetadata().getSchema().createClass("Not");

    OClass klass1 = database.getMetadata().getSchema().createClass("Extends_Not", klass);
    Assert.assertEquals(1, klass1.getSuperClasses().size(), 1);
    Assert.assertEquals("Not", klass1.getSuperClasses().get(0).getName());
  }

  @Test
  public void testSchemaGeneration() {
    var schema = database.getMetadata().getSchema();
    OClass testSchemaClass = schema.createClass("JavaTestSchemaGeneration");
    OClass childClass = schema.createClass("TestSchemaGenerationChild");

    testSchemaClass.createProperty(database, "text", OType.STRING);
    testSchemaClass.createProperty(database, "enumeration", OType.STRING);
    testSchemaClass.createProperty(database, "numberSimple", OType.INTEGER);
    testSchemaClass.createProperty(database, "longSimple", OType.LONG);
    testSchemaClass.createProperty(database, "doubleSimple", OType.DOUBLE);
    testSchemaClass.createProperty(database, "floatSimple", OType.FLOAT);
    testSchemaClass.createProperty(database, "byteSimple", OType.BYTE);
    testSchemaClass.createProperty(database, "flagSimple", OType.BOOLEAN);
    testSchemaClass.createProperty(database, "dateField", OType.DATETIME);

    testSchemaClass.createProperty(database, "stringListMap", OType.EMBEDDEDMAP,
        OType.EMBEDDEDLIST);
    testSchemaClass.createProperty(database, "enumList", OType.EMBEDDEDLIST, OType.STRING);
    testSchemaClass.createProperty(database, "enumSet", OType.EMBEDDEDSET, OType.STRING);
    testSchemaClass.createProperty(database, "stringSet", OType.EMBEDDEDSET, OType.STRING);
    testSchemaClass.createProperty(database, "stringMap", OType.EMBEDDEDMAP, OType.STRING);

    testSchemaClass.createProperty(database, "list", OType.LINKLIST, childClass);
    testSchemaClass.createProperty(database, "set", OType.LINKSET, childClass);
    testSchemaClass.createProperty(database, "children", OType.LINKMAP, childClass);
    testSchemaClass.createProperty(database, "child", OType.LINK, childClass);

    testSchemaClass.createProperty(database, "embeddedSet", OType.EMBEDDEDSET, childClass);
    testSchemaClass.createProperty(database, "embeddedChildren", OType.EMBEDDEDMAP, childClass);
    testSchemaClass.createProperty(database, "embeddedChild", OType.EMBEDDED, childClass);
    testSchemaClass.createProperty(database, "embeddedList", OType.EMBEDDEDLIST, childClass);

    // Test simple types
    checkProperty(testSchemaClass, "text", OType.STRING);
    checkProperty(testSchemaClass, "enumeration", OType.STRING);
    checkProperty(testSchemaClass, "numberSimple", OType.INTEGER);
    checkProperty(testSchemaClass, "longSimple", OType.LONG);
    checkProperty(testSchemaClass, "doubleSimple", OType.DOUBLE);
    checkProperty(testSchemaClass, "floatSimple", OType.FLOAT);
    checkProperty(testSchemaClass, "byteSimple", OType.BYTE);
    checkProperty(testSchemaClass, "flagSimple", OType.BOOLEAN);
    checkProperty(testSchemaClass, "dateField", OType.DATETIME);

    // Test complex types
    checkProperty(testSchemaClass, "stringListMap", OType.EMBEDDEDMAP, OType.EMBEDDEDLIST);
    checkProperty(testSchemaClass, "enumList", OType.EMBEDDEDLIST, OType.STRING);
    checkProperty(testSchemaClass, "enumSet", OType.EMBEDDEDSET, OType.STRING);
    checkProperty(testSchemaClass, "stringSet", OType.EMBEDDEDSET, OType.STRING);
    checkProperty(testSchemaClass, "stringMap", OType.EMBEDDEDMAP, OType.STRING);

    // Test linked types
    checkProperty(testSchemaClass, "list", OType.LINKLIST, childClass);
    checkProperty(testSchemaClass, "set", OType.LINKSET, childClass);
    checkProperty(testSchemaClass, "children", OType.LINKMAP, childClass);
    checkProperty(testSchemaClass, "child", OType.LINK, childClass);

    // Test embedded types
    checkProperty(testSchemaClass, "embeddedSet", OType.EMBEDDEDSET, childClass);
    checkProperty(testSchemaClass, "embeddedChildren", OType.EMBEDDEDMAP, childClass);
    checkProperty(testSchemaClass, "embeddedChild", OType.EMBEDDED, childClass);
    checkProperty(testSchemaClass, "embeddedList", OType.EMBEDDEDLIST, childClass);
  }

  protected void checkProperty(OClass iClass, String iPropertyName, OType iType) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
  }

  protected void checkProperty(
      OClass iClass, String iPropertyName, OType iType, OClass iLinkedClass) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
    Assert.assertEquals(prop.getLinkedClass(), iLinkedClass);
  }

  protected void checkProperty(
      OClass iClass, String iPropertyName, OType iType, OType iLinkedType) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
    Assert.assertEquals(prop.getLinkedType(), iLinkedType);
  }
}
