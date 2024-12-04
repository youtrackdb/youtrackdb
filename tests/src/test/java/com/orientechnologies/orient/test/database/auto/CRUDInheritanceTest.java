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

import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
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
    final List<YTDocument> result = executeQuery("select from Company where name.length() > 0");

    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.size(), TOT_COMPANY_RECORDS);

    int companyRecords = 0;
    YTDocument account;
    for (YTDocument entries : result) {
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
    final List<YTDocument> result = executeQuery("select * from Company where name.length() > 0");

    Assert.assertEquals(result.size(), TOT_COMPANY_RECORDS);

    YTEntity account;
    for (YTDocument entries : result) {
      account = entries;
      Assert.assertNotSame(account.<String>getProperty("name").length(), 0);
    }
  }

  @Test(dependsOnMethods = {"queryPerSuperType", "testCreate"})
  public void deleteFirst() {
    // DELETE ALL THE RECORD IN THE CLUSTER
    database.begin();
    var companyClusterIterator = database.browseClass("Company");
    for (YTEntity obj : companyClusterIterator) {
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

    YTClass abstractClass =
        database.getMetadata().getSchema().getClass("InheritanceTestAbstractClass");
    YTClass baseClass = database.getMetadata().getSchema().getClass("InheritanceTestBaseClass");
    YTClass testClass = database.getMetadata().getSchema().getClass("InheritanceTestClass");

    Assert.assertTrue(baseClass.getSuperClasses().contains(abstractClass));
    Assert.assertTrue(testClass.getSuperClasses().contains(baseClass));
  }

  @Test
  public void testIdFieldInheritanceFirstSubClass() {
    createInheritanceTestClass();

    YTEntity a = database.newInstance("InheritanceTestBaseClass");
    YTEntity b = database.newInstance("InheritanceTestClass");

    database.begin();
    database.save(a);
    database.save(b);
    database.commit();

    final List<YTDocument> result1 = executeQuery("select from InheritanceTestBaseClass");
    Assert.assertEquals(2, result1.size());
  }

  @Test
  public void testKeywordClass() {
    YTClass klass = database.getMetadata().getSchema().createClass("Not");

    YTClass klass1 = database.getMetadata().getSchema().createClass("Extends_Not", klass);
    Assert.assertEquals(1, klass1.getSuperClasses().size(), 1);
    Assert.assertEquals("Not", klass1.getSuperClasses().get(0).getName());
  }

  @Test
  public void testSchemaGeneration() {
    var schema = database.getMetadata().getSchema();
    YTClass testSchemaClass = schema.createClass("JavaTestSchemaGeneration");
    YTClass childClass = schema.createClass("TestSchemaGenerationChild");

    testSchemaClass.createProperty(database, "text", YTType.STRING);
    testSchemaClass.createProperty(database, "enumeration", YTType.STRING);
    testSchemaClass.createProperty(database, "numberSimple", YTType.INTEGER);
    testSchemaClass.createProperty(database, "longSimple", YTType.LONG);
    testSchemaClass.createProperty(database, "doubleSimple", YTType.DOUBLE);
    testSchemaClass.createProperty(database, "floatSimple", YTType.FLOAT);
    testSchemaClass.createProperty(database, "byteSimple", YTType.BYTE);
    testSchemaClass.createProperty(database, "flagSimple", YTType.BOOLEAN);
    testSchemaClass.createProperty(database, "dateField", YTType.DATETIME);

    testSchemaClass.createProperty(database, "stringListMap", YTType.EMBEDDEDMAP,
        YTType.EMBEDDEDLIST);
    testSchemaClass.createProperty(database, "enumList", YTType.EMBEDDEDLIST, YTType.STRING);
    testSchemaClass.createProperty(database, "enumSet", YTType.EMBEDDEDSET, YTType.STRING);
    testSchemaClass.createProperty(database, "stringSet", YTType.EMBEDDEDSET, YTType.STRING);
    testSchemaClass.createProperty(database, "stringMap", YTType.EMBEDDEDMAP, YTType.STRING);

    testSchemaClass.createProperty(database, "list", YTType.LINKLIST, childClass);
    testSchemaClass.createProperty(database, "set", YTType.LINKSET, childClass);
    testSchemaClass.createProperty(database, "children", YTType.LINKMAP, childClass);
    testSchemaClass.createProperty(database, "child", YTType.LINK, childClass);

    testSchemaClass.createProperty(database, "embeddedSet", YTType.EMBEDDEDSET, childClass);
    testSchemaClass.createProperty(database, "embeddedChildren", YTType.EMBEDDEDMAP, childClass);
    testSchemaClass.createProperty(database, "embeddedChild", YTType.EMBEDDED, childClass);
    testSchemaClass.createProperty(database, "embeddedList", YTType.EMBEDDEDLIST, childClass);

    // Test simple types
    checkProperty(testSchemaClass, "text", YTType.STRING);
    checkProperty(testSchemaClass, "enumeration", YTType.STRING);
    checkProperty(testSchemaClass, "numberSimple", YTType.INTEGER);
    checkProperty(testSchemaClass, "longSimple", YTType.LONG);
    checkProperty(testSchemaClass, "doubleSimple", YTType.DOUBLE);
    checkProperty(testSchemaClass, "floatSimple", YTType.FLOAT);
    checkProperty(testSchemaClass, "byteSimple", YTType.BYTE);
    checkProperty(testSchemaClass, "flagSimple", YTType.BOOLEAN);
    checkProperty(testSchemaClass, "dateField", YTType.DATETIME);

    // Test complex types
    checkProperty(testSchemaClass, "stringListMap", YTType.EMBEDDEDMAP, YTType.EMBEDDEDLIST);
    checkProperty(testSchemaClass, "enumList", YTType.EMBEDDEDLIST, YTType.STRING);
    checkProperty(testSchemaClass, "enumSet", YTType.EMBEDDEDSET, YTType.STRING);
    checkProperty(testSchemaClass, "stringSet", YTType.EMBEDDEDSET, YTType.STRING);
    checkProperty(testSchemaClass, "stringMap", YTType.EMBEDDEDMAP, YTType.STRING);

    // Test linked types
    checkProperty(testSchemaClass, "list", YTType.LINKLIST, childClass);
    checkProperty(testSchemaClass, "set", YTType.LINKSET, childClass);
    checkProperty(testSchemaClass, "children", YTType.LINKMAP, childClass);
    checkProperty(testSchemaClass, "child", YTType.LINK, childClass);

    // Test embedded types
    checkProperty(testSchemaClass, "embeddedSet", YTType.EMBEDDEDSET, childClass);
    checkProperty(testSchemaClass, "embeddedChildren", YTType.EMBEDDEDMAP, childClass);
    checkProperty(testSchemaClass, "embeddedChild", YTType.EMBEDDED, childClass);
    checkProperty(testSchemaClass, "embeddedList", YTType.EMBEDDEDLIST, childClass);
  }

  protected void checkProperty(YTClass iClass, String iPropertyName, YTType iType) {
    YTProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
  }

  protected void checkProperty(
      YTClass iClass, String iPropertyName, YTType iType, YTClass iLinkedClass) {
    YTProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
    Assert.assertEquals(prop.getLinkedClass(), iLinkedClass);
  }

  protected void checkProperty(
      YTClass iClass, String iPropertyName, YTType iType, YTType iLinkedType) {
    YTProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
    Assert.assertEquals(prop.getLinkedType(), iLinkedType);
  }
}
