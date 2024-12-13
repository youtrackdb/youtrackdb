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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal.ATTRIBUTES_INTERNAL;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentComparator;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CRUDDocumentValidationTest extends DocumentDBBaseTest {

  private EntityImpl record;
  private EntityImpl account;

  @Parameters(value = "remote")
  public CRUDDocumentValidationTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void openDb() {
    database.begin();
    account = new EntityImpl("Account");
    account.save();
    account.field("id", "1234567890");
    database.commit();
  }

  @Test(dependsOnMethods = "validationMandatoryNullableNoCloseDb")
  public void validationDisabledAdDatabaseLevel() throws ParseException {
    database.getMetadata().reload();
    try {
      EntityImpl entity = new EntityImpl("MyTestClass");
      database.begin();
      entity.save();
      database.commit();
      Assert.fail();
    } catch (ValidationException ignored) {
    }

    database
        .command("ALTER DATABASE " + ATTRIBUTES_INTERNAL.VALIDATION.name() + " FALSE")
        .close();
    try {
      EntityImpl doc = new EntityImpl("MyTestClass");
      database.begin();
      doc.save();
      database.commit();

      database.begin();
      database.bindToSession(doc).delete();
      database.commit();
    } finally {
      database.setValidationEnabled(true);
      database
          .command("ALTER DATABASE " + DatabaseSessionInternal.ATTRIBUTES_INTERNAL.VALIDATION.name()
              + " TRUE")
          .close();
    }
  }

  @Test(dependsOnMethods = "openDb", expectedExceptions = ValidationException.class)
  public void validationMandatory() {
    database.begin();
    record = database.newInstance("Whiz");
    record.clear();
    record.save();
    database.commit();
  }

  @Test(dependsOnMethods = "validationMandatory", expectedExceptions = ValidationException.class)
  public void validationMinString() {
    database.begin();
    record = database.newInstance("Whiz");
    account = database.bindToSession(account);
    record.field("account", account);
    record.field("id", 23723);
    record.field("text", "");
    record.save();
    database.commit();
  }

  @Test(
      dependsOnMethods = "validationMinString",
      expectedExceptions = ValidationException.class,
      expectedExceptionsMessageRegExp = "(?s).*more.*than.*")
  public void validationMaxString() {
    database.begin();
    record = database.newInstance("Whiz");
    account = database.bindToSession(account);
    record.field("account", account);
    record.field("id", 23723);
    record.field(
        "text",
        "clfdkkjsd hfsdkjhf fjdkghjkfdhgjdfh gfdgjfdkhgfd skdjaksdjf skdjf sdkjfsd jfkldjfkjsdf"
            + " kljdk fsdjf kldjgjdhjg khfdjgk hfjdg hjdfhgjkfhdgj kfhdjghrjg");
    record.save();
    database.commit();
  }

  @Test(
      dependsOnMethods = "validationMaxString",
      expectedExceptions = ValidationException.class,
      expectedExceptionsMessageRegExp = "(?s).*precedes.*")
  public void validationMinDate() throws ParseException {
    database.begin();
    record = database.newInstance("Whiz");
    account = database.bindToSession(account);
    record.field("account", account);
    record.field("date", new SimpleDateFormat("dd/MM/yyyy").parse("01/33/1976"));
    record.field("text", "test");
    record.save();
    database.commit();
  }

  @Test(dependsOnMethods = "validationMinDate", expectedExceptions = DatabaseException.class)
  public void validationEmbeddedType() throws ParseException {
    database.begin();
    record = database.newInstance("Whiz");
    record.field("account", database.geCurrentUser());
    record.save();
    database.commit();
  }

  @Test(
      dependsOnMethods = "validationEmbeddedType",
      expectedExceptions = ValidationException.class)
  public void validationStrictClass() throws ParseException {
    database.begin();
    EntityImpl doc = new EntityImpl("StrictTest");
    doc.field("id", 122112);
    doc.field("antani", "122112");
    doc.save();
    database.commit();
  }

  @Test(dependsOnMethods = "validationStrictClass")
  public void closeDb() {
    database.close();
  }

  @Test(dependsOnMethods = "closeDb")
  public void createSchemaForMandatoryNullableTest() throws ParseException {
    if (database.getMetadata().getSchema().existsClass("MyTestClass")) {
      database.getMetadata().getSchema().dropClass("MyTestClass");
    }

    database.command("CREATE CLASS MyTestClass").close();
    database.command("CREATE PROPERTY MyTestClass.keyField STRING").close();
    database.command("ALTER PROPERTY MyTestClass.keyField MANDATORY true").close();
    database.command("ALTER PROPERTY MyTestClass.keyField NOTNULL true").close();
    database.command("CREATE PROPERTY MyTestClass.dateTimeField DATETIME").close();
    database.command("ALTER PROPERTY MyTestClass.dateTimeField MANDATORY true").close();
    database.command("ALTER PROPERTY MyTestClass.dateTimeField NOTNULL false").close();
    database.command("CREATE PROPERTY MyTestClass.stringField STRING").close();
    database.command("ALTER PROPERTY MyTestClass.stringField MANDATORY true").close();
    database.command("ALTER PROPERTY MyTestClass.stringField NOTNULL false").close();

    database.begin();
    database
        .command(
            "INSERT INTO MyTestClass (keyField,dateTimeField,stringField) VALUES"
                + " (\"K1\",null,null)")
        .close();
    database.commit();
    database.reload();
    database.getMetadata().reload();
    database.close();
    database = acquireSession();
    List<Result> result =
        database.query("SELECT FROM MyTestClass WHERE keyField = ?", "K1").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Result doc = result.get(0);
    Assert.assertTrue(doc.hasProperty("keyField"));
    Assert.assertTrue(doc.hasProperty("dateTimeField"));
    Assert.assertTrue(doc.hasProperty("stringField"));
  }

  @Test(dependsOnMethods = "createSchemaForMandatoryNullableTest")
  public void testUpdateDocDefined() {
    List<Result> result =
        database.query("SELECT FROM MyTestClass WHERE keyField = ?", "K1").stream().toList();
    database.begin();
    Assert.assertEquals(result.size(), 1);
    Entity readDoc = result.get(0).toEntity();
    assert readDoc != null;
    readDoc.setProperty("keyField", "K1N");
    readDoc.save();
    database.commit();
  }

  @Test(dependsOnMethods = "testUpdateDocDefined")
  public void validationMandatoryNullableCloseDb() throws ParseException {
    EntityImpl doc = new EntityImpl("MyTestClass");
    doc.field("keyField", "K2");
    doc.field("dateTimeField", (Date) null);
    doc.field("stringField", (String) null);
    database.begin();
    doc.save();
    database.commit();

    database.close();
    database = acquireSession();

    List<Result> result =
        database.query("SELECT FROM MyTestClass WHERE keyField = ?", "K2").stream().toList();
    database.begin();
    Assert.assertEquals(result.size(), 1);
    Entity readDoc = result.get(0).toEntity();
    assert readDoc != null;
    readDoc.setProperty("keyField", "K2N");
    readDoc.save();
    database.commit();
  }

  @Test(dependsOnMethods = "validationMandatoryNullableCloseDb")
  public void validationMandatoryNullableNoCloseDb() throws ParseException {
    EntityImpl doc = new EntityImpl("MyTestClass");
    doc.field("keyField", "K3");
    doc.field("dateTimeField", (Date) null);
    doc.field("stringField", (String) null);

    database.begin();
    doc.save();
    database.commit();

    database.begin();
    List<Result> result =
        database.query("SELECT FROM MyTestClass WHERE keyField = ?", "K3").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Entity readDoc = result.get(0).toEntity();
    assert readDoc != null;
    readDoc.setProperty("keyField", "K3N");
    readDoc.save();
    database.commit();
  }

  @Test(dependsOnMethods = "validationDisabledAdDatabaseLevel")
  public void dropSchemaForMandatoryNullableTest() throws ParseException {
    database.command("DROP CLASS MyTestClass").close();
    database.getMetadata().reload();
  }

  @Test
  public void testNullComparison() {
    // given
    EntityImpl doc1 = new EntityImpl().field("testField", (Object) null);
    EntityImpl doc2 = new EntityImpl().field("testField", (Object) null);

    DocumentComparator comparator =
        new DocumentComparator(
            Collections.singletonList(new Pair<String, String>("testField", "asc")),
            new BasicCommandContext());

    Assert.assertEquals(comparator.compare(doc1, doc2), 0);
  }
}
