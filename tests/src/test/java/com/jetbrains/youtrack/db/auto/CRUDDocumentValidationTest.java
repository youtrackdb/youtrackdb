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

import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal.ATTRIBUTES_INTERNAL;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityComparator;
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
public class CRUDDocumentValidationTest extends BaseDBTest {

  private EntityImpl record;
  private EntityImpl account;

  @Parameters(value = "remote")
  public CRUDDocumentValidationTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void openDb() {
    db.begin();
    account = ((EntityImpl) db.newEntity("Account"));
    account.save();
    account.field("id", "1234567890");
    db.commit();
  }

  @Test(dependsOnMethods = "validationMandatoryNullableNoCloseDb")
  public void validationDisabledAdDatabaseLevel() throws ParseException {
    db.getMetadata().reload();
    try {
      EntityImpl entity = ((EntityImpl) db.newEntity("MyTestClass"));
      db.begin();
      entity.save();
      db.commit();
      Assert.fail();
    } catch (ValidationException ignored) {
    }

    db
        .command("ALTER DATABASE " + ATTRIBUTES_INTERNAL.VALIDATION.name() + " FALSE")
        .close();
    try {
      EntityImpl doc = ((EntityImpl) db.newEntity("MyTestClass"));
      db.begin();
      doc.save();
      db.commit();

      db.begin();
      db.bindToSession(doc).delete();
      db.commit();
    } finally {
      db.setValidationEnabled(true);
      db
          .command("ALTER DATABASE " + DatabaseSessionInternal.ATTRIBUTES_INTERNAL.VALIDATION.name()
              + " TRUE")
          .close();
    }
  }

  @Test(dependsOnMethods = "openDb", expectedExceptions = ValidationException.class)
  public void validationMandatory() {
    db.begin();
    record = db.newInstance("Whiz");
    record.clear();
    record.save();
    db.commit();
  }

  @Test(dependsOnMethods = "validationMandatory", expectedExceptions = ValidationException.class)
  public void validationMinString() {
    db.begin();
    record = db.newInstance("Whiz");
    account = db.bindToSession(account);
    record.field("account", account);
    record.field("id", 23723);
    record.field("text", "");
    record.save();
    db.commit();
  }

  @Test(
      dependsOnMethods = "validationMinString",
      expectedExceptions = ValidationException.class,
      expectedExceptionsMessageRegExp = "(?s).*more.*than.*")
  public void validationMaxString() {
    db.begin();
    record = db.newInstance("Whiz");
    account = db.bindToSession(account);
    record.field("account", account);
    record.field("id", 23723);
    record.field(
        "text",
        "clfdkkjsd hfsdkjhf fjdkghjkfdhgjdfh gfdgjfdkhgfd skdjaksdjf skdjf sdkjfsd jfkldjfkjsdf"
            + " kljdk fsdjf kldjgjdhjg khfdjgk hfjdg hjdfhgjkfhdgj kfhdjghrjg");
    record.save();
    db.commit();
  }

  @Test(
      dependsOnMethods = "validationMaxString",
      expectedExceptions = ValidationException.class,
      expectedExceptionsMessageRegExp = "(?s).*precedes.*")
  public void validationMinDate() throws ParseException {
    db.begin();
    record = db.newInstance("Whiz");
    account = db.bindToSession(account);
    record.field("account", account);
    record.field("date", new SimpleDateFormat("dd/MM/yyyy").parse("01/33/1976"));
    record.field("text", "test");
    record.save();
    db.commit();
  }

  @Test(dependsOnMethods = "validationMinDate", expectedExceptions = DatabaseException.class)
  public void validationEmbeddedType() throws ParseException {
    db.begin();
    record = db.newInstance("Whiz");
    record.field("account", db.geCurrentUser());
    record.save();
    db.commit();
  }

  @Test(
      dependsOnMethods = "validationEmbeddedType",
      expectedExceptions = ValidationException.class)
  public void validationStrictClass() throws ParseException {
    db.begin();
    EntityImpl doc = ((EntityImpl) db.newEntity("StrictTest"));
    doc.field("id", 122112);
    doc.field("antani", "122112");
    doc.save();
    db.commit();
  }

  @Test(dependsOnMethods = "validationStrictClass")
  public void closeDb() {
    db.close();
  }

  @Test(dependsOnMethods = "closeDb")
  public void createSchemaForMandatoryNullableTest() throws ParseException {
    if (db.getMetadata().getSchema().existsClass("MyTestClass")) {
      db.getMetadata().getSchema().dropClass("MyTestClass");
    }

    db.command("CREATE CLASS MyTestClass").close();
    db.command("CREATE PROPERTY MyTestClass.keyField STRING").close();
    db.command("ALTER PROPERTY MyTestClass.keyField MANDATORY true").close();
    db.command("ALTER PROPERTY MyTestClass.keyField NOTNULL true").close();
    db.command("CREATE PROPERTY MyTestClass.dateTimeField DATETIME").close();
    db.command("ALTER PROPERTY MyTestClass.dateTimeField MANDATORY true").close();
    db.command("ALTER PROPERTY MyTestClass.dateTimeField NOTNULL false").close();
    db.command("CREATE PROPERTY MyTestClass.stringField STRING").close();
    db.command("ALTER PROPERTY MyTestClass.stringField MANDATORY true").close();
    db.command("ALTER PROPERTY MyTestClass.stringField NOTNULL false").close();

    db.begin();
    db
        .command(
            "INSERT INTO MyTestClass (keyField,dateTimeField,stringField) VALUES"
                + " (\"K1\",null,null)")
        .close();
    db.commit();
    db.reload();
    db.getMetadata().reload();
    db.close();
    db = acquireSession();
    List<Result> result =
        db.query("SELECT FROM MyTestClass WHERE keyField = ?", "K1").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Result doc = result.get(0);
    Assert.assertTrue(doc.hasProperty("keyField"));
    Assert.assertTrue(doc.hasProperty("dateTimeField"));
    Assert.assertTrue(doc.hasProperty("stringField"));
  }

  @Test(dependsOnMethods = "createSchemaForMandatoryNullableTest")
  public void testUpdateDocDefined() {
    List<Result> result =
        db.query("SELECT FROM MyTestClass WHERE keyField = ?", "K1").stream().toList();
    db.begin();
    Assert.assertEquals(result.size(), 1);
    Entity readDoc = result.get(0).toEntity();
    assert readDoc != null;
    readDoc.setProperty("keyField", "K1N");
    readDoc.save();
    db.commit();
  }

  @Test(dependsOnMethods = "testUpdateDocDefined")
  public void validationMandatoryNullableCloseDb() throws ParseException {
    EntityImpl doc = ((EntityImpl) db.newEntity("MyTestClass"));
    doc.field("keyField", "K2");
    doc.field("dateTimeField", (Date) null);
    doc.field("stringField", (String) null);
    db.begin();
    doc.save();
    db.commit();

    db.close();
    db = acquireSession();

    List<Result> result =
        db.query("SELECT FROM MyTestClass WHERE keyField = ?", "K2").stream().toList();
    db.begin();
    Assert.assertEquals(result.size(), 1);
    Entity readDoc = result.get(0).toEntity();
    assert readDoc != null;
    readDoc.setProperty("keyField", "K2N");
    readDoc.save();
    db.commit();
  }

  @Test(dependsOnMethods = "validationMandatoryNullableCloseDb")
  public void validationMandatoryNullableNoCloseDb() throws ParseException {
    EntityImpl doc = ((EntityImpl) db.newEntity("MyTestClass"));
    doc.field("keyField", "K3");
    doc.field("dateTimeField", (Date) null);
    doc.field("stringField", (String) null);

    db.begin();
    doc.save();
    db.commit();

    db.begin();
    List<Result> result =
        db.query("SELECT FROM MyTestClass WHERE keyField = ?", "K3").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Entity readDoc = result.get(0).toEntity();
    assert readDoc != null;
    readDoc.setProperty("keyField", "K3N");
    readDoc.save();
    db.commit();
  }

  @Test(dependsOnMethods = "validationDisabledAdDatabaseLevel")
  public void dropSchemaForMandatoryNullableTest() throws ParseException {
    db.command("DROP CLASS MyTestClass").close();
    db.getMetadata().reload();
  }

  @Test
  public void testNullComparison() {
    // given
    EntityImpl doc1 = ((EntityImpl) db.newEntity()).field("testField", (Object) null);
    EntityImpl doc2 = ((EntityImpl) db.newEntity()).field("testField", (Object) null);

    EntityComparator comparator =
        new EntityComparator(
            Collections.singletonList(new Pair<String, String>("testField", "asc")),
            new BasicCommandContext());

    Assert.assertEquals(comparator.compare(doc1, doc2), 0);
  }
}
