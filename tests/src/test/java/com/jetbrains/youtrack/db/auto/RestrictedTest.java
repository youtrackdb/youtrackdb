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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.RestrictedOperation;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class RestrictedTest extends BaseDBTest {

  private RID adminRecordId;
  private RID writerRecordId;

  private Role readerRole = null;

  @Parameters(value = "remote")
  public RestrictedTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }

  @Test
  public void testCreateRestrictedClass() {
    db = createSessionInstance();
    db
        .getMetadata()
        .getSchema()
        .createClass("CMSDocument", db.getMetadata().getSchema().getClass("ORestricted"));

    db.begin();
    var adminRecord = ((EntityImpl) db.newEntity("CMSDocument")).field("user", "admin");
    adminRecord.save();
    this.adminRecordId = adminRecord.getIdentity();
    db.commit();

    db.begin();
    readerRole = db.getMetadata().getSecurity().getRole("reader");
    db.commit();

    Assert.assertTrue(adminRecord.isUnloaded());
  }

  @Test(dependsOnMethods = "testCreateRestrictedClass")
  public void testFilteredQuery() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    ResultSet result = db.query("select from CMSDocument");
    Assert.assertEquals(result.stream().count(), 0);
    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredQuery")
  public void testCreateAsWriter() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    var writerRecord = ((EntityImpl) db.newEntity("CMSDocument")).field("user", "writer");
    writerRecord.save();
    this.writerRecordId = writerRecord.getIdentity();
    db.commit();
  }

  @Test(dependsOnMethods = "testCreateAsWriter")
  public void testFilteredQueryAsReader() throws IOException {
    db = createSessionInstance("reader", "reader");

    db.begin();
    ResultSet result = db.query("select from CMSDocument");
    Assert.assertEquals(result.stream().count(), 0);
    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredQueryAsReader")
  public void testFilteredQueryAsAdmin() throws IOException {
    db = createSessionInstance();

    db.begin();
    ResultSet result = db.query("select from CMSDocument where user = 'writer'");
    Assert.assertEquals(result.stream().count(), 1);
    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredQueryAsAdmin")
  public void testFilteredQueryAsWriter() throws IOException {
    db = createSessionInstance("writer", "writer");

    db.begin();
    ResultSet result = db.query("select from CMSDocument");
    Assert.assertEquals(result.stream().count(), 1);
    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredQueryAsWriter")
  public void testFilteredDirectReadAsWriter() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    try {
      db.load(adminRecordId.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }

    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredDirectReadAsWriter")
  public void testFilteredDirectUpdateAsWriter() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    try {
      var adminRecord = db.loadEntity(this.adminRecordId);
      adminRecord.setProperty("user", "writer-hacker");
      adminRecord.save();
      db.commit();
    } catch (SecurityException | RecordNotFoundException e) {
      // OK AS EXCEPTION
    }
    db.close();

    db = createSessionInstance();
    db.begin();
    var adminRecord = db.<EntityImpl>load(this.adminRecordId);
    Assert.assertEquals(adminRecord.field("user"), "admin");
    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredDirectUpdateAsWriter")
  public void testFilteredDirectDeleteAsWriter() throws IOException {
    db = createSessionInstance("writer", "writer");
    try {
      db.begin();
      db.delete(adminRecordId);
      db.commit();
    } catch (SecurityException | RecordNotFoundException e) {
      // OK AS EXCEPTION
    }
    db.close();

    db = createSessionInstance();
    db.begin();
    var adminRecord = db.<EntityImpl>load(this.adminRecordId);
    Assert.assertEquals(adminRecord.field("user"), "admin");
    db.commit();
  }

  @Test(dependsOnMethods = "testFilteredDirectDeleteAsWriter")
  public void testFilteredHackingAllowFieldAsWriter() throws IOException {
    db = createSessionInstance("writer", "writer");
    try {
      db.begin();
      // FORCE LOADING
      EntityImpl adminRecord = db.load(this.adminRecordId);
      Set<Identifiable> allows = adminRecord.field(SecurityShared.ALLOW_ALL_FIELD);
      allows.add(
          db.getMetadata().getSecurity().getUser(db.geCurrentUser().getName(db))
              .getIdentity());
      adminRecord.save();
      db.commit();
    } catch (SecurityException | RecordNotFoundException e) {
      // OK AS EXCEPTION
    }
    db.close();

    db = createSessionInstance();
  }

  @Test(dependsOnMethods = "testFilteredHackingAllowFieldAsWriter")
  public void testAddReaderAsRole() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    var writerRecord = db.<EntityImpl>load(this.writerRecordId);
    Set<Identifiable> allows = writerRecord.field(SecurityShared.ALLOW_ALL_FIELD);
    allows.add(readerRole.getIdentity());

    writerRecord.save();
    db.commit();
  }

  @Test(dependsOnMethods = "testAddReaderAsRole")
  public void testReaderCanSeeWriterDocumentAfterPermission() throws IOException {
    db = createSessionInstance("reader", "reader");
    db.begin();
    Assert.assertNotNull(db.load(writerRecordId.getIdentity()));
    db.commit();
  }

  @Test(dependsOnMethods = "testReaderCanSeeWriterDocumentAfterPermission")
  public void testWriterRoleCanRemoveReader() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    EntityImpl writerRecord = db.load(this.writerRecordId);
    Assert.assertEquals(
        ((Collection<?>) writerRecord.field(RestrictedOperation.ALLOW_ALL.getFieldName())).size(),
        2);
    db
        .getMetadata()
        .getSecurity()
        .denyRole(writerRecord, RestrictedOperation.ALLOW_ALL, "reader");
    Assert.assertEquals(
        ((Collection<?>) writerRecord.field(RestrictedOperation.ALLOW_ALL.getFieldName())).size(),
        1);
    writerRecord.save();
    db.commit();
  }

  @Test(dependsOnMethods = "testWriterRoleCanRemoveReader")
  public void testReaderCannotSeeWriterDocument() throws IOException {
    db = createSessionInstance("reader", "reader");
    db.begin();
    try {
      db.load(writerRecordId.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
    db.commit();
  }

  @Test(dependsOnMethods = "testReaderCannotSeeWriterDocument")
  public void testWriterAddReaderUserOnlyForRead() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    EntityImpl writerRecord = db.load(this.writerRecordId);
    db
        .getMetadata()
        .getSecurity()
        .allowUser(writerRecord, RestrictedOperation.ALLOW_READ, "reader");
    writerRecord.save();
    db.commit();
  }

  @Test(dependsOnMethods = "testWriterAddReaderUserOnlyForRead")
  public void testReaderCanSeeWriterDocument() throws IOException {
    db = createSessionInstance("reader", "reader");
    db.begin();
    Assert.assertNotNull(db.load(writerRecordId.getIdentity()));
    db.commit();
  }

  /**
   * *** TESTS FOR #1980: Record Level Security: permissions don't follow role's inheritance ****
   */
  @Test(dependsOnMethods = "testReaderCanSeeWriterDocument")
  public void testWriterRemoveReaderUserOnlyForRead() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    EntityImpl writerRecord = db.load(this.writerRecordId);
    db
        .getMetadata()
        .getSecurity()
        .denyUser(writerRecord, RestrictedOperation.ALLOW_READ, "reader");
    writerRecord.save();
    db.commit();
  }

  @Test(dependsOnMethods = "testWriterRemoveReaderUserOnlyForRead")
  public void testReaderCannotSeeWriterDocumentAgain() throws IOException {
    db = createSessionInstance("reader", "reader");
    db.begin();
    try {
      db.load(writerRecordId.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
    db.commit();
  }

  @Test(dependsOnMethods = "testReaderCannotSeeWriterDocumentAgain")
  public void testReaderRoleInheritsFromWriterRole() throws IOException {
    db = createSessionInstance();
    db.begin();
    Role reader = db.getMetadata().getSecurity().getRole("reader");
    reader.setParentRole(db, db.getMetadata().getSecurity().getRole("writer"));

    reader.save(db);
    db.commit();
  }

  @Test(dependsOnMethods = "testReaderRoleInheritsFromWriterRole")
  public void testWriterRoleCanSeeWriterDocument() throws IOException {
    db = createSessionInstance("writer", "writer");
    db.begin();
    EntityImpl writerRecord = db.load(this.writerRecordId);
    db
        .getMetadata()
        .getSecurity()
        .allowRole(writerRecord, RestrictedOperation.ALLOW_READ, "writer");
    writerRecord.save();
    db.commit();
  }

  @Test(dependsOnMethods = "testWriterRoleCanSeeWriterDocument")
  public void testReaderRoleCanSeeInheritedDocument() {
    db = createSessionInstance("reader", "reader");

    db.begin();
    Assert.assertNotNull(db.load(writerRecordId.getIdentity()));
    db.commit();
  }

  @Test(dependsOnMethods = "testReaderRoleCanSeeInheritedDocument")
  public void testReaderRoleDesntInheritsFromWriterRole() throws IOException {
    db = createSessionInstance();
    db.begin();
    Role reader = db.getMetadata().getSecurity().getRole("reader");
    reader.setParentRole(db, null);
    reader.save(db);
    db.commit();
  }

  /**
   * ** END TEST FOR #1980: Record Level Security: permissions don't follow role's inheritance ***
   */
  @Test(dependsOnMethods = "testReaderRoleDesntInheritsFromWriterRole")
  public void testTruncateClass() {
    db = createSessionInstance();
    try {
      db.command("truncate class CMSDocument").close();
      Assert.fail();
    } catch (SecurityException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "testTruncateClass")
  public void testTruncateUnderlyingCluster() {
    db = createSessionInstance();
    try {
      db.command("truncate cluster CMSDocument").close();
    } catch (SecurityException e) {

    }
  }

  @Test(dependsOnMethods = "testTruncateUnderlyingCluster")
  public void testUpdateRestricted() {
    db = createSessionInstance();
    db
        .getMetadata()
        .getSchema()
        .createClass(
            "TestUpdateRestricted", db.getMetadata().getSchema().getClass("ORestricted"));

    db.begin();
    var adminRecord = ((EntityImpl) db.newEntity("TestUpdateRestricted")).field("user", "admin");
    adminRecord.save();
    this.adminRecordId = adminRecord.getIdentity();
    db.commit();

    db.close();

    db = createSessionInstance("writer", "writer");
    db.begin();
    ResultSet result = db.query("select from TestUpdateRestricted");
    Assert.assertEquals(result.stream().count(), 0);
    db.commit();

    db.close();

    db = createSessionInstance();
    db.begin();
    db
        .command("update TestUpdateRestricted content {\"data\":\"My Test\"}").close();
    db.commit();

    db.begin();
    result = db.query("select from TestUpdateRestricted");
    Result res = result.next();
    Assert.assertFalse(result.hasNext());

    final Entity doc = res.getEntity().get();
    Assert.assertEquals(doc.getProperty("data"), "My Test");
    doc.setProperty("user", "admin");

    db.save(doc);
    db.commit();

    db.close();

    db = createSessionInstance("writer", "writer");
    db.begin();
    result = db.query("select from TestUpdateRestricted");
    Assert.assertEquals(result.stream().count(), 0);
    db.commit();
  }

  @BeforeMethod
  protected void closeDb() {
    db.close();
  }
}
