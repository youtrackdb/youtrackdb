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

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.Result;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-findReferences")
public class SQLFindReferencesTest extends BaseDBTest {

  private static final String WORKPLACE = "Workplace";
  private static final String WORKER = "Worker";
  private static final String CAR = "Car";

  private RecordId carID;
  private RecordId johnDoeID;
  private RecordId janeDoeID;
  private RecordId chuckNorrisID;
  private RecordId jackBauerID;
  private RecordId ctuID;
  private RecordId fbiID;

  @Parameters(value = "remote")
  public SQLFindReferencesTest(boolean remote) {
    super(remote);
  }

  @Test
  public void findSimpleReference() {
    List<Result> result = db.command("find references " + carID).stream().toList();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.iterator().next().getProperty("referredBy"), johnDoeID);

    // SUB QUERY
    result = db.command("find references ( select from " + carID + ")").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.iterator().next().getProperty("referredBy"), johnDoeID);

    result = db.command("find references " + chuckNorrisID).stream().toList();
    Assert.assertEquals(result.size(), 2);

    for (Result rid : result) {
      Assert.assertTrue(
          rid.getProperty("referredBy").equals(ctuID)
              || rid.getProperty("referredBy").equals(fbiID));
    }

    result = db.command("find references " + johnDoeID).stream().toList();
    Assert.assertEquals(result.size(), 0);

    result = null;
  }

  @Test
  public void findReferenceByClassAndClusters() {
    List<Result> result =
        db.command("find references " + janeDoeID + " [" + WORKPLACE + "]").stream().toList();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(ctuID, result.iterator().next().getProperty("referredBy"));

    result =
        db
            .command("find references " + jackBauerID + " [" + WORKPLACE + ",cluster:" + CAR + "]")
            .stream()
            .toList();

    Assert.assertEquals(result.size(), 3);

    for (Result res : result) {
      Identifiable rid = res.getProperty("referredBy");
      Assert.assertTrue(rid.equals(ctuID) || rid.equals(fbiID) || rid.equals(carID));
    }

    result =
        db
            .command(
                "find references "
                    + johnDoeID
                    + " ["
                    + WORKPLACE
                    + ","
                    + CAR
                    + ",cluster:"
                    + WORKER
                    + "]")
            .stream()
            .toList();

    Assert.assertEquals(result.size(), 0);

    result = null;
  }

  @BeforeClass
  public void createTestEnvironment() {
    createSchema();
    populateDatabase();
  }

  private void createSchema() {
    SchemaClass worker = db.getMetadata().getSchema().createClass(WORKER);
    SchemaClass workplace = db.getMetadata().getSchema().createClass(WORKPLACE);
    SchemaClass car = db.getMetadata().getSchema().createClass(CAR);

    worker.createProperty(db, "name", PropertyType.STRING);
    worker.createProperty(db, "surname", PropertyType.STRING);
    worker.createProperty(db, "colleagues", PropertyType.LINKLIST, worker);
    worker.createProperty(db, "car", PropertyType.LINK, car);

    workplace.createProperty(db, "name", PropertyType.STRING);
    workplace.createProperty(db, "boss", PropertyType.LINK, worker);
    workplace.createProperty(db, "workers", PropertyType.LINKLIST, worker);

    car.createProperty(db, "plate", PropertyType.STRING);
    car.createProperty(db, "owner", PropertyType.LINK, worker);
  }

  private void populateDatabase() {
    db.begin();
    EntityImpl car = ((EntityImpl) db.newEntity(CAR));
    car.field("plate", "JINF223S");

    EntityImpl johnDoe = ((EntityImpl) db.newEntity(WORKER));
    johnDoe.field("name", "John");
    johnDoe.field("surname", "Doe");
    johnDoe.field("car", car);
    johnDoe.save();

    EntityImpl janeDoe = ((EntityImpl) db.newEntity(WORKER));
    janeDoe.field("name", "Jane");
    janeDoe.field("surname", "Doe");
    janeDoe.save();

    EntityImpl chuckNorris = ((EntityImpl) db.newEntity(WORKER));
    chuckNorris.field("name", "Chuck");
    chuckNorris.field("surname", "Norris");
    chuckNorris.save();

    EntityImpl jackBauer = ((EntityImpl) db.newEntity(WORKER));
    jackBauer.field("name", "Jack");
    jackBauer.field("surname", "Bauer");
    jackBauer.save();

    EntityImpl ctu = ((EntityImpl) db.newEntity(WORKPLACE));
    ctu.field("name", "CTU");
    ctu.field("boss", jackBauer);
    List<EntityImpl> workplace1Workers = new ArrayList<EntityImpl>();
    workplace1Workers.add(chuckNorris);
    workplace1Workers.add(janeDoe);
    ctu.field("workers", workplace1Workers);
    ctu.save();

    EntityImpl fbi = ((EntityImpl) db.newEntity(WORKPLACE));
    fbi.field("name", "FBI");
    fbi.field("boss", chuckNorris);
    List<EntityImpl> workplace2Workers = new ArrayList<EntityImpl>();
    workplace2Workers.add(chuckNorris);
    workplace2Workers.add(jackBauer);
    fbi.field("workers", workplace2Workers);
    fbi.save();

    car.field("owner", jackBauer);
    car.save();
    db.commit();

    chuckNorrisID = chuckNorris.getIdentity().copy();
    janeDoeID = janeDoe.getIdentity().copy();
    johnDoeID = johnDoe.getIdentity().copy();
    jackBauerID = jackBauer.getIdentity().copy();
    ctuID = ctu.getIdentity();
    fbiID = fbi.getIdentity();
    carID = car.getIdentity().copy();
  }

  @AfterClass
  public void deleteTestEnvironment() {
    db = createSessionInstance();

    carID.reset();
    carID = null;
    johnDoeID.reset();
    johnDoeID = null;
    janeDoeID.reset();
    janeDoeID = null;
    chuckNorrisID.reset();
    chuckNorrisID = null;
    jackBauerID.reset();
    jackBauerID = null;
    ctuID.reset();
    ctuID = null;
    fbiID.reset();
    fbiID = null;
    deleteSchema();

    db.close();
  }

  private void deleteSchema() {
    dropClass(CAR);
    dropClass(WORKER);
    dropClass(WORKPLACE);
  }

  private void dropClass(String iClass) {
    db.command("drop class " + iClass).close();
    while (db.getMetadata().getSchema().existsClass(iClass)) {
      db.getMetadata().getSchema().dropClass(iClass);
      db.reload();
    }
    while (db.getClusterIdByName(iClass) > -1) {
      db.dropCluster(iClass);
      db.reload();
    }
  }
}
