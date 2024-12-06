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

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-findReferences")
public class SQLFindReferencesTest extends DocumentDBBaseTest {

  private static final String WORKPLACE = "Workplace";
  private static final String WORKER = "Worker";
  private static final String CAR = "Car";

  private RID carID;
  private RID johnDoeID;
  private RID janeDoeID;
  private RID chuckNorrisID;
  private RID jackBauerID;
  private RID ctuID;
  private RID fbiID;

  @Parameters(value = "remote")
  public SQLFindReferencesTest(boolean remote) {
    super(remote);
  }

  @Test
  public void findSimpleReference() {
    List<Result> result = database.command("find references " + carID).stream().toList();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.iterator().next().getProperty("referredBy"), johnDoeID);

    // SUB QUERY
    result = database.command("find references ( select from " + carID + ")").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.iterator().next().getProperty("referredBy"), johnDoeID);

    result = database.command("find references " + chuckNorrisID).stream().toList();
    Assert.assertEquals(result.size(), 2);

    for (Result rid : result) {
      Assert.assertTrue(
          rid.getProperty("referredBy").equals(ctuID)
              || rid.getProperty("referredBy").equals(fbiID));
    }

    result = database.command("find references " + johnDoeID).stream().toList();
    Assert.assertEquals(result.size(), 0);

    result = null;
  }

  @Test
  public void findReferenceByClassAndClusters() {
    List<Result> result =
        database.command("find references " + janeDoeID + " [" + WORKPLACE + "]").stream().toList();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(ctuID, result.iterator().next().getProperty("referredBy"));

    result =
        database
            .command("find references " + jackBauerID + " [" + WORKPLACE + ",cluster:" + CAR + "]")
            .stream()
            .toList();

    Assert.assertEquals(result.size(), 3);

    for (Result res : result) {
      Identifiable rid = res.getProperty("referredBy");
      Assert.assertTrue(rid.equals(ctuID) || rid.equals(fbiID) || rid.equals(carID));
    }

    result =
        database
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
    SchemaClass worker = database.getMetadata().getSchema().createClass(WORKER);
    SchemaClass workplace = database.getMetadata().getSchema().createClass(WORKPLACE);
    SchemaClass car = database.getMetadata().getSchema().createClass(CAR);

    worker.createProperty(database, "name", PropertyType.STRING);
    worker.createProperty(database, "surname", PropertyType.STRING);
    worker.createProperty(database, "colleagues", PropertyType.LINKLIST, worker);
    worker.createProperty(database, "car", PropertyType.LINK, car);

    workplace.createProperty(database, "name", PropertyType.STRING);
    workplace.createProperty(database, "boss", PropertyType.LINK, worker);
    workplace.createProperty(database, "workers", PropertyType.LINKLIST, worker);

    car.createProperty(database, "plate", PropertyType.STRING);
    car.createProperty(database, "owner", PropertyType.LINK, worker);
  }

  private void populateDatabase() {
    database.begin();
    EntityImpl car = new EntityImpl(CAR);
    car.field("plate", "JINF223S");

    EntityImpl johnDoe = new EntityImpl(WORKER);
    johnDoe.field("name", "John");
    johnDoe.field("surname", "Doe");
    johnDoe.field("car", car);
    johnDoe.save();

    EntityImpl janeDoe = new EntityImpl(WORKER);
    janeDoe.field("name", "Jane");
    janeDoe.field("surname", "Doe");
    janeDoe.save();

    EntityImpl chuckNorris = new EntityImpl(WORKER);
    chuckNorris.field("name", "Chuck");
    chuckNorris.field("surname", "Norris");
    chuckNorris.save();

    EntityImpl jackBauer = new EntityImpl(WORKER);
    jackBauer.field("name", "Jack");
    jackBauer.field("surname", "Bauer");
    jackBauer.save();

    EntityImpl ctu = new EntityImpl(WORKPLACE);
    ctu.field("name", "CTU");
    ctu.field("boss", jackBauer);
    List<EntityImpl> workplace1Workers = new ArrayList<EntityImpl>();
    workplace1Workers.add(chuckNorris);
    workplace1Workers.add(janeDoe);
    ctu.field("workers", workplace1Workers);
    ctu.save();

    EntityImpl fbi = new EntityImpl(WORKPLACE);
    fbi.field("name", "FBI");
    fbi.field("boss", chuckNorris);
    List<EntityImpl> workplace2Workers = new ArrayList<EntityImpl>();
    workplace2Workers.add(chuckNorris);
    workplace2Workers.add(jackBauer);
    fbi.field("workers", workplace2Workers);
    fbi.save();

    car.field("owner", jackBauer);
    car.save();
    database.commit();

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
    database = createSessionInstance();

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

    database.close();
  }

  private void deleteSchema() {
    dropClass(CAR);
    dropClass(WORKER);
    dropClass(WORKPLACE);
  }

  private void dropClass(String iClass) {
    database.command("drop class " + iClass).close();
    while (database.getMetadata().getSchema().existsClass(iClass)) {
      database.getMetadata().getSchema().dropClass(iClass);
      database.reload();
    }
    while (database.getClusterIdByName(iClass) > -1) {
      database.dropCluster(iClass);
      database.reload();
    }
  }
}
