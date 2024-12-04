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

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
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

  private YTRID carID;
  private YTRID johnDoeID;
  private YTRID janeDoeID;
  private YTRID chuckNorrisID;
  private YTRID jackBauerID;
  private YTRID ctuID;
  private YTRID fbiID;

  @Parameters(value = "remote")
  public SQLFindReferencesTest(boolean remote) {
    super(remote);
  }

  @Test
  public void findSimpleReference() {
    List<OResult> result = database.command("find references " + carID).stream().toList();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.iterator().next().getProperty("referredBy"), johnDoeID);

    // SUB QUERY
    result = database.command("find references ( select from " + carID + ")").stream().toList();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.iterator().next().getProperty("referredBy"), johnDoeID);

    result = database.command("find references " + chuckNorrisID).stream().toList();
    Assert.assertEquals(result.size(), 2);

    for (OResult rid : result) {
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
    List<OResult> result =
        database.command("find references " + janeDoeID + " [" + WORKPLACE + "]").stream().toList();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(ctuID, result.iterator().next().getProperty("referredBy"));

    result =
        database
            .command("find references " + jackBauerID + " [" + WORKPLACE + ",cluster:" + CAR + "]")
            .stream()
            .toList();

    Assert.assertEquals(result.size(), 3);

    for (OResult res : result) {
      YTIdentifiable rid = res.getProperty("referredBy");
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
    YTClass worker = database.getMetadata().getSchema().createClass(WORKER);
    YTClass workplace = database.getMetadata().getSchema().createClass(WORKPLACE);
    YTClass car = database.getMetadata().getSchema().createClass(CAR);

    worker.createProperty(database, "name", YTType.STRING);
    worker.createProperty(database, "surname", YTType.STRING);
    worker.createProperty(database, "colleagues", YTType.LINKLIST, worker);
    worker.createProperty(database, "car", YTType.LINK, car);

    workplace.createProperty(database, "name", YTType.STRING);
    workplace.createProperty(database, "boss", YTType.LINK, worker);
    workplace.createProperty(database, "workers", YTType.LINKLIST, worker);

    car.createProperty(database, "plate", YTType.STRING);
    car.createProperty(database, "owner", YTType.LINK, worker);
  }

  private void populateDatabase() {
    database.begin();
    YTDocument car = new YTDocument(CAR);
    car.field("plate", "JINF223S");

    YTDocument johnDoe = new YTDocument(WORKER);
    johnDoe.field("name", "John");
    johnDoe.field("surname", "Doe");
    johnDoe.field("car", car);
    johnDoe.save();

    YTDocument janeDoe = new YTDocument(WORKER);
    janeDoe.field("name", "Jane");
    janeDoe.field("surname", "Doe");
    janeDoe.save();

    YTDocument chuckNorris = new YTDocument(WORKER);
    chuckNorris.field("name", "Chuck");
    chuckNorris.field("surname", "Norris");
    chuckNorris.save();

    YTDocument jackBauer = new YTDocument(WORKER);
    jackBauer.field("name", "Jack");
    jackBauer.field("surname", "Bauer");
    jackBauer.save();

    YTDocument ctu = new YTDocument(WORKPLACE);
    ctu.field("name", "CTU");
    ctu.field("boss", jackBauer);
    List<YTDocument> workplace1Workers = new ArrayList<YTDocument>();
    workplace1Workers.add(chuckNorris);
    workplace1Workers.add(janeDoe);
    ctu.field("workers", workplace1Workers);
    ctu.save();

    YTDocument fbi = new YTDocument(WORKPLACE);
    fbi.field("name", "FBI");
    fbi.field("boss", chuckNorris);
    List<YTDocument> workplace2Workers = new ArrayList<YTDocument>();
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
