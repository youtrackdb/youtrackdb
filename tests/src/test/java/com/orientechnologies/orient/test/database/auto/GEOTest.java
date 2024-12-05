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

import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.query.OSQLSynchQuery;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class GEOTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public GEOTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void geoSchema() {
    final YTClass mapPointClass = database.getMetadata().getSchema().createClass("MapPoint");
    mapPointClass.createProperty(database, "x", YTType.DOUBLE)
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
    mapPointClass.createProperty(database, "y", YTType.DOUBLE)
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);

    final Set<OIndex> xIndexes =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("x")
            .getIndexes(database);
    Assert.assertEquals(xIndexes.size(), 1);

    final Set<OIndex> yIndexes =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("y")
            .getIndexes(database);
    Assert.assertEquals(yIndexes.size(), 1);
  }

  @Test(dependsOnMethods = "geoSchema")
  public void checkGeoIndexes() {
    final Set<OIndex> xIndexes =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("x")
            .getIndexes(database);
    Assert.assertEquals(xIndexes.size(), 1);

    final Set<OIndex> yIndexDefinitions =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("y")
            .getIndexes(database);
    Assert.assertEquals(yIndexDefinitions.size(), 1);
  }

  @Test(dependsOnMethods = "checkGeoIndexes")
  public void queryCreatePoints() {
    YTEntityImpl point;

    for (int i = 0; i < 10000; ++i) {
      point = new YTEntityImpl();
      point.setClassName("MapPoint");

      point.field("x", (52.20472d + i / 100d));
      point.field("y", (0.14056d + i / 100d));

      database.begin();
      point.save();
      database.commit();
    }
  }

  @Test(dependsOnMethods = "queryCreatePoints")
  public void queryDistance() {
    Assert.assertEquals(database.countClass("MapPoint"), 10000);

    List<YTEntityImpl> result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from MapPoint where distance(x, y,52.20472, 0.14056 ) <= 30"))
            .execute(database);

    Assert.assertTrue(result.size() != 0);

    for (YTEntityImpl d : result) {
      Assert.assertEquals(d.getClassName(), "MapPoint");
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test(dependsOnMethods = "queryCreatePoints")
  public void queryDistanceOrdered() {
    Assert.assertEquals(database.countClass("MapPoint"), 10000);

    // MAKE THE FIRST RECORD DIRTY TO TEST IF DISTANCE JUMP IT
    List<YTEntityImpl> result =
        database.command(new OSQLSynchQuery<YTEntityImpl>("select from MapPoint limit 1"))
            .execute(database);
    try {
      result.get(0).field("x", "--wrong--");
      Assert.fail();
    } catch (YTDatabaseException e) {
      Assert.assertTrue(true);
    }

    result =
        executeQuery(
            "select distance(x, y,52.20472, 0.14056 ) as distance from MapPoint order by"
                + " distance desc");

    Assert.assertTrue(result.size() != 0);

    Double lastDistance = null;
    for (YTEntityImpl d : result) {
      if (lastDistance != null && d.field("distance") != null) {
        Assert.assertTrue(((Double) d.field("distance")).compareTo(lastDistance) <= 0);
      }
      lastDistance = d.field("distance");
    }
  }
}
