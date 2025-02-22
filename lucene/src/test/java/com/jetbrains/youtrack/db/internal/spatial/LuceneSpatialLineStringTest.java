/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.jetbrains.youtrack.db.internal.spatial;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;

/**
 *
 */
public class LuceneSpatialLineStringTest extends BaseSpatialLuceneTest {

  public static String LINEWKT =
      "LINESTRING(-149.8871332 61.1484656,-149.8871655 61.1489556,-149.8871569"
          + " 61.15043,-149.8870366 61.1517722)";

  @Before
  public void initMore() {
    Schema schema = session.getMetadata().getSchema();
    var v = schema.getClass("V");
    var oClass = schema.createClass("Place");
    oClass.setSuperClass(session, v);
    oClass.createProperty(session, "location", PropertyType.EMBEDDED,
        schema.getClass("OLineString"));
    oClass.createProperty(session, "name", PropertyType.STRING);

    session.command("CREATE INDEX Place.location ON Place(location) SPATIAL ENGINE LUCENE").close();

    var linestring1 = ((EntityImpl) session.newEntity("Place"));
    linestring1.field("name", "LineString1");
    linestring1.field(
        "location",
        createLineString(
            new ArrayList<List<Double>>() {
              {
                add(Arrays.asList(0d, 0d));
                add(Arrays.asList(3d, 3d));
              }
            }));

    var linestring2 = ((EntityImpl) session.newEntity("Place"));
    linestring2.field("name", "LineString2");
    linestring2.field(
        "location",
        createLineString(
            new ArrayList<List<Double>>() {
              {
                add(Arrays.asList(0d, 1d));
                add(Arrays.asList(0d, 5d));
              }
            }));

    session.begin();
    session.commit();

    session.begin();
    session.command(
            "insert into Place set name = 'LineString3' , location = ST_GeomFromText('"
                + LINEWKT
                + "')")
        .close();
    session.commit();
  }

  public EntityImpl createLineString(List<List<Double>> coordinates) {
    var location = ((EntityImpl) session.newEntity("OLineString"));
    location.field("coordinates", coordinates);
    return location;
  }

  @Ignore
  public void testLineStringWithoutIndex() throws IOException {
    session.command("drop index Place.location").close();
    queryLineString();
  }

  protected void queryLineString() {
    var query =
        "select * from Place where location && { 'shape' : { 'type' : 'OLineString' , 'coordinates'"
            + " : [[1,2],[4,6]]} } ";
    var docs = session.query(query).toEntityList();

    Assert.assertEquals(1, docs.size());

    query = "select * from Place where location && 'LINESTRING(1 2, 4 6)' ";
    docs = session.query(new SQLSynchQuery<EntityImpl>(query));

    Assert.assertEquals(1, docs.size());

    query = "select * from Place where location && ST_GeomFromText('LINESTRING(1 2, 4 6)') ";
    docs = session.query(query).toEntityList();

    Assert.assertEquals(1, docs.size());

    query =
        "select * from Place where location && 'POLYGON((-150.205078125"
            + " 61.40723633876356,-149.2657470703125 61.40723633876356,-149.2657470703125"
            + " 61.05562700886678,-150.205078125 61.05562700886678,-150.205078125"
            + " 61.40723633876356))' ";
    docs = session.query(query).toEntityList();

    Assert.assertEquals(1, docs.size());
  }

  @Ignore
  public void testIndexingLineString() throws IOException {

    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "Place.location");

    session.begin();
    Assert.assertEquals(3, index.getInternal().size(session));
    session.commit();
    queryLineString();
  }
}
