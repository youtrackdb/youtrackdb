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
package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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
    db.set(ATTRIBUTES.CUSTOM, "strictSql=false");
    YTSchema schema = db.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    YTClass oClass = schema.createClass("Place");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "location", YTType.EMBEDDED, schema.getClass("OLineString"));
    oClass.createProperty(db, "name", YTType.STRING);

    db.command("CREATE INDEX Place.location ON Place(location) SPATIAL ENGINE LUCENE").close();

    YTDocument linestring1 = new YTDocument("Place");
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

    YTDocument linestring2 = new YTDocument("Place");
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

    db.begin();
    db.save(linestring1);
    db.save(linestring2);
    db.commit();

    db.begin();
    db.command(
            "insert into Place set name = 'LineString3' , location = ST_GeomFromText('"
                + LINEWKT
                + "')")
        .close();
    db.commit();
  }

  public YTDocument createLineString(List<List<Double>> coordinates) {
    YTDocument location = new YTDocument("OLineString");
    location.field("coordinates", coordinates);
    return location;
  }

  @Ignore
  public void testLineStringWithoutIndex() throws IOException {
    db.command("drop index Place.location").close();
    queryLineString();
  }

  protected void queryLineString() {
    String query =
        "select * from Place where location && { 'shape' : { 'type' : 'OLineString' , 'coordinates'"
            + " : [[1,2],[4,6]]} } ";
    var docs = db.query(query).toEntityList();

    Assert.assertEquals(1, docs.size());

    query = "select * from Place where location && 'LINESTRING(1 2, 4 6)' ";
    docs = db.query(new OSQLSynchQuery<YTDocument>(query));

    Assert.assertEquals(1, docs.size());

    query = "select * from Place where location && ST_GeomFromText('LINESTRING(1 2, 4 6)') ";
    docs = db.query(query).toEntityList();

    Assert.assertEquals(1, docs.size());

    query =
        "select * from Place where location && 'POLYGON((-150.205078125"
            + " 61.40723633876356,-149.2657470703125 61.40723633876356,-149.2657470703125"
            + " 61.05562700886678,-150.205078125 61.05562700886678,-150.205078125"
            + " 61.40723633876356))' ";
    docs = db.query(query).toEntityList();

    Assert.assertEquals(1, docs.size());
  }

  @Ignore
  public void testIndexingLineString() throws IOException {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Place.location");

    db.begin();
    Assert.assertEquals(3, index.getInternal().size(db));
    db.commit();
    queryLineString();
  }
}
