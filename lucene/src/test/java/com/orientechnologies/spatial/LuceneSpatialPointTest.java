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

import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialPointTest extends BaseSpatialLuceneTest {

  private static final String PWKT = "POINT(-160.2075374 21.9029803)";

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    YTClass oClass = schema.createClass("City");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "location", YTType.EMBEDDED, schema.getClass("OPoint"));
    oClass.createProperty(db, "name", YTType.STRING);

    YTClass place = schema.createClass("Place");
    place.setSuperClass(db, v);
    place.createProperty(db, "latitude", YTType.DOUBLE);
    place.createProperty(db, "longitude", YTType.DOUBLE);
    place.createProperty(db, "name", YTType.STRING);

    db.command("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE").close();

    db.command("CREATE INDEX Place.l_lon ON Place(latitude,longitude) SPATIAL ENGINE LUCENE")
        .close();

    YTDocument rome = newCity("Rome", 12.5, 41.9);
    YTDocument london = newCity("London", -0.1275, 51.507222);

    YTDocument rome1 = new YTDocument("Place");
    rome1.field("name", "Rome");
    rome1.field("latitude", 41.9);
    rome1.field("longitude", 12.5);

    db.begin();
    db.save(rome1);
    db.save(rome);
    db.save(london);
    db.commit();

    db.begin();
    db.command(
            "insert into City set name = 'TestInsert' , location = ST_GeomFromText('" + PWKT + "')")
        .close();
    db.commit();
  }

  @Test
  public void testPointWithoutIndex() {

    db.command("Drop INDEX City.location").close();
    queryPoint();
  }

  @Test
  public void testIndexingPoint() {

    queryPoint();
  }

  protected void queryPoint() {
    // TODO remove = true when parser will support index function without expression
    String query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' ,"
            + " 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} }) = true";
    OResultSet docs = db.query(query);

    Assert.assertEquals(1, docs.stream().count());

    query =
        "select * from City where  ST_WITHIN(location,'POLYGON ((12.314015 41.8262816, 12.314015"
            + " 41.963125, 12.6605063 41.963125, 12.6605063 41.8262816, 12.314015 41.8262816))') ="
            + " true";
    docs = db.query(query);

    Assert.assertEquals(1, docs.stream().count());

    query =
        "select * from City where  ST_WITHIN(location,ST_GeomFromText('POLYGON ((12.314015"
            + " 41.8262816, 12.314015 41.963125, 12.6605063 41.963125, 12.6605063 41.8262816,"
            + " 12.314015 41.8262816))')) = true";
    docs = db.query(query);
    Assert.assertEquals(1, docs.stream().count());

//    query =
//        "select * from City where location && 'LINESTRING(-160.06393432617188"
//            + " 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375"
//            + " 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594"
//            + " 21.787556698550834)' ";
//    List<?> old = db.query(query).toList();
//
//    Assert.assertEquals(1, old.size());
  }

  protected static YTDocument newCity(String name, final Double longitude, final Double latitude) {
    YTDocument location = new YTDocument("OPoint");
    location.field(
        "coordinates",
        new ArrayList<Double>() {
          {
            add(longitude);
            add(latitude);
          }
        });

    YTDocument city = new YTDocument("City");
    city.field("name", name);
    city.field("location", location);
    return city;
  }
}
