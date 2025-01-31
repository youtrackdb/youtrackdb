/*
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.jetbrains.youtrack.db.internal.spatial;

import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialTxPointTest extends BaseSpatialLuceneTest {

  @Before
  public void init() {

    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    var oClass = schema.createClass("City");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "location", PropertyType.EMBEDDED, schema.getClass("OPoint"));
    oClass.createProperty(db, "name", PropertyType.STRING);

    var place = schema.createClass("Place");
    place.setSuperClass(db, v);
    place.createProperty(db, "latitude", PropertyType.DOUBLE);
    place.createProperty(db, "longitude", PropertyType.DOUBLE);
    place.createProperty(db, "name", PropertyType.STRING);

    db.command("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE").close();
  }

  protected EntityImpl newCity(String name, final Double longitude, final Double latitude) {

    var location = newPoint(longitude, latitude);

    var city = ((EntityImpl) db.newEntity("City"));
    city.field("name", name);
    city.field("location", location);
    return city;
  }

  private EntityImpl newPoint(final Double longitude, final Double latitude) {
    var location = ((EntityImpl) db.newEntity("OPoint"));
    location.field(
        "coordinates",
        new ArrayList<Double>() {
          {
            add(longitude);
            add(latitude);
          }
        });
    return location;
  }

  @Test
  public void testIndexingTxPoint() {

    var rome = newCity("Rome", 12.5, 41.9);

    db.begin();

    db.save(rome);

    var query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' ,"
            + " 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} }) = true";
    var docs = db.query(query);

    Assert.assertEquals(1, docs.stream().count());

    db.rollback();

    query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' ,"
            + " 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} }) = true";
    docs = db.query(query);

    Assert.assertEquals(0, docs.stream().count());
  }

  @Test
  public void testIndexingUpdateTxPoint() {

    var rome = newCity("Rome", -0.1275, 51.507222);

    db.begin();
    rome = db.save(rome);
    db.commit();

    db.begin();

    rome = db.bindToSession(rome);
    rome.field("location", newPoint(12.5, 41.9));

    db.save(rome);

    db.commit();

    var query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' ,"
            + " 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} }) = true";
    var docs = db.query(query);

    Assert.assertEquals(1, docs.stream().count());

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");

    db.begin();
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
  }

  @Test
  public void testIndexingComplexUpdateTxPoint() {

    var rome = newCity("Rome", 12.5, 41.9);
    var london = newCity("London", -0.1275, 51.507222);

    db.begin();
    rome = db.save(rome);
    london = db.save(london);
    db.commit();

    db.begin();

    rome = db.bindToSession(rome);
    london = db.bindToSession(london);

    rome.field("location", newPoint(12.5, 41.9));
    london.field("location", newPoint(-0.1275, 51.507222));
    london.field("location", newPoint(-0.1275, 51.507222));
    london.field("location", newPoint(12.5, 41.9));

    db.save(rome);
    db.save(london);

    db.commit();

    db.begin();
    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");

    Assert.assertEquals(2, index.getInternal().size(db));
    db.commit();
  }
}
