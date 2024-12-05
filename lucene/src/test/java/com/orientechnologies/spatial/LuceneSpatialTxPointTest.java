/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.spatial;

import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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
  }

  protected EntityImpl newCity(String name, final Double longitude, final Double latitude) {

    EntityImpl location = newPoint(longitude, latitude);

    EntityImpl city = new EntityImpl("City");
    city.field("name", name);
    city.field("location", location);
    return city;
  }

  private EntityImpl newPoint(final Double longitude, final Double latitude) {
    EntityImpl location = new EntityImpl("OPoint");
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

    EntityImpl rome = newCity("Rome", 12.5, 41.9);

    db.begin();

    db.save(rome);

    String query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' ,"
            + " 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} }) = true";
    YTResultSet docs = db.query(query);

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

    EntityImpl rome = newCity("Rome", -0.1275, 51.507222);

    db.begin();
    rome = db.save(rome);
    db.commit();

    db.begin();

    rome = db.bindToSession(rome);
    rome.field("location", newPoint(12.5, 41.9));

    db.save(rome);

    db.commit();

    String query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' ,"
            + " 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} }) = true";
    YTResultSet docs = db.query(query);

    Assert.assertEquals(1, docs.stream().count());

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");

    db.begin();
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
  }

  @Test
  public void testIndexingComplexUpdateTxPoint() {

    EntityImpl rome = newCity("Rome", 12.5, 41.9);
    EntityImpl london = newCity("London", -0.1275, 51.507222);

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
    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");

    Assert.assertEquals(2, index.getInternal().size(db));
    db.commit();
  }
}
