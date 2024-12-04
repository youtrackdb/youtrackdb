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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneTransactionGeoQueryTest {

  private static final String PWKT = "POINT(-160.2075374 21.9029803)";

  @Test
  public void testPointTransactionRollBack() {

    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx("memory:txPoint");

    try {
      db.create();

      YTSchema schema = db.getMetadata().getSchema();
      YTClass v = schema.getClass("V");
      YTClass oClass = schema.createClass("City");
      oClass.setSuperClass(db, v);
      oClass.createProperty(db, "location", YTType.EMBEDDED, schema.getClass("OPoint"));
      oClass.createProperty(db, "name", YTType.STRING);

      db.command("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE").close();

      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");
      YTDocument rome = newCity("Rome", 12.5, 41.9);
      YTDocument london = newCity("London", -0.1275, 51.507222);

      db.begin();

      db.command(
              "insert into City set name = 'TestInsert' , location = ST_GeomFromText('"
                  + PWKT
                  + "')")
          .close();
      db.save(rome);
      db.save(london);
      String query =
          "select * from City where location && 'LINESTRING(-160.06393432617188"
              + " 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375"
              + " 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594"
              + " 21.787556698550834)' ";
      List<YTDocument> docs = db.query(new OSQLSynchQuery<YTDocument>(query));
      Assert.assertEquals(1, docs.size());
      Assert.assertEquals(3, idx.getInternal().size(db));
      db.rollback();

      query =
          "select * from City where location && 'LINESTRING(-160.06393432617188"
              + " 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375"
              + " 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594"
              + " 21.787556698550834)' ";
      docs = db.query(new OSQLSynchQuery<YTDocument>(query));

      db.begin();
      Assert.assertEquals(0, docs.size());
      Assert.assertEquals(0, idx.getInternal().size(db));
      db.commit();
    } finally {
      db.drop();
    }
  }

  @Test
  public void testPointTransactionUpdate() {

    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx("memory:txPoint");

    try {
      db.create();

      YTSchema schema = db.getMetadata().getSchema();
      YTClass v = schema.getClass("V");
      YTClass oClass = schema.createClass("City");
      oClass.setSuperClass(db, v);
      oClass.createProperty(db, "location", YTType.EMBEDDED, schema.getClass("OPoint"));
      oClass.createProperty(db, "name", YTType.STRING);

      db.command("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE").close();

      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");
      YTDocument rome = newCity("Rome", 12.5, 41.9);

      db.begin();

      db.save(rome);

      db.commit();

      String query =
          "select * from City where location && 'LINESTRING(-160.06393432617188"
              + " 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375"
              + " 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594"
              + " 21.787556698550834)' ";
      List<YTDocument> docs = db.query(new OSQLSynchQuery<YTDocument>(query));

      db.begin();
      Assert.assertEquals(0, docs.size());
      Assert.assertEquals(1, idx.getInternal().size(db));

      db.command("update City set location = ST_GeomFromText('" + PWKT + "')").close();

      query =
          "select * from City where location && 'LINESTRING(-160.06393432617188"
              + " 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375"
              + " 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594"
              + " 21.787556698550834)' ";
      docs = db.query(new OSQLSynchQuery<YTDocument>(query));
      Assert.assertEquals(1, docs.size());
      Assert.assertEquals(1, idx.getInternal().size(db));

      db.commit();

      query =
          "select * from City where location && 'LINESTRING(-160.06393432617188"
              + " 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375"
              + " 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594"
              + " 21.787556698550834)' ";
      docs = db.query(new OSQLSynchQuery<YTDocument>(query));

      db.begin();
      Assert.assertEquals(1, docs.size());
      Assert.assertEquals(1, idx.getInternal().size(db));
      db.commit();

    } finally {
      db.drop();
    }
  }

  protected YTDocument newCity(String name, final Double longitude, final Double latitude) {

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
