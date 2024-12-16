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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

/**
 *
 */
public class LuceneSpatialFunctionAsTextTest extends BaseSpatialLuceneTest {

  @Before
  public void init() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");
    SchemaClass oClass = schema.createClass("Location");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "geometry", PropertyType.EMBEDDED, schema.getClass("OShape"));
    oClass.createProperty(db, "name", PropertyType.STRING);

    initData();
  }

  private void initData() {

    createLocation("OPoint", point());
    createLocation("OMultiPoint", multiPoint());
    createLocation("OLineString", lineStringDoc());
    createLocation("OMultiLineString", multiLineString());
    createLocation("ORectangle", rectangle());
    createLocation("OPolygon", polygon());
    createLocation("OMultiPolygon", loadMultiPolygon());
    createLocation("OGeometryCollection", geometryCollection());
  }

  protected void createLocation(String name, EntityImpl geometry) {
    EntityImpl doc = new EntityImpl("Location");
    doc.field("name", name);
    doc.field("geometry", geometry);

    db.begin();
    db.save(doc);
    db.commit();
  }

  @Test
  public void testPoint() {

    queryAndAssertGeom("OPoint", POINTWKT);
  }

  protected void queryAndAssertGeom(String name, String wkt) {
    ResultSet results =
        db.command("select *, ST_AsText(geometry) as text from Location where name = ? ", name);

    assertTrue(results.hasNext());
    Result doc = results.next();

    String asText = doc.getProperty("text");

    Assert.assertNotNull(asText);
    Assert.assertEquals(asText, wkt);
    assertFalse(results.hasNext());
  }

  @Test
  public void testMultiPoint() {

    queryAndAssertGeom("OMultiPoint", MULTIPOINTWKT);
  }

  @Test
  public void testLineString() {
    queryAndAssertGeom("OLineString", LINESTRINGWKT);
  }

  @Test
  public void testMultiLineString() {
    queryAndAssertGeom("OMultiLineString", MULTILINESTRINGWKT);
  }

  @Test
  @Ignore
  public void testRectangle() {
    queryAndAssertGeom("ORectangle", RECTANGLEWKT);
  }

  @Test
  public void testBugEnvelope() {
    try {
      Shape shape = context.readShapeFromWkt(RECTANGLEWKT);

      Geometry geometryFrom = context.getGeometryFrom(shape);
      Assert.assertEquals(geometryFrom.toText(), RECTANGLEWKT);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPolygon() {
    queryAndAssertGeom("OPolygon", POLYGONWKT);
  }

  @Test
  public void testGeometryCollection() {
    queryAndAssertGeom("OGeometryCollection", GEOMETRYCOLLECTION);
  }

  @Test
  public void testMultiPolygon() {
    queryAndAssertGeom("OMultiPolygon", MULTIPOLYGONWKT);
  }
}
