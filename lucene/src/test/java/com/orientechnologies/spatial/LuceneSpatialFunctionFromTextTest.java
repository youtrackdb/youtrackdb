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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.spatial.shape.legacy.OPointLegecyBuilder;
import java.io.IOException;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialFunctionFromTextTest extends BaseSpatialLuceneTest {

  @Test
  public void geomFromTextLineStringTest() {

    YTDocument point = lineStringDoc();
    checkFromText(point, "select ST_GeomFromText('" + LINESTRINGWKT + "') as geom");
  }

  protected void checkFromText(YTDocument source, String query) {

    OResultSet docs = db.command(query);

    assertTrue(docs.hasNext());

    YTEntity geom = ((OResult) docs.next().getProperty("geom")).toElement();
    assertGeometry(source, geom);
    assertFalse(docs.hasNext());
  }

  private void assertGeometry(YTEntity source, YTEntity geom) {
    Assert.assertNotNull(geom);

    Assert.assertNotNull(geom.getProperty("coordinates"));

    Assert.assertEquals(
        source.getSchemaType().get().getName(), geom.getSchemaType().get().getName());
    Assert.assertEquals(
        geom.<OPointLegecyBuilder>getProperty("coordinates"), source.getProperty("coordinates"));
  }

  @Test
  public void geomFromTextMultiLineStringTest() {

    YTDocument point = multiLineString();
    checkFromText(point, "select ST_GeomFromText('" + MULTILINESTRINGWKT + "') as geom");
  }

  @Test
  public void geomFromTextPointTest() {

    YTDocument point = point();
    checkFromText(point, "select ST_GeomFromText('" + POINTWKT + "') as geom");
  }

  @Test
  public void geomFromTextMultiPointTest() {

    YTDocument point = multiPoint();
    checkFromText(point, "select ST_GeomFromText('" + MULTIPOINTWKT + "') as geom");
  }

  // TODO enable
  @Test
  @Ignore
  public void geomFromTextRectangleTest() {
    YTDocument polygon = rectangle();
    // RECTANGLE
    checkFromText(polygon, "select ST_GeomFromText('" + RECTANGLEWKT + "') as geom");
  }

  @Test
  public void geomFromTextPolygonTest() {
    YTDocument polygon = polygon();
    checkFromText(polygon, "select ST_GeomFromText('" + POLYGONWKT + "') as geom");
  }

  @Test
  public void geomFromTextMultiPolygonTest() throws IOException {
    YTDocument polygon = loadMultiPolygon();

    checkFromText(polygon, "select ST_GeomFromText('" + MULTIPOLYGONWKT + "') as geom");
  }

  @Test
  public void geomCollectionFromText() {
    checkFromCollectionText(
        geometryCollection(), "select ST_GeomFromText('" + GEOMETRYCOLLECTION + "') as geom");
  }

  protected void checkFromCollectionText(YTDocument source, String query) {

    OResultSet docs = db.command(query);

    assertTrue(docs.hasNext());
    YTEntity geom = ((OResult) docs.next().getProperty("geom")).toElement();
    assertFalse(docs.hasNext());
    Assert.assertNotNull(geom);

    Assert.assertNotNull(geom.getProperty("geometries"));

    Assert.assertEquals(source.getClassName(), geom.getSchemaType().get().getName());

    List<YTDocument> sourceCollection = source.getProperty("geometries");
    List<YTDocument> targetCollection = source.getProperty("geometries");
    Assert.assertEquals(sourceCollection.size(), targetCollection.size());

    int i = 0;
    for (YTDocument entries : sourceCollection) {
      assertGeometry(entries, targetCollection.get(i));
      i++;
    }
  }
}
