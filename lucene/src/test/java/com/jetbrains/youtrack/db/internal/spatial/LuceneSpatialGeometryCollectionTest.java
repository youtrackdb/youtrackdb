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

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialGeometryCollectionTest extends BaseSpatialLuceneTest {

  @Before
  public void init() {
    db.command("create class test").close();
    db.command("create property test.name STRING").close();
    db.command("create property test.geometry EMBEDDED OGeometryCollection").close();

    db.command("create index test.geometry on test (geometry) SPATIAL engine lucene").close();
  }

  @Test
  public void testGeoCollectionOutsideTx() {
    var test1 = ((EntityImpl) db.newEntity("test"));
    test1.field("name", "test1");
    var geometry = ((EntityImpl) db.newEntity("OGeometryCollection"));
    var point = ((EntityImpl) db.newEntity("OPoint"));
    point.field("coordinates", Arrays.asList(1.0, 2.0));
    var polygon = ((EntityImpl) db.newEntity("OPolygon"));
    polygon.field(
        "coordinates",
        List.of(
            Arrays.asList(
                Arrays.asList(0.0, 0.0),
                Arrays.asList(10.0, 0.0),
                Arrays.asList(10.0, 10.0),
                Arrays.asList(0.0, 10.0),
                Arrays.asList(0.0, 0.0))));
    geometry.field("geometries", Arrays.asList(point, polygon));
    test1.field("geometry", geometry);

    db.begin();
    test1.save();
    db.commit();

    var execute =
        db.command(
            "SELECT from test where ST_Contains(geometry, ST_GeomFromText('POINT(1 1)')) = true");

    Assert.assertEquals(execute.stream().count(), 1);
  }

  @Test
  public void testGeoCollectionInsideTransaction() {
    db.begin();

    var test1 = ((EntityImpl) db.newEntity("test"));
    test1.field("name", "test1");
    var geometry = ((EntityImpl) db.newEntity("OGeometryCollection"));
    var point = ((EntityImpl) db.newEntity("OPoint"));
    point.field("coordinates", Arrays.asList(1.0, 2.0));
    var polygon = ((EntityImpl) db.newEntity("OPolygon"));
    polygon.field(
        "coordinates",
        List.of(
            Arrays.asList(
                Arrays.asList(0.0, 0.0),
                Arrays.asList(10.0, 0.0),
                Arrays.asList(10.0, 10.0),
                Arrays.asList(0.0, 10.0),
                Arrays.asList(0.0, 0.0))));
    geometry.field("geometries", Arrays.asList(point, polygon));
    test1.field("geometry", geometry);
    test1.save();

    db.commit();

    var execute =
        db.command(
            "SELECT from test where ST_Contains(geometry, ST_GeomFromText('POINT(1 1)')) = true");

    Assert.assertEquals(execute.stream().count(), 1);
  }
}
