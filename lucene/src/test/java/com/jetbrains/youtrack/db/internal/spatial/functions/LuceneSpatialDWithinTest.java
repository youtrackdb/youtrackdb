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
package com.jetbrains.youtrack.db.internal.spatial.functions;

import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.spatial.BaseSpatialLuceneTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialDWithinTest extends BaseSpatialLuceneTest {

  @Test
  public void testDWithinNoIndex() {

    ResultSet execute =
        db.query(
            "SELECT ST_DWithin(ST_GeomFromText('POLYGON((0 0, 10 0, 10 5, 0 5, 0 0))'),"
                + " ST_GeomFromText('POLYGON((12 0, 14 0, 14 6, 12 6, 12 0))'), 2.0d) as distance");

    Result next = execute.next();

    Assert.assertEquals(true, next.getProperty("distance"));
    Assert.assertFalse(execute.hasNext());
  }

  // TODO
  // Need more test with index
  @Test
  public void testWithinIndex() {

    db.command("create class Polygon extends v").close();
    db.command("create property Polygon.geometry EMBEDDED OPolygon").close();

    db.begin();
    db.command(
            "insert into Polygon set geometry = ST_GeomFromText('POLYGON((0 0, 10 0, 10 5, 0 5, 0"
                + " 0))')")
        .close();
    db.commit();

    db.command("create index Polygon.g on Polygon (geometry) SPATIAL engine lucene").close();

    ResultSet execute =
        db.query(
            "SELECT from Polygon where ST_DWithin(geometry, ST_GeomFromText('POLYGON((12 0, 14 0,"
                + " 14 6, 12 6, 12 0))'), 2.0) = true");

    Assert.assertEquals(1, execute.stream().count());

    ResultSet resultSet =
        db.query(
            "SELECT from Polygon where ST_DWithin(geometry, ST_GeomFromText('POLYGON((12 0, 14 0,"
                + " 14 6, 12 6, 12 0))'), 2.0) = true");

    resultSet.stream().forEach(r -> System.out.println("r = " + r));
    resultSet.close();
  }
}
