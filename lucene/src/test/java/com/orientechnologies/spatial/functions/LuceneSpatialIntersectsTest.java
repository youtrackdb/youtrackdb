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
package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialIntersectsTest extends BaseSpatialLuceneTest {

  @Test
  public void testIntersectsNoIndex() {

    YTResultSet execute =
        db.query("SELECT ST_Intersects('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )') as ST_Intersects");
    YTResult next = execute.next();

    Assert.assertEquals(next.getProperty("ST_Intersects"), false);
    execute.close();
    execute =
        db.query("SELECT ST_Intersects('POINT(0 0)', 'LINESTRING ( 0 0, 0 2 )') as ST_Intersects");
    next = execute.next();

    Assert.assertEquals(next.getProperty("ST_Intersects"), true);
    execute.close();
  }

  @Test
  public void testIntersectsIndex() {

    db.command("create class Lines extends v").close();
    db.command("create property Lines.geometry EMBEDDED OLINESTRING").close();

    db.begin();
    db.command("insert into Lines set geometry = ST_GeomFromText('LINESTRING ( 2 0, 0 2 )')")
        .close();
    db.command("insert into Lines set geometry = ST_GeomFromText('LINESTRING ( 0 0, 0 2 )')")
        .close();
    db.commit();

    db.command("create index L.g on Lines (geometry) SPATIAL engine lucene").close();
    YTResultSet execute =
        db.query("SELECT from lines where ST_Intersects(geometry, 'POINT(0 0)') = true");

    Assert.assertEquals(execute.stream().count(), 1);
  }
}
