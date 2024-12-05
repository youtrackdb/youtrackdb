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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialPolygonTest extends BaseSpatialLuceneTest {

  @Before
  public void init() {

    db.set(ATTRIBUTES.CUSTOM, "strictSql=false");
    YTSchema schema = db.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    YTClass oClass = schema.createClass("Place");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "location", YTType.EMBEDDED, schema.getClass("OPolygon"));
    oClass.createProperty(db, "name", YTType.STRING);

    db.command("CREATE INDEX Place.location ON Place(location) SPATIAL ENGINE LUCENE").close();
  }

  @Test
  public void testPolygonWithoutIndex() throws IOException {
    testIndexingPolygon();
    db.command("drop index Place.location").close();
    queryPolygon();
  }

  protected void queryPolygon() {

    String query = "select * from Place where location && 'POINT(13.383333 52.516667)'";
    List<EntityImpl> docs = db.query(new OSQLSynchQuery<EntityImpl>(query));

    Assert.assertEquals(docs.size(), 1);

    query = "select * from Place where location && 'POINT(12.5 41.9)'";
    docs = db.query(new OSQLSynchQuery<EntityImpl>(query));

    Assert.assertEquals(docs.size(), 0);
  }

  @Test
  public void testIndexingPolygon() throws IOException {

    InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("germany.json");

    EntityImpl doc = new EntityImpl().fromJSON(systemResourceAsStream);

    Map geometry = doc.field("geometry");

    String type = (String) geometry.get("type");
    EntityImpl location = new EntityImpl("O" + type);
    location.field("coordinates", geometry.get("coordinates"));
    EntityImpl germany = new EntityImpl("Place");
    germany.field("name", "Germany");
    germany.field("location", location);

    db.begin();
    db.save(germany);
    db.commit();

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Place.location");

    db.begin();
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
    queryPolygon();
  }
}
