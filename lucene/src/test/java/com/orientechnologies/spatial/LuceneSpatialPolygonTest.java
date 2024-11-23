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

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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

    db.set(ODatabaseSession.ATTRIBUTES.CUSTOM, "strictSql=false");
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("Place");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "location", OType.EMBEDDED, schema.getClass("OPolygon"));
    oClass.createProperty(db, "name", OType.STRING);

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
    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    query = "select * from Place where location && 'POINT(12.5 41.9)'";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 0);
  }

  @Test
  public void testIndexingPolygon() throws IOException {

    InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("germany.json");

    ODocument doc = new ODocument().fromJSON(systemResourceAsStream);

    Map geometry = doc.field("geometry");

    String type = (String) geometry.get("type");
    ODocument location = new ODocument("O" + type);
    location.field("coordinates", geometry.get("coordinates"));
    ODocument germany = new ODocument("Place");
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
