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

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.lucene.test.BaseLuceneTest;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialQueryTest extends BaseLuceneTest {

  @Before
  public void init() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");

    SchemaClass oClass = schema.createClass("Place");
    oClass.setSuperClass(db, v);
    oClass.createProperty(db, "latitude", PropertyType.DOUBLE);
    oClass.createProperty(db, "longitude", PropertyType.DOUBLE);
    oClass.createProperty(db, "name", PropertyType.STRING);

    db.command("CREATE INDEX Place.l_lon ON Place(latitude,longitude) SPATIAL ENGINE LUCENE")
        .close();

    try {
      ZipFile zipFile =
          new ZipFile(new File(ClassLoader.getSystemResource("location.csv.zip").getPath()));
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();

        YouTrackDBEnginesManager.instance()
            .getScheduler()
            .scheduleTask(
                new Runnable() {
                  @Override
                  public void run() {

                    Runtime runtime = Runtime.getRuntime();

                    StringBuilder sb = new StringBuilder();
                    long maxMemory = runtime.maxMemory();
                    long allocatedMemory = runtime.totalMemory();
                    long freeMemory = runtime.freeMemory();
                    LogManager.instance()
                        .info(
                            this,
                            "Memory Stats: free [%d], allocated [%d], max [%d] total free [%d]",
                            freeMemory / 1024,
                            allocatedMemory / 1024,
                            maxMemory / 1024,
                            (freeMemory + (maxMemory - allocatedMemory)) / 1024);
                  }
                },
                10000,
                10000);
        if (entry.getName().equals("location.csv")) {

          InputStream stream = zipFile.getInputStream(entry);
          LineNumberReader lnr = new LineNumberReader(new InputStreamReader(stream));

          String line;
          int i = 0;
          while ((line = lnr.readLine()) != null) {
            String[] nextLine = line.split(",");
            EntityImpl doc = new EntityImpl("Place");
            doc.field("name", nextLine[3]);
            doc.field("country", nextLine[1]);
            try {

              Double lat = PropertyType.convert(db, nextLine[5], Double.class).doubleValue();
              Double lng = PropertyType.convert(db, nextLine[6], Double.class).doubleValue();
              doc.field("latitude", lat);
              doc.field("longitude", lng);
            } catch (Exception e) {
              continue;
            }

            doc.save();
            if (i % 100000 == 0) {
              LogManager.instance().info(this, "Imported: [%d] records", i);
              db.commit();
              db.begin();
            }
            i++;
          }
          lnr.close();
          stream.close();
          db.commit();
        }
      }

    } catch (Exception e) {

    }
  }

  @Test
  @Ignore
  public void testNearQuery() {

    String query =
        "select *,$distance from Place where [latitude,longitude,$spatial] NEAR"
            + " [41.893056,12.482778,{\"maxDistance\": 0.5}]";
    ResultSet docs = db.query(query);

    Assert.assertTrue(docs.hasNext());

    // WHY ? 0.2749329729746763
    // Assert.assertEquals(0.27504313167833594, docs.get(0).field("$distance"));
    //    Assert.assertEquals(db, docs.get(0).field("$distance"));

    assertThat(docs.next().<Float>getProperty("$distance")).isEqualTo(0.2749329729746763);
    Assert.assertFalse(docs.hasNext());
  }

  @Test
  @Ignore
  public void testWithinQuery() {
    String query =
        "select * from Place where [latitude,longitude] WITHIN"
            + " [[51.507222,-0.1275],[55.507222,-0.1275]]";
    ResultSet docs = db.query(query);
    Assert.assertEquals(238, docs.stream().count());
  }
}
