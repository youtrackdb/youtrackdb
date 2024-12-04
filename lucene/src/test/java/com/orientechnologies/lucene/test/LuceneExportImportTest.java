/*
 *
 *
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

package com.orientechnologies.lucene.test;

import static com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE.FULLTEXT;
import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.lucene.OLuceneIndexFactory;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneExportImportTest extends BaseLuceneTest {

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", YTType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();

    YTDocument doc = new YTDocument("City");
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();
  }

  @Test
  public void testExportImport() {

    String file = "./target/exportTest.json";

    OResultSet query = db.query("select from City where name lucene 'Rome'");

    Assert.assertEquals(query.stream().count(), 1);

    try {

      // export
      new ODatabaseExport(
          db,
          file,
          new OCommandOutputListener() {
            @Override
            public void onMessage(String s) {
            }
          })
          .exportDatabase();

      dropDatabase();

      createDatabase();

      db = openDatabase();

      GZIPInputStream stream = new GZIPInputStream(new FileInputStream(file + ".gz"));
      new ODatabaseImport(
          db,
          stream,
          new OCommandOutputListener() {
            @Override
            public void onMessage(String s) {
            }
          })
          .importDatabase();
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    assertThat(db.countClass("City")).isEqualTo(1);
    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.name");

    assertThat(index.getType()).isEqualTo(FULLTEXT.toString());

    assertThat(index.getAlgorithm()).isEqualTo(OLuceneIndexFactory.LUCENE_ALGORITHM);

    // redo the query
    query = db.query("select from City where name lucene 'Rome'");

    assertThat(query).hasSize(1);
  }
}
