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

package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneDocumentEmbeddedTest extends OLuceneBaseTest {

  @Before
  public void init() {
    YTClass type = db.getMetadata().getSchema().createClass("City");
    type.createProperty(db, "name", YTType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void embeddedNoTx() {

    YTDocument doc = new YTDocument("City");

    doc.field("name", "London");
    db.begin();
    db.save(doc);
    db.commit();

    doc = new YTDocument("City");
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();

    OResultSet results =
        db.command("select from City where SEARCH_FIELDS(['name'] ,'London') = true ");

    Assertions.assertThat(results).hasSize(1);
  }

  @Test
  public void embeddedTx() {

    YTDocument doc = new YTDocument("City");

    db.begin();
    doc.field("name", "Berlin");

    db.save(doc);

    db.commit();

    OResultSet results =
        db.command("select from City where SEARCH_FIELDS(['name'] ,'Berlin')=true ");

    Assertions.assertThat(results).hasSize(1);
  }
}
