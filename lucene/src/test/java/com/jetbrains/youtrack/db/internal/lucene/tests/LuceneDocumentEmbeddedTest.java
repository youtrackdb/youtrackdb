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

package com.jetbrains.youtrack.db.internal.lucene.tests;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneDocumentEmbeddedTest extends LuceneBaseTest {

  @Before
  public void init() {
    var type = session.getMetadata().getSchema().createClass("City");
    type.createProperty(session, "name", PropertyType.STRING);

    session.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void embeddedNoTx() {

    var doc = ((EntityImpl) session.newEntity("City"));

    doc.field("name", "London");
    session.begin();
    session.commit();

    doc = ((EntityImpl) session.newEntity("City"));
    doc.field("name", "Rome");

    session.begin();
    session.commit();

    var results =
        session.command("select from City where SEARCH_FIELDS(['name'] ,'London') = true ");

    Assertions.assertThat(results).hasSize(1);
  }

  @Test
  public void embeddedTx() {

    var doc = ((EntityImpl) session.newEntity("City"));

    session.begin();
    doc.field("name", "Berlin");

    session.commit();

    var results =
        session.command("select from City where SEARCH_FIELDS(['name'] ,'Berlin')=true ");

    Assertions.assertThat(results).hasSize(1);
  }
}
