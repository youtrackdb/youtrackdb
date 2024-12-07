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

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneMiscTest extends LuceneBaseTest {

  @Test
  public void testDoubleLucene() {

    db.command("create class Test extends V");
    db.command("create property Test.attr1 string");
    db.command("create index Test.attr1 on Test(attr1) FULLTEXT ENGINE LUCENE");
    db.command("create property Test.attr2 string");
    db.command("create index Test.attr2 on Test(attr2) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.command("insert into Test set attr1='foo', attr2='bar'");
    db.command("insert into Test set attr1='bar', attr2='foo'");
    db.commit();

    ResultSet results =
        db.query(
            "select from Test where search_index('Test.attr1',\"foo*\") =true OR"
                + " search_index('Test.attr2', \"foo*\")=true  ");
    assertThat(results).hasSize(2);
    results.close();

    results =
        db.query(
            "select from Test where SEARCH_FIELDS( ['attr1'], 'bar') = true OR"
                + " SEARCH_FIELDS(['attr2'], 'bar*' )= true ");
    assertThat(results).hasSize(2);
    results.close();

    results =
        db.query(
            "select from Test where SEARCH_FIELDS( ['attr1'], 'foo*') = true AND"
                + " SEARCH_FIELDS(['attr2'], 'bar*') = true");
    assertThat(results).hasSize(1);
    results.close();

    results =
        db.query(
            "select from Test where SEARCH_FIELDS( ['attr1'], 'bar*') = true AND"
                + " SEARCH_FIELDS(['attr2'], 'foo*')= true");
    assertThat(results).hasSize(1);
    results.close();
  }

  @Test
  public void testSubLucene() {

    db.command("create class Person extends V");

    db.command("create property Person.name string");

    db.command("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.command("insert into Person set name='Enrico', age=18");
    db.commit();

    String query =
        "select  from (select from Person where age = 18) where search_fields(['name'],'Enrico') ="
            + " true";
    ResultSet results = db.query(query);

    assertThat(results).hasSize(1);
    results.close();

    // WITH PROJECTION it works using index directly

    query =
        "select  from (select name from Person where age = 18) where"
            + " search_index('Person.name','Enrico') = true";
    results = db.query(query);
    assertThat(results).hasSize(1);
    results.close();
  }

  @Test
  public void testNamedParams() {

    db.command("create class Test extends V");

    db.command("create property Test.attr1 string");

    db.command("create index Test.attr1 on Test(attr1) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.command("insert into Test set attr1='foo', attr2='bar'");
    db.commit();

    String query = "select from Test where  search_class( :name) =true";
    Map params = new HashMap();
    params.put("name", "FOO or");
    ResultSet results = db.command(query, params);

    assertThat(results).hasSize(1);
  }

  @Test
  @Ignore
  public void dottedNotationTest() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");
    SchemaClass e = schema.getClass("E");
    SchemaClass author = schema.createClass("Author", v);
    author.createProperty(db, "name", PropertyType.STRING);

    SchemaClass song = schema.createClass("Song", v);
    song.createProperty(db, "title", PropertyType.STRING);

    SchemaClass authorOf = schema.createClass("AuthorOf", e);
    authorOf.createProperty(db, "in", PropertyType.LINK, song);
    db.commit();

    db.command("create index AuthorOf.in on AuthorOf (in) NOTUNIQUE");
    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE");

    Vertex authorVertex = db.newVertex("Author");
    authorVertex.setProperty("name", "Bob Dylan");

    db.begin();
    db.save(authorVertex);
    db.commit();

    Vertex songVertex = db.newVertex("Song");
    songVertex.setProperty("title", "hurricane");

    db.begin();
    db.save(songVertex);
    db.commit();

    Edge edge = authorVertex.addEdge(songVertex, "AuthorOf");
    db.begin();
    db.save(edge);
    db.commit();

    ResultSet results = db.query("select from AuthorOf");

    assertThat(results).hasSize(1);

    results.close();
    results = db.query("select from AuthorOf where in.title lucene 'hurricane'");

    assertThat(results).hasSize(1);
    results.close();
  }
}
