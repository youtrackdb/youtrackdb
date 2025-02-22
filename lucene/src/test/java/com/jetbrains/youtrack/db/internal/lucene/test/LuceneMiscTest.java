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

package com.jetbrains.youtrack.db.internal.lucene.test;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneMiscTest extends BaseLuceneTest {

  @Test
  public void testDoubleLucene() {

    session.command("create class Test extends V").close();
    session.command("create property Test.attr1 string").close();
    session.command("create index Test.attr1 on Test (attr1) fulltext engine lucene").close();
    session.command("create property Test.attr2 string").close();
    session.command("create index Test.attr2 on Test (attr2) fulltext engine lucene").close();

    session.begin();
    session.command("insert into Test set attr1='foo', attr2='bar'").close();
    session.command("insert into Test set attr1='bar', attr2='foo'").close();
    session.commit();

    var results =
        session.command("select from Test where attr1 lucene 'foo*' OR attr2 lucene 'foo*'");
    Assert.assertEquals(2, results.stream().count());

    results = session.command("select from Test where attr1 lucene 'bar*' OR attr2 lucene 'bar*'");

    Assert.assertEquals(2, results.stream().count());

    results = session.command("select from Test where attr1 lucene 'foo*' AND attr2 lucene 'bar*'");

    Assert.assertEquals(1, results.stream().count());

    results = session.command("select from Test where attr1 lucene 'bar*' AND attr2 lucene 'foo*'");

    Assert.assertEquals(1, results.stream().count());
  }

  @Test
  public void testSubLucene() {

    session.command("create class Person extends V").close();

    session.command("create property Person.name string").close();

    session.command("create index Person.name on Person (name) fulltext engine lucene").close();

    session.begin();
    session.command("insert into Person set name='Enrico', age=18").close();
    session.commit();

    var results =
        session.query(
            "select  from (select from Person where age = 18) where name lucene 'Enrico'");
    Assert.assertEquals(1, results.stream().count());

    // WITH PROJECTION does not work as the class is missing

    results =
        session.query(
            "select  from (select name  from Person where age = 18) where name lucene 'Enrico'");
    Assert.assertEquals(0, results.stream().count());
  }

  @Test
  public void testNamedParams() {

    session.command("create class Test extends V").close();

    session.command("create property Test.attr1 string").close();

    session.command("create index Test.attr1 on Test (attr1) fulltext engine lucene").close();

    session.begin();
    session.command("insert into Test set attr1='foo', attr2='bar'").close();
    session.commit();

    Map params = new HashMap();
    params.put("name", "FOO or");
    var results = session.query("select from Test where attr1 lucene :name", params);
    Assert.assertEquals(1, results.stream().count());
  }

  @Test
  public void dottedNotationTest() {

    Schema schema = session.getMetadata().getSchema();
    var v = schema.getClass("V");
    var e = schema.getClass("E");
    var author = schema.createClass("Author", v);
    author.createProperty(session, "name", PropertyType.STRING);

    var song = schema.createClass("Song", v);
    song.createProperty(session, "title", PropertyType.STRING);

    var authorOf = schema.createClass("AuthorOf", e);
    authorOf.createProperty(session, "in", PropertyType.LINK, song);

    session.command("create index AuthorOf.in on AuthorOf (in) NOTUNIQUE").close();
    session.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE").close();

    var authorVertex = session.newVertex("Author");
    authorVertex.setProperty("name", "Bob Dylan");

    session.begin();
    session.commit();

    var songVertex = session.newVertex("Song");
    songVertex.setProperty("title", "hurricane");

    session.begin();
    session.commit();

    session.begin();
    authorVertex = session.bindToSession(authorVertex);
    songVertex = session.bindToSession(songVertex);
    var edge = authorVertex.addEdge(songVertex, "AuthorOf");
    session.commit();

    var results = session.query("select from AuthorOf");
    Assert.assertEquals(results.stream().count(), 1);

    List<?> results1 =
        session.command(new CommandSQL("select from AuthorOf where in.title lucene 'hurricane'"))
            .execute(session);

    Assert.assertEquals(results1.size(), 1);
  }

  @Test
  public void testUnderscoreField() {

    session.command("create class Test extends V").close();

    session.command("create property V._attr1 string").close();

    session.command("create index V._attr1 on V (_attr1) fulltext engine lucene").close();

    session.begin();
    session.command("insert into Test set _attr1='anyPerson', attr2='bar'").close();
    session.commit();

    Map params = new HashMap();
    params.put("name", "anyPerson");
    var results = session.command("select from Test where _attr1 lucene :name", params);
    Assert.assertEquals(results.stream().count(), 1);
  }
}
