package com.jetbrains.youtrack.db.internal.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import org.junit.Before;
import org.junit.Test;

public class LucenePhraseQueriesTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {

    var type = session.createVertexClass("Role");
    type.createProperty(session, "name", PropertyType.STRING);

    session.command(
            "create index Role.name on Role (name) FULLTEXT ENGINE LUCENE "
                + "METADATA {"
                + "\"name_index\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
                + "\"name_index_stopwords\": [],"
                + "\"name_query\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\","
                + "\"name_query_stopwords\": []"
                //                + "\"name_query\":
                // \"org.apache.lucene.analysis.core.KeywordAnalyzer\""
                + "} ")
        .close();

    session.begin();
    var role = session.newVertex("Role");
    role.setProperty("name", "System IT Owner");
    session.save(role);

    role = session.newVertex("Role");
    role.setProperty("name", "System Business Owner");
    session.save(role);

    role = session.newVertex("Role");
    role.setProperty("name", "System Business SME");
    session.save(role);

    role = session.newVertex("Role");
    role.setProperty("name", "System Technical SME");
    session.save(role);

    role = session.newVertex("Role");
    role.setProperty("name", "System");
    session.save(role);

    role = session.newVertex("Role");
    role.setProperty("name", "boat");
    session.save(role);

    role = session.newVertex("Role");
    role.setProperty("name", "moat");
    session.save(role);
    session.commit();
  }

  @Test
  public void testPhraseQueries() throws Exception {

    var vertexes = session.query("select from Role where name lucene ' \"Business Owner\" '  ");

    assertThat(vertexes).hasSize(1);

    vertexes = session.query("select from Role where name lucene ' \"Owner of Business\" '  ");

    assertThat(vertexes).hasSize(0);

    vertexes = session.query("select from Role where name lucene ' \"System Owner\" '  ");

    assertThat(vertexes).hasSize(0);

    vertexes = session.query("select from Role where name lucene ' \"System SME\"~1 '  ");

    assertThat(vertexes).hasSize(2);

    vertexes = session.query("select from Role where name lucene ' \"System Business\"~1 '  ");

    assertThat(vertexes).hasSize(2);

    vertexes = session.query("select from Role where name lucene ' /[mb]oat/ '  ");

    assertThat(vertexes).hasSize(2);
  }

  @Test
  public void testComplexPhraseQueries() throws Exception {

    var vertexes = session.query("select from Role where name lucene ?", "\"System SME\"~1");

    assertThat(vertexes).allMatch(v -> v.<String>getProperty("name").contains("SME"));

    vertexes = session.query("select from Role where name lucene ? ", "\"SME System\"~1");

    assertThat(vertexes).isEmpty();

    vertexes = session.query("select from Role where name lucene ? ", "\"Owner Of Business\"");
    vertexes.stream().forEach(v -> System.out.println("v = " + v.getProperty("name")));

    assertThat(vertexes).isEmpty();

    vertexes = session.query("select from Role where name lucene ? ", "\"System Business SME\"");

    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business SME"));

    vertexes = session.query("select from Role where name lucene ? ", "\"System Owner\"~1 -IT");
    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes = session.query("select from Role where name lucene ? ", "+System +Own*~0.0 -IT");
    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes = session.query("select from Role where name lucene ? ",
        "\"System Owner\"~1 -Business");
    assertThat(vertexes)
        .hasSize(1)
        .allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System IT Owner"));
  }
}
