package com.orientechnologies.lucene.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.orientechnologies.lucene.test.BaseLuceneTest;
import java.io.InputStream;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSearchMoreLikeThisFunctionTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql")) {
      //noinspection resource
      db.execute("sql", getScriptFromStream(stream)).close();
    }
  }

  @Test
  public void shouldSearchMoreLikeThisWithRid() throws Exception {
    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    SchemaClass clazz = db.getMetadata().getSchema().getClass("Song");
    int defCluster = clazz.getDefaultClusterId();

    try (ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_More([#"
                + defCluster
                + ":2, #"
                + defCluster
                + ":3],{'minTermFreq':1, 'minDocFreq':1} ) = true")) {
      assertThat(resultSet).hasSize(48);
    }
  }

  @Test
  public void shouldSearchMoreLikeThisWithRidOnMultiFieldsIndex() throws Exception {

    db.command("create index Song.multi on Song (title,author) FULLTEXT ENGINE LUCENE ");

    SchemaClass clazz = db.getMetadata().getSchema().getClass("Song");
    int defCluster = clazz.getDefaultClusterId();

    try (ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_More([#"
                + defCluster
                + ":2, #"
                + defCluster
                + ":3] , {'minTermFreq':1, 'minDocFreq':1} ) = true")) {
      assertThat(resultSet).hasSize(84);
    }
  }

  @Test
  public void shouldSearchOnFieldAndMoreLikeThisWithRidOnMultiFieldsIndex() throws Exception {
    db.command("create index Song.multi on Song (title) FULLTEXT ENGINE LUCENE ");

    SchemaClass clazz = db.getMetadata().getSchema().getClass("Song");
    int defCluster = clazz.getDefaultClusterId();

    try (ResultSet resultSet =
        db.query(
            "SELECT from Song where author ='Hunter' AND SEARCH_More([#"
                + defCluster
                + ":2, #"
                + defCluster
                + ":3,#"
                + defCluster
                + ":4,#"
                + defCluster
                + ":5],{'minTermFreq':1, 'minDocFreq':1} ) = true")) {
      assertThat(resultSet).hasSize(8);
    }
  }

  @Test
  public void shouldSearchOnFieldOrMoreLikeThisWithRidOnMultiFieldsIndex() throws Exception {

    db.command("create index Song.multi on Song (title) FULLTEXT ENGINE LUCENE ");

    SchemaClass clazz = db.getMetadata().getSchema().getClass("Song");
    int defCluster = clazz.getDefaultClusterId();

    try (ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_More([#"
                + defCluster
                + ":2, #"
                + defCluster
                + ":3], {'minTermFreq':1, 'minDocFreq':1} ) = true OR author ='Hunter' ")) {
      resultSet.getExecutionPlan().ifPresent(c -> System.out.println(c.prettyPrint(1, 1)));
      assertThat(resultSet).hasSize(138);
    }
  }

  @Test
  public void shouldSearchMoreLikeThisWithRidOnMultiFieldsIndexWithMetadata() throws Exception {

    db.command("create index Song.multi on Song (title,author) FULLTEXT ENGINE LUCENE ");

    SchemaClass clazz = db.getMetadata().getSchema().getClass("Song");
    int defCluster = clazz.getDefaultClusterId();

    try (ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_More( [#"
                + defCluster
                + ":2, #"
                + defCluster
                + ":3] , {'fields': [ 'title' ], 'minTermFreq':1, 'minDocFreq':1}) = true")) {

      resultSet.getExecutionPlan().ifPresent(c -> System.out.println(c.prettyPrint(1, 1)));
      assertThat(resultSet).hasSize(84);
    }
  }

  @Test
  public void shouldSearchMoreLikeThisWithInnerQuery() throws Exception {

    db.command("create index Song.multi on Song (title,author) FULLTEXT ENGINE LUCENE ");

    try (ResultSet resultSet =
        db.query(
            "SELECT from Song  let $a=(SELECT @rid FROM Song WHERE author = 'Hunter')  where"
                + " SEARCH_More( $a, { 'minTermFreq':1, 'minDocFreq':1} ) = true")) {
      assertThat(resultSet).hasSize(229);
    }
  }
}
