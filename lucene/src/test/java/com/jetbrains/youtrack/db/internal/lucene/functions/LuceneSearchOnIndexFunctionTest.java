package com.jetbrains.youtrack.db.internal.lucene.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.lucene.test.BaseLuceneTest;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSearchOnIndexFunctionTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    //    db.execute("sql", getScriptFromStream(stream)).close();
    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");
    db.command("create index Author.name on Author (name) FULLTEXT ENGINE LUCENE ");
    db.command(
        "create index Song.lyrics_description on Song (lyrics,description) FULLTEXT ENGINE LUCENE"
            + " ");
  }

  @Test
  public void shouldSearchOnSingleIndex() throws Exception {

    ResultSet resultSet =
        db.query("SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE') = true");

    //    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(2);

    resultSet.close();

    resultSet = db.query("SELECT from Song where SEARCH_INDEX('Song.title', \"bel*\") = true");

    assertThat(resultSet).hasSize(3);
    resultSet.close();

    resultSet = db.query("SELECT from Song where SEARCH_INDEX('Song.title', 'bel*') = true");

    assertThat(resultSet).hasSize(3);

    resultSet.close();
  }

  @Test
  public void shouldFindNothingOnEmptyQuery() throws Exception {

    ResultSet resultSet = db.query(
        "SELECT from Song where SEARCH_INDEX('Song.title', '') = true");

    //    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(0);

    resultSet.close();
  }

  @Test
  //  @Ignore
  public void shouldSearchOnSingleIndexWithLeadingWildcard() throws Exception {

    // TODO: metadata still not used
    ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', '*EVE*', {'allowLeadingWildcard':"
                + " true}) = true");

    //    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(14);

    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoIndexesInOR() throws Exception {

    ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE') = true OR"
                + " SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(41);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoIndexesInAND() throws Exception {

    ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'tambourine') = true AND"
                + " SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoIndexesWithLeadingWildcardInAND() throws Exception {

    ResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'tambourine') = true AND"
                + " SEARCH_INDEX('Song.author', 'Bob', {'allowLeadingWildcard': true}) = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test(expected = CommandExecutionException.class)
  public void shouldFailWithWrongIndexName() throws Exception {

    db.query("SELECT from Song where SEARCH_INDEX('Song.wrongName', 'tambourine') = true ").close();
  }

  @Test
  public void shouldSupportParameterizedMetadata() throws Exception {
    final String query = "SELECT from Song where SEARCH_INDEX('Song.title', '*EVE*', ?) = true";

    db.query(query, "{'allowLeadingWildcard': true}").close();
    db.query(query, new EntityImpl("allowLeadingWildcard", Boolean.TRUE)).close();

    Map<String, Object> mdMap = new HashMap();
    mdMap.put("allowLeadingWildcard", true);
    db.query(query, new Object[]{mdMap}).close();
  }
}
