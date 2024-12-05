package com.orientechnologies.lucene.tests;

import static com.orientechnologies.lucene.functions.OLuceneFunctionsUtils.doubleEscape;
import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.InputStream;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneMetadataFieldsTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
  }

  @Test
  public void shouldFetchOnlyFromACluster() throws Exception {

    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Song.title")
            .getInternal()
            .size(db))
        .isEqualTo(585);

    int cluster = db.getMetadata().getSchema().getClass("Song").getClusterIds()[1];

    YTResultSet results =
        db.query("SELECT FROM Song WHERE search_class('+_CLUSTER:" + cluster + "')=true ");

    assertThat(results).hasSize(73);
    results.close();
  }

  @Test
  public void shouldFetchByRid() throws Exception {
    var songs = db.query("SELECT FROM Song limit 2").toList();

    String ridQuery = doubleEscape(songs.get(0).getRecordId() + " " + songs.get(1).getRecordId());
    YTResultSet results =
        db.query("SELECT FROM Song WHERE search_class('RID:(" + ridQuery + ") ')=true ");

    assertThat(results).hasSize(2);
    results.close();
  }
}
