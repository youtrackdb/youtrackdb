/*
 *
 *  * Copyright 2014 Orient Technologies.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.lucene.analyzer.OLucenePerFieldAnalyzerWrapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class LuceneVsLuceneTest extends BaseLuceneTest {

  private IndexWriter indexWriter;
  private OLucenePerFieldAnalyzerWrapper analyzer;
  private Directory directory;

  @Before
  public void init() throws IOException {
    directory = NIOFSDirectory.open(getPath().toPath());

    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
      OFileUtils.deleteRecursively(getPath().getAbsoluteFile());

      analyzer = new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());
      analyzer.add("title", new StandardAnalyzer()).add("Song.title", new StandardAnalyzer());

      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      indexWriter = new IndexWriter(directory, iwc);

      db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE").close();
    }
  }

  private File getPath() {
    return new File(getDirectoryPath(getClass()));
  }

  @Test
  public void testLuceneVsLucene() throws IOException, ParseException {

    for (YTEntityImpl oDocument : db.browseClass("Song")) {

      String title = oDocument.field("title");
      if (title != null) {
        Document d = new Document();

        d.add(new TextField("title", title, Field.Store.YES));
        d.add(new TextField("Song.title", title, Field.Store.YES));
        indexWriter.addDocument(d);
      }
    }

    indexWriter.commit();
    indexWriter.close();

    try (IndexReader reader = DirectoryReader.open(directory)) {
      assertThat(reader.numDocs()).isEqualTo(Long.valueOf(db.countClass("Song")).intValue());

      IndexSearcher searcher = new IndexSearcher(reader);

      Query query = new MultiFieldQueryParser(new String[]{"title"}, analyzer).parse("down the");
      final TopDocs docs = searcher.search(query, Integer.MAX_VALUE);
      ScoreDoc[] hits = docs.scoreDocs;

      YTResultSet oDocs =
          db.query(
              "select *,$score from Song where title LUCENE \"down the\" order by $score desc");

      int i = 0;
      for (ScoreDoc hit : hits) {
        assertThat(oDocs.next().<Float>getProperty("$score")).isEqualTo(hit.score);
        i++;
      }
      Assert.assertEquals(i, hits.length);
    }
  }

  @After
  public void after() throws IOException {
    indexWriter.close();
    analyzer.close();

    directory.close();
  }
}
