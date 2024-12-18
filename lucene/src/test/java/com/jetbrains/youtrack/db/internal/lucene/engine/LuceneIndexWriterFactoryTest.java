package com.jetbrains.youtrack.db.internal.lucene.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.lucene.engine.LuceneIndexWriterFactory;
import com.jetbrains.youtrack.db.internal.lucene.test.BaseLuceneTest;
import java.io.File;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

/**
 *
 */
public class LuceneIndexWriterFactoryTest extends BaseLuceneTest {

  @Test
  public void shouldCreateIndexWriterConfiguredWithMetadataValues() throws Exception {

    LuceneIndexWriterFactory fc = new LuceneIndexWriterFactory();

    // sample metadata json
    var meta = ((EntityImpl) db.newEntity());
    meta.fromJSON(
        IOUtils.readFileAsString(
            new File("./src/test/resources/index_metadata_new.json")));

    IndexWriter writer = fc.createIndexWriter(new RAMDirectory(), meta.toMap(),
        new StandardAnalyzer());

    LiveIndexWriterConfig config = writer.getConfig();
    assertThat(config.getUseCompoundFile()).isFalse();

    assertThat(config.getAnalyzer()).isInstanceOf(StandardAnalyzer.class);

    assertThat(config.getMaxBufferedDocs()).isEqualTo(-1);

    assertThat(config.getRAMPerThreadHardLimitMB()).isEqualTo(1024);
  }
}
