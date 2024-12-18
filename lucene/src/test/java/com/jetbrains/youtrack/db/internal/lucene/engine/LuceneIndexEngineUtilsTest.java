package com.jetbrains.youtrack.db.internal.lucene.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.lucene.test.BaseLuceneTest;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.lucene.search.SortField;
import org.junit.Test;

public class LuceneIndexEngineUtilsTest extends BaseLuceneTest {

  @Test
  public void buildSortFields() {
    var metadata = new HashMap<String, Object>();
    metadata.put(
        "sort",
        Collections.singletonList(
            ((EntityImpl) db.newEntity())
                .field("field", "score")
                .field("reverse", false)
                .field("type", "INT")
                .toMap()));

    final List<SortField> fields = LuceneIndexEngineUtils.buildSortFields(metadata);

    assertThat(fields).hasSize(1);
    final SortField sortField = fields.get(0);

    assertThat(sortField.getField()).isEqualTo("score");
    assertThat(sortField.getType()).isEqualTo(SortField.Type.INT);
    assertThat(sortField.getReverse()).isFalse();
  }

  @Test
  public void buildIntSortField() throws Exception {

    final EntityImpl sortConf =
        ((EntityImpl) db.newEntity()).field("field", "score").field("reverse", true)
            .field("type", "INT");

    final SortField sortField = LuceneIndexEngineUtils.buildSortField(sortConf);

    assertThat(sortField.getField()).isEqualTo("score");
    assertThat(sortField.getType()).isEqualTo(SortField.Type.INT);
    assertThat(sortField.getReverse()).isTrue();
  }

  @Test
  public void buildDocSortField() throws Exception {

    final EntityImpl sortConf = ((EntityImpl) db.newEntity()).field("type", "DOC");

    final SortField sortField = LuceneIndexEngineUtils.buildSortField(sortConf);

    assertThat(sortField.getField()).isNull();
    assertThat(sortField.getType()).isEqualTo(SortField.Type.DOC);
    assertThat(sortField.getReverse()).isFalse();
  }
}
