package com.jetbrains.youtrack.db.auto;

import static org.testng.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.RecordMetadata;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 11.03.13 12:00
 */
@Test
public class RecordMetadataTest extends BaseDBTest {

  @Parameters(value = "remote")
  public RecordMetadataTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  private static void assetORIDEquals(RID actual, RID expected) {
    assertEquals(actual.getClusterId(), expected.getClusterId());
    assertEquals(actual.getClusterPosition(), expected.getClusterPosition());
  }

  public void testGetRecordMetadata() {

    EntityImpl doc = new EntityImpl();
    for (int i = 0; i < 5; i++) {
      database.begin();
      if (!doc.getIdentity().isNew()) {
        doc = database.bindToSession(doc);
      }

      doc.field("field", i);
      database.save(doc, database.getClusterNameById(database.getDefaultClusterId()));
      database.commit();

      final RecordMetadata metadata = database.getRecordMetadata(doc.getIdentity());
      assetORIDEquals(doc.getIdentity(), metadata.getRecordId());
      assertEquals(database.bindToSession(doc).getVersion(), metadata.getVersion());
    }
  }
}
