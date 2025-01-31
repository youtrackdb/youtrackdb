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
public class DBRecordMetadataTest extends BaseDBTest {

  @Parameters(value = "remote")
  public DBRecordMetadataTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  private static void assetORIDEquals(RID actual, RID expected) {
    assertEquals(actual.getClusterId(), expected.getClusterId());
    assertEquals(actual.getClusterPosition(), expected.getClusterPosition());
  }

  public void testGetRecordMetadata() {

    EntityImpl doc = ((EntityImpl) db.newEntity());
    for (int i = 0; i < 5; i++) {
      db.begin();
      if (!doc.getIdentity().isNew()) {
        doc = db.bindToSession(doc);
      }

      doc.field("field", i);
      db.save(doc);
      db.commit();

      final RecordMetadata metadata = db.getRecordMetadata(doc.getIdentity());
      assetORIDEquals(doc.getIdentity(), metadata.getRecordId());
      assertEquals(db.bindToSession(doc).getVersion(), metadata.getVersion());
    }
  }
}
