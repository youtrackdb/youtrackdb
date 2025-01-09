package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal;

import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.RecordOperationMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationMetadata;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AtomicUnitEndDBRecordTest {

  @Test
  public void recordMetadataSerializationTest() {
    RecordOperationMetadata recordOperationMetadata = new RecordOperationMetadata();
    recordOperationMetadata.addRid(new RecordId(10, 42));
    recordOperationMetadata.addRid(new RecordId(42, 10));

    Map<String, AtomicOperationMetadata<?>> metadata = new LinkedHashMap<>();
    metadata.put(recordOperationMetadata.getKey(), recordOperationMetadata);

    AtomicUnitEndRecord atomicUnitEndRecord = new AtomicUnitEndRecord(1, false, metadata);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    byte[] content = new byte[arraySize];

    final int endOffset = atomicUnitEndRecord.toStream(content, 1);
    Assert.assertEquals(endOffset, content.length);

    AtomicUnitEndRecord atomicUnitEndRecordD = new AtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content, 1);
    Assert.assertEquals(dEndOffset, content.length);

    Assert.assertEquals(
        atomicUnitEndRecordD.getOperationUnitId(), atomicUnitEndRecord.getOperationUnitId());
    RecordOperationMetadata recordOperationMetadataD =
        (RecordOperationMetadata)
            atomicUnitEndRecordD
                .getAtomicOperationMetadata()
                .get(RecordOperationMetadata.RID_METADATA_KEY);

    Assert.assertEquals(recordOperationMetadataD.getValue(), recordOperationMetadata.getValue());
  }

  @Test
  public void recordNoMetadataSerializationTest() {
    AtomicUnitEndRecord atomicUnitEndRecord = new AtomicUnitEndRecord(1, false, null);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    byte[] content = new byte[arraySize];

    final int endOffset = atomicUnitEndRecord.toStream(content, 1);
    Assert.assertEquals(endOffset, content.length);

    AtomicUnitEndRecord atomicUnitEndRecordD = new AtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content, 1);
    Assert.assertEquals(dEndOffset, content.length);
  }
}
