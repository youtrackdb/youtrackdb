package com.jetbrains.youtrack.db.internal.core.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import org.junit.Test;

/**
 *
 */
public class RecordNotFoundExceptionTest {

  @Test
  public void simpleExceptionCopyTest() {
    YTRecordNotFoundException ex = new YTRecordNotFoundException(new YTRecordId(1, 2));
    YTRecordNotFoundException ex1 = new YTRecordNotFoundException(ex);
    assertNotNull(ex1.getRid());
    assertEquals(ex1.getRid().getClusterId(), 1);
    assertEquals(ex1.getRid().getClusterPosition(), 2);
  }
}
