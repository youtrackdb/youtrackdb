package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 *
 */
public class RemoteErrorMessageTest extends DbTestBase {

  @Test
  public void testReadWriteErrorMessage() throws IOException {
    MockChannel channel = new MockChannel();
    Map<String, String> messages = new HashMap<>();
    messages.put("one", "two");
    OError37Response response =
        new OError37Response(ErrorCode.GENERIC_ERROR, 10, messages, "some".getBytes());
    response.write(null, channel, 0, null);
    channel.close();
    OError37Response readResponse = new OError37Response();
    readResponse.read(db, channel, null);

    assertEquals(readResponse.getCode(), ErrorCode.GENERIC_ERROR);
    assertEquals(readResponse.getErrorIdentifier(), 10);
    assertNotNull(readResponse.getMessages());
    assertEquals(readResponse.getMessages().get("one"), "two");
    assertEquals(new String(readResponse.getVerbose()), "some");
  }
}
