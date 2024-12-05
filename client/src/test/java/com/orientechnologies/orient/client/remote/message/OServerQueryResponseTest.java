package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OServerQueryResponseTest extends DBTestBase {

  @Test
  public void test() throws IOException {

    List<YTResult> resuls = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      YTResultInternal item = new YTResultInternal(null);
      item.setProperty("name", "foo");
      item.setProperty("counter", i);
      resuls.add(item);
    }
    OServerQueryResponse response =
        new OServerQueryResponse(
            "query", true, resuls, Optional.empty(), false, new HashMap<>(), true);

    MockChannel channel = new MockChannel();
    response.write(null,
        channel,
        OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION,
        ORecordSerializerNetworkFactory.INSTANCE.current());

    channel.close();

    OServerQueryResponse newResponse = new OServerQueryResponse();

    newResponse.read(db, channel, null);
    Iterator<YTResult> responseRs = newResponse.getResult().iterator();

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(responseRs.hasNext());
      YTResult item = responseRs.next();
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals((Integer) i, item.getProperty("counter"));
    }
    Assert.assertFalse(responseRs.hasNext());
    Assert.assertTrue(newResponse.isReloadMetadata());
    Assert.assertTrue(newResponse.isTxChanges());
  }
}
