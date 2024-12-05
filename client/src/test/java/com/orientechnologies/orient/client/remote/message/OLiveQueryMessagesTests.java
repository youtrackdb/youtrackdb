package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.exception.OErrorCode;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 *
 */
public class OLiveQueryMessagesTests extends DBTestBase {

  @Test
  public void testRequestWriteRead() throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("par", "value");
    OSubscribeLiveQueryRequest request = new OSubscribeLiveQueryRequest("select from Some", params);
    MockChannel channel = new MockChannel();
    request.write(null, channel, null);
    channel.close();
    OSubscribeLiveQueryRequest requestRead = new OSubscribeLiveQueryRequest();
    requestRead.read(db, channel, -1, new ORecordSerializerNetworkV37());
    assertEquals("select from Some", requestRead.getQuery());
    assertEquals(requestRead.getParams(), params);
  }

  @Test
  public void testSubscribeResponseWriteRead() throws IOException {
    OSubscribeLiveQueryResponse response = new OSubscribeLiveQueryResponse(20);
    MockChannel channel = new MockChannel();
    response.write(null, channel, 0, null);
    channel.close();
    OSubscribeLiveQueryResponse responseRead = new OSubscribeLiveQueryResponse();
    responseRead.read(db, channel, null);
    assertEquals(20, responseRead.getMonitorId());
  }

  @Test
  public void testLiveQueryErrorPushRequest() throws IOException {

    OLiveQueryPushRequest pushRequest =
        new OLiveQueryPushRequest(10, 20, OErrorCode.GENERIC_ERROR, "the message");
    MockChannel channel = new MockChannel();
    pushRequest.write(null, channel);
    channel.close();
    OLiveQueryPushRequest pushRequestRead = new OLiveQueryPushRequest();
    pushRequestRead.read(db, channel);
    assertEquals(10, pushRequestRead.getMonitorId());
    assertEquals(OLiveQueryPushRequest.ERROR, pushRequestRead.getStatus());
    assertEquals(20, pushRequestRead.getErrorIdentifier());
    assertEquals(OErrorCode.GENERIC_ERROR, pushRequestRead.getErrorCode());
    assertEquals("the message", pushRequestRead.getErrorMessage());
  }

  @Test
  public void testLiveQueryPushRequest() throws IOException {

    List<OLiveQueryResult> events = new ArrayList<>();
    YTResultInternal res = new YTResultInternal(db);
    res.setProperty("one", "one");
    res.setProperty("two", 10);
    events.add(new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, res, null));
    events.add(
        new OLiveQueryResult(
            OLiveQueryResult.UPDATE_EVENT, new YTResultInternal(db), new YTResultInternal(db)));
    events.add(new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, new YTResultInternal(db), null));

    OLiveQueryPushRequest pushRequest =
        new OLiveQueryPushRequest(10, OLiveQueryPushRequest.END, events);
    MockChannel channel = new MockChannel();
    pushRequest.write(null, channel);
    channel.close();
    OLiveQueryPushRequest pushRequestRead = new OLiveQueryPushRequest();
    pushRequestRead.read(db, channel);

    assertEquals(10, pushRequestRead.getMonitorId());
    assertEquals(OLiveQueryPushRequest.END, pushRequestRead.getStatus());
    assertEquals(3, pushRequestRead.getEvents().size());
    assertEquals("one", pushRequestRead.getEvents().get(0).getCurrentValue().getProperty("one"));
    assertEquals(10, (int) pushRequestRead.getEvents().get(0).getCurrentValue().getProperty("two"));
  }
}
