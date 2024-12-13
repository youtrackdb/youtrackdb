package com.jetbrains.youtrack.db.internal.test.server.network.http;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests HTTP "class" command.
 */
public class HttpClassTest extends BaseHttpDatabaseTest {

  @Test
  public void testExistentClass() throws Exception {
    ClassicHttpResponse response = get("class/" + getDatabaseName() + "/OUser").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), 200, response.getCode());
  }

  @Test
  public void testNonExistentClass() throws Exception {
    ClassicHttpResponse response =
        get("class/" + getDatabaseName() + "/NonExistentCLass").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), 404, response.getCode());
  }

  @Test
  public void testCreateClass() throws Exception {
    ClassicHttpResponse response = post("class/" + getDatabaseName() + "/NewClass").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), 201, response.getCode());
  }

  @Test
  public void testDropClass() throws Exception {
    ClassicHttpResponse response =
        post("class/" + getDatabaseName() + "/NewClassToDrop").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), 201, response.getCode());

    ClassicHttpResponse response1 =
        delete("class/" + getDatabaseName() + "/NewClassToDrop").getResponse();
    Assert.assertEquals(response1.getReasonPhrase(), 204, response1.getCode());
  }

  @Override
  public String getDatabaseName() {
    return "httpclass";
  }
}
