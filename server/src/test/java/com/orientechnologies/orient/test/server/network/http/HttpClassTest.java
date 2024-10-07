package com.orientechnologies.orient.test.server.network.http;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests HTTP "class" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpClassTest extends BaseHttpDatabaseTest {

  @Test
  public void testExistentClass() throws Exception {
    ClassicHttpResponse response = get("class/" + getDatabaseName() + "/OUser").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), response.getCode(), 200);
  }

  @Test
  public void testNonExistentClass() throws Exception {
    ClassicHttpResponse response =
        get("class/" + getDatabaseName() + "/NonExistentCLass").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), response.getCode(), 404);
  }

  @Test
  public void testCreateClass() throws Exception {
    ClassicHttpResponse response = post("class/" + getDatabaseName() + "/NewClass").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), response.getCode(), 201);
  }

  @Test
  public void testDropClass() throws Exception {
    ClassicHttpResponse response =
        post("class/" + getDatabaseName() + "/NewClassToDrop").getResponse();
    Assert.assertEquals(response.getReasonPhrase(), response.getCode(), 201);

    ClassicHttpResponse response1 =
        delete("class/" + getDatabaseName() + "/NewClassToDrop").getResponse();
    Assert.assertEquals(response1.getReasonPhrase(), response1.getCode(), 204);
  }

  @Override
  public String getDatabaseName() {
    return "httpclass";
  }
}
