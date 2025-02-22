package com.jetbrains.youtrack.db.internal.test.server.network.http;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class HttpImportTest extends BaseHttpDatabaseTest {

  @Test
  @Ignore
  public void testImport() throws IOException {
    String content =
        "{\"records\": [{\"@type\": \"d\", \"@rid\": \"#9:0\",\"@version\": 1,\"@class\": \"V\"}]}";
    post("import/" + getDatabaseName() + "?merge=true").payload(content, CONTENT.TEXT);
    var response = getResponse();
    assertEquals(response.getReasonPhrase(), 200, response.getCode());

    InputStream is = response.getEntity().getContent();
    List<String> out = new LinkedList<>();
    BufferedReader r = new BufferedReader(new InputStreamReader(is));

    try {
      String line;
      while ((line = r.readLine()) != null) {
        out.add(line);
      }

      System.out.println(out);
    } catch (IOException var5) {
      throw new IllegalArgumentException("Problems reading from: " + is, var5);
    }
  }

  @Override
  protected String getDatabaseName() {
    return this.getClass().getSimpleName();
  }
}
