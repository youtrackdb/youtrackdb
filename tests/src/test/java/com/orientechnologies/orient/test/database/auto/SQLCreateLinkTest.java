/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLCreateLinkTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public SQLCreateLinkTest(boolean remote) {
    super(remote);
  }

  @Test
  public void createLinktest() {
    database.command("CREATE CLASS POST").close();
    database.command("CREATE PROPERTY POST.comments LINKSET").close();

    database.begin();
    database.command("INSERT INTO POST (id, title) VALUES ( 10, 'NoSQL movement' )").close();
    database.command("INSERT INTO POST (id, title) VALUES ( 20, 'New YouTrackDB' )").close();

    database.command("INSERT INTO POST (id, title) VALUES ( 30, '(')").close();

    database.command("INSERT INTO POST (id, title) VALUES ( 40, ')')").close();
    database.commit();

    database.command("CREATE CLASS COMMENT").close();

    database.begin();
    database.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 0, 10, 'First' )").close();
    database.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 1, 10, 'Second' )").close();
    database.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 21, 10, 'Another' )").close();
    database
        .command("INSERT INTO COMMENT (id, postId, text) VALUES ( 41, 20, 'First again' )")
        .close();
    database
        .command("INSERT INTO COMMENT (id, postId, text) VALUES ( 82, 20, 'Second Again' )")
        .close();

    Assert.assertEquals(
        ((Number)
            database
                .command(
                    "CREATE LINK comments TYPE LINKSET FROM comment.postId TO post.id"
                        + " INVERSE")
                .next()
                .getProperty("count"))
            .intValue(),
        5);
    database.commit();

    database.begin();
    Assert.assertEquals(
        ((Number) database.command("UPDATE comment REMOVE postId").next().getProperty("count"))
            .intValue(),
        5);
    database.commit();
  }

  @Test
  public void createRIDLinktest() {

    database.command("CREATE CLASS POST2").close();
    database.command("CREATE PROPERTY POST2.comments LINKSET").close();

    database.begin();
    Object p1 =
        database
            .command("INSERT INTO POST2 (id, title) VALUES ( 10, 'NoSQL movement' )")
            .next()
            .toEntity();
    Assert.assertTrue(p1 instanceof YTEntityImpl);
    Object p2 =
        database
            .command("INSERT INTO POST2 (id, title) VALUES ( 20, 'New YouTrackDB' )")
            .next()
            .toEntity();
    Assert.assertTrue(p2 instanceof YTEntityImpl);

    Object p3 =
        database.command("INSERT INTO POST2 (id, title) VALUES ( 30, '(')").next().toEntity();
    Assert.assertTrue(p3 instanceof YTEntityImpl);

    Object p4 =
        database.command("INSERT INTO POST2 (id, title) VALUES ( 40, ')')").next().toEntity();
    Assert.assertTrue(p4 instanceof YTEntityImpl);
    database.commit();

    database.command("CREATE CLASS COMMENT2");

    database.begin();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 0, '"
                + ((YTEntityImpl) p1).getIdentity().toString()
                + "', 'First' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 1, '"
                + ((YTEntityImpl) p1).getIdentity().toString()
                + "', 'Second' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 21, '"
                + ((YTEntityImpl) p1).getIdentity().toString()
                + "', 'Another' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 41, '"
                + ((YTEntityImpl) p2).getIdentity().toString()
                + "', 'First again' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 82, '"
                + ((YTEntityImpl) p2).getIdentity().toString()
                + "', 'Second Again' )")
        .close();
    database.commit();

    database.begin();
    Assert.assertEquals(
        ((Number)
            database
                .command(
                    "CREATE LINK comments TYPE LINKSET FROM comment2.postId TO post2.id"
                        + " INVERSE")
                .next()
                .getProperty("count"))
            .intValue(),
        5);
    database.commit();

    database.begin();
    Assert.assertEquals(
        ((Number) database.command("UPDATE comment2 REMOVE postId").next().getProperty("count"))
            .intValue(),
        5);
    database.commit();
  }
}
