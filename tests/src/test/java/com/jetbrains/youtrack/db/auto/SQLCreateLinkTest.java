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
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLCreateLinkTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLCreateLinkTest(boolean remote) {
    super(remote);
  }

  @Test
  public void createLinktest() {
    db.command("CREATE CLASS POST").close();
    db.command("CREATE PROPERTY POST.comments LINKSET").close();

    db.begin();
    db.command("INSERT INTO POST (id, title) VALUES ( 10, 'NoSQL movement' )").close();
    db.command("INSERT INTO POST (id, title) VALUES ( 20, 'New YouTrackDB' )").close();

    db.command("INSERT INTO POST (id, title) VALUES ( 30, '(')").close();

    db.command("INSERT INTO POST (id, title) VALUES ( 40, ')')").close();
    db.commit();

    db.command("CREATE CLASS COMMENT").close();

    db.begin();
    db.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 0, 10, 'First' )").close();
    db.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 1, 10, 'Second' )").close();
    db.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 21, 10, 'Another' )").close();
    db
        .command("INSERT INTO COMMENT (id, postId, text) VALUES ( 41, 20, 'First again' )")
        .close();
    db
        .command("INSERT INTO COMMENT (id, postId, text) VALUES ( 82, 20, 'Second Again' )")
        .close();

    Assert.assertEquals(
        ((Number)
            db
                .command(
                    "CREATE LINK comments TYPE LINKSET FROM comment.postId TO post.id"
                        + " INVERSE")
                .next()
                .getProperty("count"))
            .intValue(),
        5);
    db.commit();

    db.begin();
    Assert.assertEquals(
        ((Number) db.command("UPDATE comment REMOVE postId").next().getProperty("count"))
            .intValue(),
        5);
    db.commit();
  }

  @Test
  public void createRIDLinktest() {

    db.command("CREATE CLASS POST2").close();
    db.command("CREATE PROPERTY POST2.comments LINKSET").close();

    db.begin();
    Object p1 =
        db
            .command("INSERT INTO POST2 (id, title) VALUES ( 10, 'NoSQL movement' )")
            .next()
            .toEntity();
    Assert.assertTrue(p1 instanceof EntityImpl);
    Object p2 =
        db
            .command("INSERT INTO POST2 (id, title) VALUES ( 20, 'New YouTrackDB' )")
            .next()
            .toEntity();
    Assert.assertTrue(p2 instanceof EntityImpl);

    Object p3 =
        db.command("INSERT INTO POST2 (id, title) VALUES ( 30, '(')").next().toEntity();
    Assert.assertTrue(p3 instanceof EntityImpl);

    Object p4 =
        db.command("INSERT INTO POST2 (id, title) VALUES ( 40, ')')").next().toEntity();
    Assert.assertTrue(p4 instanceof EntityImpl);
    db.commit();

    db.command("CREATE CLASS COMMENT2");

    db.begin();
    db
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 0, '"
                + ((EntityImpl) p1).getIdentity().toString()
                + "', 'First' )")
        .close();
    db
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 1, '"
                + ((EntityImpl) p1).getIdentity().toString()
                + "', 'Second' )")
        .close();
    db
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 21, '"
                + ((EntityImpl) p1).getIdentity().toString()
                + "', 'Another' )")
        .close();
    db
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 41, '"
                + ((EntityImpl) p2).getIdentity().toString()
                + "', 'First again' )")
        .close();
    db
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 82, '"
                + ((EntityImpl) p2).getIdentity().toString()
                + "', 'Second Again' )")
        .close();
    db.commit();

    db.begin();
    Assert.assertEquals(
        ((Number)
            db
                .command(
                    "CREATE LINK comments TYPE LINKSET FROM comment2.postId TO post2.id"
                        + " INVERSE")
                .next()
                .getProperty("count"))
            .intValue(),
        5);
    db.commit();

    db.begin();
    Assert.assertEquals(
        ((Number) db.command("UPDATE comment2 REMOVE postId").next().getProperty("count"))
            .intValue(),
        5);
    db.commit();
  }
}
