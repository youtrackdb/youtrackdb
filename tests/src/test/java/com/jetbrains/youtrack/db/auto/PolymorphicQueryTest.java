/*
 *
 *  *  Copyright YouTrackDB
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.common.profiler.Profiler;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 *
 */
public class PolymorphicQueryTest extends BaseDBTest {

  @Parameters(value = "remote")
  public PolymorphicQueryTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    db.command("create class IndexInSubclassesTestBase").close();
    db.command("create property IndexInSubclassesTestBase.name string").close();

    db
        .command("create class IndexInSubclassesTestChild1 extends IndexInSubclassesTestBase")
        .close();
    db
        .command(
            "create index IndexInSubclassesTestChild1.name on IndexInSubclassesTestChild1 (name)"
                + " notunique")
        .close();

    db
        .command("create class IndexInSubclassesTestChild2 extends IndexInSubclassesTestBase")
        .close();
    db
        .command(
            "create index IndexInSubclassesTestChild2.name on IndexInSubclassesTestChild2 (name)"
                + " notunique")
        .close();

    db.command("create class IndexInSubclassesTestBaseFail").close();
    db.command("create property IndexInSubclassesTestBaseFail.name string").close();

    db
        .command(
            "create class IndexInSubclassesTestChild1Fail extends IndexInSubclassesTestBaseFail")
        .close();
    // database.command(
    // new CommandSQL("create index IndexInSubclassesTestChild1Fail.name on
    // IndexInSubclassesTestChild1Fail (name) notunique"))
    // .execute();

    db
        .command(
            "create class IndexInSubclassesTestChild2Fail extends IndexInSubclassesTestBaseFail")
        .close();
    db
        .command(
            "create index IndexInSubclassesTestChild2Fail.name on IndexInSubclassesTestChild2Fail"
                + " (name) notunique")
        .close();

    db.command("create class GenericCrash").close();
    db.command("create class SpecificCrash extends GenericCrash").close();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    db.command("delete from IndexInSubclassesTestBase").close();
    db.command("delete from IndexInSubclassesTestChild1").close();
    db.command("delete from IndexInSubclassesTestChild2").close();

    db.command("delete from IndexInSubclassesTestBaseFail").close();
    db.command("delete from IndexInSubclassesTestChild1Fail").close();
    db.command("delete from IndexInSubclassesTestChild2Fail").close();
  }

  @Test
  public void testSubclassesIndexes() throws Exception {
    db.begin();

    var profiler = YouTrackDBEnginesManager.instance().getProfiler();

    var indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    var indexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");

    if (indexUsage < 0) {
      indexUsage = 0;
    }

    if (indexUsageReverted < 0) {
      indexUsageReverted = 0;
    }

    profiler.startRecording();
    for (var i = 0; i < 10000; i++) {

      final var doc1 = ((EntityImpl) db.newEntity("IndexInSubclassesTestChild1"));
      doc1.field("name", "name" + i);
      doc1.save();

      final var doc2 = ((EntityImpl) db.newEntity("IndexInSubclassesTestChild2"));
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        db.commit();
      }
    }
    db.commit();

    List<EntityImpl> result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from IndexInSubclassesTestBase where name > 'name9995' and name <"
                    + " 'name9999' order by name ASC"));
    Assert.assertEquals(result.size(), 6);
    String lastName = result.get(0).field("name");

    for (var i = 1; i < result.size(); i++) {
      var current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) <= 0);
      lastName = currentName;
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), indexUsage + 2);
    var reverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");
    Assert.assertEquals(reverted < 0 ? 0 : reverted, indexUsageReverted);

    result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from IndexInSubclassesTestBase where name > 'name9995' and name <"
                    + " 'name9999' order by name DESC"));
    Assert.assertEquals(result.size(), 6);
    lastName = result.get(0).field("name");
    for (var i = 1; i < result.size(); i++) {
      var current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) >= 0);
      lastName = currentName;
    }
    profiler.stopRecording();
  }

  @Test
  public void testBaseWithoutIndexAndSubclassesIndexes() throws Exception {
    db.begin();

    var profiler = YouTrackDBEnginesManager.instance().getProfiler();

    var indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    var indexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");

    if (indexUsage < 0) {
      indexUsage = 0;
    }

    if (indexUsageReverted < 0) {
      indexUsageReverted = 0;
    }

    profiler.startRecording();
    for (var i = 0; i < 10000; i++) {
      final var doc0 = ((EntityImpl) db.newEntity("IndexInSubclassesTestBase"));
      doc0.field("name", "name" + i);
      doc0.save();

      final var doc1 = ((EntityImpl) db.newEntity("IndexInSubclassesTestChild1"));
      doc1.field("name", "name" + i);
      doc1.save();

      final var doc2 = ((EntityImpl) db.newEntity("IndexInSubclassesTestChild2"));
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        db.commit();
      }
    }
    db.commit();

    List<EntityImpl> result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from IndexInSubclassesTestBase where name > 'name9995' and name <"
                    + " 'name9999' order by name ASC"));
    Assert.assertEquals(result.size(), 9);
    String lastName = result.get(0).field("name");
    for (var i = 1; i < result.size(); i++) {
      var current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) <= 0);
      lastName = currentName;
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), indexUsage + 2);

    var reverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");
    Assert.assertEquals(reverted < 0 ? 0 : reverted, indexUsageReverted);

    result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from IndexInSubclassesTestBase where name > 'name9995' and name <"
                    + " 'name9999' order by name DESC"));
    Assert.assertEquals(result.size(), 9);
    lastName = result.get(0).field("name");
    for (var i = 1; i < result.size(); i++) {
      var current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) >= 0);
      lastName = currentName;
    }
    profiler.stopRecording();
  }

  @Test
  public void testSubclassesIndexesFailed() throws Exception {
    db.begin();

    var profiler = YouTrackDBEnginesManager.instance().getProfiler();
    profiler.startRecording();

    for (var i = 0; i < 10000; i++) {

      final var doc1 = ((EntityImpl) db.newEntity("IndexInSubclassesTestChild1Fail"));
      doc1.field("name", "name" + i);
      doc1.save();

      final var doc2 = ((EntityImpl) db.newEntity("IndexInSubclassesTestChild2Fail"));
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        db.commit();
      }
    }
    db.commit();

    var indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    var indexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");

    if (indexUsage < 0) {
      indexUsage = 0;
    }

    if (indexUsageReverted < 0) {
      indexUsageReverted = 0;
    }

    var result =
        db.query(
            "select from IndexInSubclassesTestBaseFail where name > 'name9995' and name <"
                + " 'name9999' order by name ASC");
    Assert.assertEquals(result.stream().count(), 6);

    var lastIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    var lastIndexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");
    if (lastIndexUsage < 0) {
      lastIndexUsage = 0;
    }

    if (lastIndexUsageReverted < 0) {
      lastIndexUsageReverted = 0;
    }

    Assert.assertEquals(lastIndexUsage - indexUsage, lastIndexUsageReverted - indexUsageReverted);

    profiler.stopRecording();
  }

  @Test
  public void testIteratorOnSubclassWithoutValues() {
    for (var i = 0; i < 2; i++) {
      final var doc1 = ((EntityImpl) db.newEntity("GenericCrash"));
      db.begin();
      doc1.field("name", "foo");
      doc1.save();
    }

    // crashed with YTIOException, issue #3632
    var result =
        db.query("SELECT FROM GenericCrash WHERE @class='GenericCrash' ORDER BY @rid DESC");

    var count = 0;
    while (result.hasNext()) {
      var doc = result.next();
      Assert.assertEquals(doc.getProperty("name"), "foo");
      count++;
    }
    Assert.assertEquals(count, 2);
  }
}
