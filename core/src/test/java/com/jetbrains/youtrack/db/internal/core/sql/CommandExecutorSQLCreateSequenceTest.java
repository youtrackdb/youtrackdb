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
package com.jetbrains.youtrack.db.internal.core.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.List;
import org.junit.Test;

public class CommandExecutorSQLCreateSequenceTest extends DbTestBase {

  @Test
  public void testSimple() {
    db.command("CREATE SEQUENCE Sequence1 TYPE ORDERED").close();

    List<Result> results =
        db.query("select sequence('Sequence1').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(1L);
    }
    results =
        db.query("select sequence('Sequence1').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(2L);
    }
    results =
        db.query("select sequence('Sequence1').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(3L);
    }
  }

  @Test
  public void testIncrement() {
    db.command("CREATE SEQUENCE SequenceIncrement TYPE ORDERED INCREMENT 3").close();
    var results =
        db.query("select sequence('SequenceIncrement').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(3L);
    }
    results =
        db.query("select sequence('SequenceIncrement').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(6L);
    }
    results =
        db.query("select sequence('SequenceIncrement').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(9L);
    }
  }

  @Test
  public void testStart() {
    db.command("CREATE SEQUENCE SequenceStart TYPE ORDERED START 3").close();

    var results =
        db.query("select sequence('SequenceStart').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(4L);
    }
    results =
        db.query("select sequence('SequenceStart').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(5L);
    }
    results =
        db.query("select sequence('SequenceStart').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(6L);
    }
  }

  @Test
  public void testStartIncrement() {
    db.command("CREATE SEQUENCE SequenceStartIncrement TYPE ORDERED START 3 INCREMENT 10").close();

    var results =
        db.query("select sequence('SequenceStartIncrement').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(13L);
    }
    results =
        db.query("select sequence('SequenceStartIncrement').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(23L);
    }
    results =
        db.query("select sequence('SequenceStartIncrement').next() as val").toList();
    assertEquals(1, results.size());
    for (var result : results) {
      assertThat(result.<Long>getProperty("val")).isEqualTo(33L);
    }
  }
}
