/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.jetbrains.youtrack.db.internal.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.lucene.test.BaseLuceneTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialQueryIntegrationTest extends BaseLuceneTest {

  @Test
  public void testIssueGH9105() {

    session.command("create class Country extends V").close();
    session.command("create property Country.name STRING").close();
    session.command("create property Country.geometry EMBEDDED OMultiPolygon").close();
    session.command("create class POI extends V").close();
    session.command("create property POI.name STRING").close();
    session.command("create property POI.location EMBEDDED OPoint").close();

    session.begin();
    session.command(
            "insert into POI(name, location) values(\"zeropoint\", St_GeomFromText(\"Point(0"
                + " 0)\"))")
        .close();
    session.command(
            "insert into Country(name, geometry) values(\"zeroland\","
                + " St_GeomFromText(\"MultiPolygon(((1 1, 1 -1, -1 -1, -1 1, 1 1)))\"))")
        .close();
    session.commit();

    session.command("CREATE INDEX POI.location ON POI(location) SPATIAL ENGINE LUCENE");
    session.command("CREATE INDEX Country.geometry ON Country(geometry) SPATIAL ENGINE LUCENE;");

    try (var resultSet =
        session.query(
            "select name from Country let locations = (select from Poi) where ST_Contains(geometry,"
                + " $locations[0].location) = true")) {

      assertThat(resultSet.stream().count()).isEqualTo(1);
    }

    try (var resultSet =
        session.query(
            "select name from Country where ST_Contains(geometry, (select location from POI)) ="
                + " true;")) {

      assertThat(resultSet.stream().count()).isEqualTo(1);
    }

    try (var resultSet =
        session.query(
            "select name from Country where ST_Contains(geometry, (select name,location from POI))"
                + " = true;")) {

      assertThat(resultSet.stream().count()).isEqualTo(0);
    }

    session.begin();
    session.command(
            "insert into POI(name, location) values(\"zeropoint\", St_GeomFromText(\"Point(0"
                + " 0)\"))")
        .close();
    session.commit();

    try (var resultSet =
        session.query(
            "select name from Country where ST_Contains(geometry, (select location from POI)) ="
                + " true;")) {

      Assert.fail("It should throw an exception");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof CommandExecutionException);
    }

    session.begin();
    session.command("delete vertex Poi").close();
    session.commit();

    try (var resultSet =
        session.query(
            "select name from Country where ST_Contains(geometry, (select location from POI)) ="
                + " true;")) {

      assertThat(resultSet.stream().count()).isEqualTo(0);
    }
  }
}
