package com.jetbrains.youtrack.db.internal.spatial;

import org.junit.Assert;
import org.junit.Test;

public class GeometryCollectionTest extends BaseSpatialLuceneTest {

  @Test
  public void testDeleteVerticesWithGeometryCollection() {
    session.command("CREATE CLASS TestInsert extends V").close();
    session.command("CREATE PROPERTY TestInsert.name STRING").close();
    session.command("CREATE PROPERTY TestInsert.geometry EMBEDDED OGeometryCollection").close();

    session.command(
            "CREATE INDEX TestInsert.geometry ON TestInsert(geometry) SPATIAL ENGINE LUCENE")
        .close();

    session.begin();
    session.command(
            "insert into TestInsert content {'name': 'loc1', 'geometry':"
                + " {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[0,10],[10,10],[10,0],[0,0]]]}]}}")
        .close();
    session.command(
            "insert into TestInsert content {'name': 'loc2', 'geometry':"
                + " {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[0,20],[20,20],[20,0],[0,0]]]}]}}")
        .close();
    session.commit();

    var qResult =
        session.command(
            "select * from TestInsert where ST_WITHIN(geometry,'POLYGON ((0 0, 15 0, 15 15, 0 15, 0"
                + " 0))') = true");
    Assert.assertEquals(1, qResult.stream().count());

    session.begin();
    session.command("DELETE VERTEX TestInsert").close();
    session.commit();

    var qResult2 = session.command("select * from TestInsert");
    Assert.assertEquals(0, qResult2.stream().count());
  }
}
