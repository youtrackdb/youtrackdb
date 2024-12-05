package com.orientechnologies.spatial;

import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class GeometryCollectionTest extends BaseSpatialLuceneTest {

  @Test
  public void testDeleteVerticesWithGeometryCollection() {
    db.command("CREATE CLASS TestInsert extends V").close();
    db.command("CREATE PROPERTY TestInsert.name STRING").close();
    db.command("CREATE PROPERTY TestInsert.geometry EMBEDDED OGeometryCollection").close();

    db.command("CREATE INDEX TestInsert.geometry ON TestInsert(geometry) SPATIAL ENGINE LUCENE")
        .close();

    db.begin();
    db.command(
            "insert into TestInsert content {'name': 'loc1', 'geometry':"
                + " {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[0,10],[10,10],[10,0],[0,0]]]}]}}")
        .close();
    db.command(
            "insert into TestInsert content {'name': 'loc2', 'geometry':"
                + " {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[0,20],[20,20],[20,0],[0,0]]]}]}}")
        .close();
    db.commit();

    YTResultSet qResult =
        db.command(
            "select * from TestInsert where ST_WITHIN(geometry,'POLYGON ((0 0, 15 0, 15 15, 0 15, 0"
                + " 0))') = true");
    Assert.assertEquals(1, qResult.stream().count());

    db.begin();
    db.command("DELETE VERTEX TestInsert").close();
    db.commit();

    YTResultSet qResult2 = db.command("select * from TestInsert");
    Assert.assertEquals(0, qResult2.stream().count());
  }
}
