package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertNotEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.command.script.OCommandScript;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import org.junit.Test;

public class BatchUniqueProjectionRid extends DBTestBase {

  @Test
  public void testBatchUniqueRid() {
    List<List<EntityImpl>> res =
        db.command(
                new OCommandScript(
                    "begin;let $a = select \"a\" as a ; let $b = select \"a\" as b; return"
                        + " [$a,$b] "))
            .execute(db);

    assertNotEquals(
        res.get(0).get(0).getIdentity().getClusterPosition(),
        res.get(1).get(0).getIdentity().getClusterPosition());

    //    assertEquals(1, res.get(0).get(0).getIdentity().getClusterPosition());
    //    assertEquals(2, res.get(1).get(0).getIdentity().getClusterPosition());
  }
}
