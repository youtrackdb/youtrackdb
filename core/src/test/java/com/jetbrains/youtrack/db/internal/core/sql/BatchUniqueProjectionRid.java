package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertNotEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandScript;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import org.junit.Test;

public class BatchUniqueProjectionRid extends DbTestBase {

  @Test
  public void testBatchUniqueRid() {
    List<List<EntityImpl>> res =
        session.command(
                new CommandScript(
                    "begin;let $a = select \"a\" as a ; let $b = select \"a\" as b; return"
                        + " [$a,$b] "))
            .execute(session);

    assertNotEquals(
        res.get(0).get(0).getIdentity().getClusterPosition(),
        res.get(1).get(0).getIdentity().getClusterPosition());

    //    assertEquals(1, res.get(0).get(0).getIdentity().getClusterPosition());
    //    assertEquals(2, res.get(1).get(0).getIdentity().getClusterPosition());
  }
}
