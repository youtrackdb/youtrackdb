package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import java.util.Set;

/**
 *
 */
public class ConverterData {

  protected DatabaseSession session;
  protected Set<RID> brokenRids;

  public ConverterData(DatabaseSession session, Set<RID> brokenRids) {
    this.session = session;
    this.brokenRids = brokenRids;
  }
}
