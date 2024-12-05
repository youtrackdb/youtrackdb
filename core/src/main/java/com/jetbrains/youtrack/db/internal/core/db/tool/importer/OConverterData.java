package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.util.Set;

/**
 *
 */
public class OConverterData {

  protected YTDatabaseSession session;
  protected Set<YTRID> brokenRids;

  public OConverterData(YTDatabaseSession session, Set<YTRID> brokenRids) {
    this.session = session;
    this.brokenRids = brokenRids;
  }
}
