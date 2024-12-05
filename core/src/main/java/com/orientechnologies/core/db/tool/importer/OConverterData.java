package com.orientechnologies.core.db.tool.importer;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.id.YTRID;
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
