package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.record.Record;
import java.util.Set;

public interface OFetchPlanResults {

  Set<Record> getFetchedRecordsToSend();
}
