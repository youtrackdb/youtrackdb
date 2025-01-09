package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import java.util.Set;

public interface FetchPlanResults {

  Set<DBRecord> getFetchedRecordsToSend();
}
