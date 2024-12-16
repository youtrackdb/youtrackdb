package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.api.record.Record;
import java.util.Set;

public interface FetchPlanResults {

  Set<Record> getFetchedRecordsToSend();
}
