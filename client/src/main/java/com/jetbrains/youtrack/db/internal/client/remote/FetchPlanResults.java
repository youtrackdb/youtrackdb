package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.internal.core.record.Record;
import java.util.Set;

public interface FetchPlanResults {

  Set<Record> getFetchedRecordsToSend();
}
