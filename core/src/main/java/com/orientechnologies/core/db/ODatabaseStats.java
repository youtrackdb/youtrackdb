package com.orientechnologies.core.db;

import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import javax.annotation.Nonnull;

public class ODatabaseStats {

  public long loadedRecords;
  public long averageLoadRecordTimeMs;
  public long minLoadRecordTimeMs;
  public long maxLoadRecordTimeMs;

  public long prefetchedRidbagsCount;
  public long ridbagPrefetchTimeMs;
  public long minRidbagPrefetchTimeMs;
  public long maxRidbagPrefetchTimeMs;

  public YTResult toResult(@Nonnull YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);

    result.setProperty("loadedRecords", loadedRecords);
    result.setProperty("averageLoadRecordTimeMs", averageLoadRecordTimeMs);
    result.setProperty("minLoadRecordTimeMs", minLoadRecordTimeMs);
    result.setProperty("maxLoadRecordTimeMs", maxLoadRecordTimeMs);
    result.setProperty("prefetchedRidbagsCount", prefetchedRidbagsCount);
    result.setProperty("ridbagPrefetchTimeMs", ridbagPrefetchTimeMs);
    result.setProperty("minRidbagPrefetchTimeMs", minRidbagPrefetchTimeMs);
    result.setProperty("maxRidbagPrefetchTimeMs", maxRidbagPrefetchTimeMs);

    return result;
  }
}
