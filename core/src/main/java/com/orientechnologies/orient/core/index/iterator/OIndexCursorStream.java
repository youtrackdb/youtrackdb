package com.orientechnologies.orient.core.index.iterator;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class OIndexCursorStream extends OIndexAbstractCursor {

  private final Iterator<ORawPair<Object, YTRID>> iterator;

  public OIndexCursorStream(final Stream<ORawPair<Object, YTRID>> stream) {
    iterator = stream.iterator();
  }

  @Override
  public Map.Entry<Object, YTIdentifiable> nextEntry() {
    if (iterator.hasNext()) {
      final ORawPair<Object, YTRID> pair = iterator.next();

      return new Map.Entry<Object, YTIdentifiable>() {
        @Override
        public Object getKey() {
          return pair.first;
        }

        @Override
        public YTIdentifiable getValue() {
          return pair.second;
        }

        @Override
        public YTIdentifiable setValue(YTIdentifiable value) {
          throw new UnsupportedOperationException();
        }
      };
    }

    return null;
  }
}
