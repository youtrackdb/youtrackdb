/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.sql.functions.geo;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionAbstract;

/**
 * Haversine formula to compute the distance between 2 gro points.
 */
public class OSQLFunctionDistance extends OSQLFunctionAbstract {

  public static final String NAME = "distance";

  private static final double EARTH_RADIUS = 6371;

  public OSQLFunctionDistance() {
    super(NAME, 4, 5);
  }

  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      CommandContext iContext) {
    double distance;

    final double[] values = new double[4];

    for (int i = 0; i < iParams.length && i < 4; ++i) {
      if (iParams[i] == null) {
        return null;
      }

      values[i] = (Double) YTType.convert(iContext.getDatabase(), iParams[i],
          Double.class);
    }

    final double deltaLat = Math.toRadians(values[2] - values[0]);
    final double deltaLon = Math.toRadians(values[3] - values[1]);

    final double a =
        Math.pow(Math.sin(deltaLat / 2), 2)
            + Math.cos(Math.toRadians(values[0]))
            * Math.cos(Math.toRadians(values[2]))
            * Math.pow(Math.sin(deltaLon / 2), 2);
    distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

    if (iParams.length > 4) {
      final String unit = iParams[4].toString();
      if (unit.equalsIgnoreCase("km"))
        // ALREADY IN KM
        ;
      else if (unit.equalsIgnoreCase("mi"))
      // MILES
      {
        distance *= 0.621371192;
      } else if (unit.equalsIgnoreCase("nmi"))
      // NAUTICAL MILES
      {
        distance *= 0.539956803;
      } else {
        throw new IllegalArgumentException(
            "Unsupported unit '" + unit + "'. Use km, mi and nmi. Default is km.");
      }
    }

    return distance;
  }

  public String getSyntax(YTDatabaseSession session) {
    return "distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
  }
}
