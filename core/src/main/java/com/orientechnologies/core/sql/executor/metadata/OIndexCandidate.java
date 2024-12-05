package com.orientechnologies.core.sql.executor.metadata;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.List;
import java.util.Optional;

public interface OIndexCandidate {

  String getName();

  Optional<OIndexCandidate> invert();

  Operation getOperation();

  Optional<OIndexCandidate> normalize(OCommandContext ctx);

  List<YTProperty> properties();
}
