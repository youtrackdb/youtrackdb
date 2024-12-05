package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.RidBag;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class DepthFirstTraverseStep extends AbstractTraverseStep {

  public DepthFirstTraverseStep(
      List<OTraverseProjectionItem> projections,
      OWhereClause whileClause,
      OInteger maxDepth,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(projections, whileClause, maxDepth, ctx, profilingEnabled);
  }

  @Override
  protected void fetchNextEntryPoints(
      OExecutionStream nextN, OCommandContext ctx, List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    // Doing max batch of 100 entry points for now
    while (nextN.hasNext(ctx) && entryPoints.size() < 100) {
      YTResult item = toTraverseResult(ctx.getDatabase(), nextN.next(ctx));
      if (item == null) {
        continue;
      }
      ((YTResultInternal) item).setMetadata("$depth", 0);

      List stack = new ArrayList();
      item.getIdentity().ifPresent(x -> stack.add(x));
      ((YTResultInternal) item).setMetadata("$stack", stack);

      List<YTIdentifiable> path = new ArrayList<>();
      if (item.getIdentity().isPresent()) {
        path.add(item.getIdentity().get());
      } else if (item.getProperty("@rid") != null) {
        path.add(item.getProperty("@rid"));
      }
      ((YTResultInternal) item).setMetadata("$path", path);

      if (item.isEntity() && !traversed.contains(item.getEntity().get().getIdentity())) {
        tryAddEntryPointAtTheEnd(item, ctx, entryPoints, traversed);
        traversed.add(item.getEntity().get().getIdentity());
      } else if (item.getProperty("@rid") != null
          && item.getProperty("@rid") instanceof YTIdentifiable) {
        tryAddEntryPointAtTheEnd(item, ctx, entryPoints, traversed);
        traversed.add(((YTIdentifiable) item.getProperty("@rid")).getIdentity());
      }
    }
  }

  private YTResult toTraverseResult(YTDatabaseSessionInternal db, YTResult item) {
    YTTraverseResult res = null;
    if (item instanceof YTTraverseResult) {
      res = (YTTraverseResult) item;
    } else if (item.isEntity() && item.getEntity().get().getIdentity().isValid()) {
      res = new YTTraverseResult(db, item.getEntity().get());
      res.depth = 0;
    } else if (item.getPropertyNames().size() == 1) {
      Object val = item.getProperty(item.getPropertyNames().iterator().next());
      if (val instanceof YTIdentifiable) {
        res = new YTTraverseResult(db, (YTIdentifiable) val);
        res.depth = 0;
        res.setMetadata("$depth", 0);
      }
    } else {
      res = new YTTraverseResult(db);
      for (String key : item.getPropertyNames()) {
        res.setProperty(key, convert(item.getProperty(key)));
      }
      for (String md : item.getMetadataKeys()) {
        res.setMetadata(md, item.getMetadata(md));
      }
    }

    return res;
  }

  public Object convert(Object value) {
    if (value instanceof RidBag) {
      List result = new ArrayList();
      ((RidBag) value).forEach(x -> result.add(x));
      return result;
    }
    return value;
  }

  @Override
  protected void fetchNextResults(
      OCommandContext ctx, List<YTResult> results, List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (!entryPoints.isEmpty()) {
      YTTraverseResult item = (YTTraverseResult) entryPoints.remove(0);
      results.add(item);
      for (OTraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        Integer depth = item.depth != null ? item.depth : (Integer) item.getMetadata("$depth");
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > depth) {
          addNextEntryPoints(
              nextStep,
              depth + 1,
              (List) item.getMetadata("$path"),
              (List) item.getMetadata("$stack"),
              ctx,
              entryPoints,
              traversed);
        }
      }
    }
  }

  private void addNextEntryPoints(
      Object nextStep,
      int depth,
      List<YTIdentifiable> path,
      List<YTIdentifiable> stack,
      OCommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (nextStep instanceof YTIdentifiable) {
      addNextEntryPoint(
          ((YTIdentifiable) nextStep), depth, path, stack, ctx, entryPoints, traversed);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(
          ((Iterable) nextStep).iterator(), depth, path, stack, ctx, entryPoints, traversed);
    } else if (nextStep instanceof Map) {
      addNextEntryPoints(
          ((Map) nextStep).values().iterator(), depth, path, stack, ctx, entryPoints, traversed);
    } else if (nextStep instanceof YTResult) {
      addNextEntryPoint(((YTResult) nextStep), depth, path, stack, ctx, entryPoints, traversed);
    }
  }

  private void addNextEntryPoints(
      Iterator nextStep,
      int depth,
      List<YTIdentifiable> path,
      List<YTIdentifiable> stack,
      OCommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, path, stack, ctx, entryPoints, traversed);
    }
  }

  private void addNextEntryPoint(
      YTIdentifiable nextStep,
      int depth,
      List<YTIdentifiable> path,
      List<YTIdentifiable> stack,
      OCommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (traversed.contains(nextStep.getIdentity())) {
      return;
    }
    YTTraverseResult res = new YTTraverseResult(ctx.getDatabase(), nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    List<YTIdentifiable> newPath = new ArrayList<>(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    List newStack = new ArrayList();
    newStack.add(res.getIdentity().get());
    newStack.addAll(stack);
    //    for (int i = 0; i < newPath.size(); i++) {
    //      newStack.offerLast(newPath.get(i));
    //    }
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, ctx, entryPoints, traversed);
  }

  private void addNextEntryPoint(
      YTResult nextStep,
      int depth,
      List<YTIdentifiable> path,
      List<YTIdentifiable> stack,
      OCommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (!nextStep.isEntity()) {
      return;
    }
    if (traversed.contains(nextStep.getEntity().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof YTTraverseResult) {
      ((YTTraverseResult) nextStep).depth = depth;
      ((YTTraverseResult) nextStep).setMetadata("$depth", depth);
      List<YTIdentifiable> newPath = new ArrayList<>();
      newPath.addAll(path);
      nextStep.getIdentity().ifPresent(x -> newPath.add(x.getIdentity()));
      ((YTTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      List newStack = new ArrayList();
      newStack.addAll(reverseStack);
      ((YTTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(nextStep, ctx, entryPoints, traversed);
    } else {
      YTTraverseResult res = new YTTraverseResult(ctx.getDatabase(), nextStep.getEntity().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);
      List<YTIdentifiable> newPath = new ArrayList<>();
      newPath.addAll(path);
      nextStep.getIdentity().ifPresent(x -> newPath.add(x.getIdentity()));
      ((YTTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      List newStack = new ArrayList();
      newStack.addAll(reverseStack);
      ((YTTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(res, ctx, entryPoints, traversed);
    }
  }

  private void tryAddEntryPoint(
      YTResult res, OCommandContext ctx, List<YTResult> entryPoints, Set<YTRID> traversed) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      entryPoints.add(0, res);
    }

    if (res.isEntity()) {
      traversed.add(res.getEntity().get().getIdentity());
    } else if (res.getProperty("@rid") != null
        && res.getProperty("@rid") instanceof YTIdentifiable) {
      traversed.add(((YTIdentifiable) res.getProperty("@rid")).getIdentity());
    }
  }

  private void tryAddEntryPointAtTheEnd(
      YTResult res, OCommandContext ctx, List<YTResult> entryPoints, Set<YTRID> traversed) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      entryPoints.add(res);
    }

    if (res.isEntity()) {
      traversed.add(res.getEntity().get().getIdentity());
    } else if (res.getProperty("@rid") != null
        && res.getProperty("@rid") instanceof YTIdentifiable) {
      traversed.add(((YTIdentifiable) res.getProperty("@rid")).getIdentity());
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DEPTH-FIRST TRAVERSE \n");
    result.append(spaces);
    result.append("  " + projections.toString());
    if (whileClause != null) {
      result.append("\n");
      result.append(spaces);
      result.append("WHILE " + whileClause);
    }
    return result.toString();
  }
}
