package com.orientechnologies.lucene.functions;

import static com.orientechnologies.lucene.functions.OLuceneFunctionsUtils.getOrCreateMemoryIndex;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataInternal;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromItem;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/**
 *
 */
public class OLuceneSearchOnFieldsFunction extends OLuceneSearchFunctionTemplate {

  public static final String NAME = "search_fields";

  public OLuceneSearchOnFieldsFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public String getName(YTDatabaseSession session) {
    return NAME;
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      CommandContext ctx) {

    if (iThis instanceof YTRID) {
      try {
        iThis = ((YTRID) iThis).getRecord();
      } catch (YTRecordNotFoundException rnf) {
        return false;
      }
    }
    if (iThis instanceof YTIdentifiable) {
      iThis = new YTResultInternal(ctx.getDatabase(), (YTIdentifiable) iThis);
    }
    YTResult result = (YTResult) iThis;

    Entity element = result.toEntity();
    if (!element.getSchemaType().isPresent()) {
      return false;
    }
    String className = element.getSchemaType().get().getName();
    List<String> fieldNames = (List<String>) params[0];

    OLuceneFullTextIndex index = searchForIndex(className, ctx, fieldNames);

    if (index == null) {
      return false;
    }

    String query = (String) params[1];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> element.getProperty(s))
            .collect(Collectors.toList());

    for (IndexableField field : index.buildDocument(ctx.getDatabase(), key).getFields()) {
      memoryIndex.addField(field, index.indexAnalyzer());
    }

    var metadata = getMetadata(params);
    OLuceneKeyAndMetadata keyAndMetadata =
        new OLuceneKeyAndMetadata(
            new OLuceneCompositeKey(Collections.singletonList(query)).setContext(ctx), metadata);

    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private Map<String, ?> getMetadata(Object[] params) {

    if (params.length == 3) {
      return (Map<String, ?>) params[2];
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return "SEARCH_INDEX( indexName, [ metdatada {} ] )";
  }

  @Override
  public Iterable<YTIdentifiable> searchFromTarget(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);

    SQLExpression expression = args[1];
    Object query = expression.execute((YTIdentifiable) null, ctx);
    if (index != null) {

      var meta = getMetadata(args, ctx);
      Set<YTIdentifiable> luceneResultSet;
      try (Stream<YTRID> rids =
          index
              .getInternal()
              .getRids(ctx.getDatabase(),
                  new OLuceneKeyAndMetadata(
                      new OLuceneCompositeKey(Collections.singletonList(query)).setContext(ctx),
                      meta))) {
        luceneResultSet = rids.collect(Collectors.toSet());
      }

      return luceneResultSet;
    }
    throw new RuntimeException();
  }

  private Map<String, ?> getMetadata(SQLExpression[] args, CommandContext ctx) {
    if (args.length == 3) {
      return getMetadata(args[2], ctx);
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected OLuceneFullTextIndex searchForIndex(
      SQLFromClause target, CommandContext ctx, SQLExpression... args) {
    List<String> fieldNames = (List<String>) args[0].execute((YTIdentifiable) null, ctx);
    SQLFromItem item = target.getItem();
    String className = item.getIdentifier().getStringValue();

    return searchForIndex(className, ctx, fieldNames);
  }

  private static OLuceneFullTextIndex searchForIndex(
      String className, CommandContext ctx, List<String> fieldNames) {
    var db = ctx.getDatabase();
    db.activateOnCurrentThread();
    OMetadataInternal dbMetadata = db.getMetadata();

    List<OLuceneFullTextIndex> indices =
        dbMetadata.getImmutableSchemaSnapshot().getClass(className).getIndexes(db).stream()
            .filter(idx -> idx instanceof OLuceneFullTextIndex)
            .map(idx -> (OLuceneFullTextIndex) idx)
            .filter(idx -> intersect(idx.getDefinition().getFields(), fieldNames))
            .toList();

    if (indices.size() > 1) {
      throw new IllegalArgumentException(
          "too many indices matching given field name: " + String.join(",", fieldNames));
    }

    return indices.isEmpty() ? null : indices.get(0);
  }

  public static <T> boolean intersect(List<T> list1, List<T> list2) {
    for (T t : list1) {
      if (list2.contains(t)) {
        return true;
      }
    }

    return false;
  }
}
