/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.PriorityQueue;

import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/**
 * Calculates query weights and build query scorers for hybrid query.
 */
@Setter
public final class NeuralSparseQueryWeight extends Weight {

    // The Weights for our query, in 1-1 correspondence

    private Weight highScoreTokenWeight;

    private Weight lowScoreTokenWeight;
    IndexSearcher searcher;
    ScoreMode scoreMode;
    float boost;
    LeafReaderContext context;
    boolean setLow = false;

    private Query lowQuery;
    @Getter
    private PriorityQueue<Float> priorityQueue = new PriorityQueue<>();

    /**
     * Construct the Weight for this Query searched by searcher. Recursively construct subquery weights.
     */

    public NeuralSparseQueryWeight(
        Query lowTokenQuery,
        IndexSearcher searcher,
        ScoreMode scoreMode,
        float boost,
        Weight highScoreTokenQueryWeight,
        NeuralSparseQuery neuralSparseQuery
    ) {
        super(neuralSparseQuery);
        setHighScoreTokenWeight(highScoreTokenQueryWeight);
        lowQuery = lowTokenQuery;
        this.searcher = searcher;
        this.scoreMode = scoreMode;
        this.boost = boost;

    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return Explanation.match(1.0f, "CustomQuery Explanation");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier highScoreTokenscorerSupplier = highScoreTokenWeight.scorerSupplier(context);
        this.context = context;
        if (highScoreTokenscorerSupplier != null) {
            return new NeuralSparseScorer(highScoreTokenscorerSupplier, this, this.priorityQueue);
        }
        return null;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        // 如果两步查询的权重都是可缓存的，则此权重也可缓存
        return false;
    }

    @Override
    public Matches matches(LeafReaderContext ctx, int doc) throws IOException {
        return highScoreTokenWeight.matches(ctx, doc);
    }

    public ScorerSupplier getLowTokenSS() throws IOException {
        if (!setLow) {
            lowScoreTokenWeight = lowQuery.createWeight(searcher, scoreMode, boost);
        }
        return lowScoreTokenWeight.scorerSupplier(context);
    }
}
