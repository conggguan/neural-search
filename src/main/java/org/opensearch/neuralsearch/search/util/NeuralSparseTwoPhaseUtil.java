/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.NeuralSparseQuery;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.QueryRescorer;
import org.opensearch.search.rescore.RescoreContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_WINDOW_SIZE;

public class NeuralSparseTwoPhaseUtil {
    private static ClusterService clusterService;

    public static void initialize(ClusterService clusterService) {
        NeuralSparseTwoPhaseUtil.clusterService = clusterService;
    }

    private static void populateQueryWeightsMap(final Query query, Map<Query, Float> query2Weight, float weight) {
        if (query instanceof BoostQuery) {
            BoostQuery boostQuery = (BoostQuery) query;
            weight *= boostQuery.getBoost();
            populateQueryWeightsMap(boostQuery.getQuery(), query2Weight, weight);
        } else if (query instanceof BooleanQuery) {
            for (BooleanClause clause : (BooleanQuery) query) {
                if (clause.isScoring()) populateQueryWeightsMap(clause.getQuery(), query2Weight, weight);
            }
        } else if (query instanceof NeuralSparseQuery) {
            query2Weight.put(((NeuralSparseQuery) query).getLowScoreTokenQuery(), weight);
            ((NeuralSparseQuery) query).extractLowScoreToken();
        }
        // ToDo Support for other compound query.
    }

    private static float getOriginQueryWeightAfterRescore(List<RescoreContext> rescoreContextList) {
        return rescoreContextList.stream()
            .filter(ctx -> ctx instanceof QueryRescorer.QueryRescoreContext)
            .map(ctx -> ((QueryRescorer.QueryRescoreContext) ctx).queryWeight())
            .reduce(1.0f, (a, b) -> a * b);
    }

    private static Query getNestedTwoPhaseQuery(Map<Query, Float> query2weight) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        query2weight.forEach((query, weight) -> { builder.add(new BoostQuery(query, weight), BooleanClause.Occur.SHOULD); });
        return builder.build();
    }

    public static void addTwoPhaseNeuralSparseQuery(final Query query, SearchContext searchContext) {
        Map<Query, Float> query2weight = new HashMap<>();
        populateQueryWeightsMap(query, query2weight, 1.0f);
        Query twoPhaseQuery;
        if (query2weight.isEmpty()) {
            return;
        } else if (query2weight.size() == 1) {
            Map.Entry<Query, Float> entry = query2weight.entrySet().stream().findFirst().get();
            twoPhaseQuery = new BoostQuery(entry.getKey(), entry.getValue());
        } else {
            twoPhaseQuery = getNestedTwoPhaseQuery(query2weight);
        }
        int windowSize = NEURAL_SPARSE_TWO_PHASE_WINDOW_SIZE.get(clusterService.getSettings());
        if (windowSize < 0 || windowSize > 3000) {
            windowSize = 50;
        }
        QueryRescorer.QueryRescoreContext rescoreContext = new QueryRescorer.QueryRescoreContext(windowSize);
        rescoreContext.setQuery(twoPhaseQuery);
        rescoreContext.setRescoreQueryWeight(getOriginQueryWeightAfterRescore(searchContext.rescore()));
        searchContext.addRescore(rescoreContext);
    }

    public static Query getTwoPhaseQuery(final Query query) {
        Map<Query, Float> query2weight = new HashMap<>();
        populateQueryWeightsMap(query, query2weight, 1.0f);
        Query twoPhaseQuery;
        if (query2weight.isEmpty()) {
            return new MatchNoDocsQuery();
        } else if (query2weight.size() == 1) {
            Map.Entry<Query, Float> entry = query2weight.entrySet().stream().findFirst().get();
            twoPhaseQuery = new BoostQuery(entry.getKey(), entry.getValue());
        } else {
            twoPhaseQuery = getNestedTwoPhaseQuery(query2weight);
        }
        return twoPhaseQuery;
    }

    public static TopDocsAndMaxScore getRescoredTopDocsAndMaxScore(
        IndexSearcher searcher,
        TopDocsAndMaxScore topDocsAndMaxScore,
        Query secondPhaseQuery
    ) {
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        TopDocs rescoredTopDocs;
        try {
            rescoredTopDocs = org.apache.lucene.search.QueryRescorer.rescore(
                searcher,
                topDocs,
                secondPhaseQuery,
                1.0,
                NEURAL_SPARSE_TWO_PHASE_WINDOW_SIZE.get(clusterService.getSettings())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Optional<ScoreDoc> maxScoreDoc = Arrays.stream(rescoredTopDocs.scoreDocs)
            .max(Comparator.comparingDouble(scoreDoc -> scoreDoc.score));
        final float rescoreMaxScore = maxScoreDoc.map(scoreDoc -> scoreDoc.score).orElse(0.0f);
        return new TopDocsAndMaxScore(rescoredTopDocs, rescoreMaxScore);
    }
}
