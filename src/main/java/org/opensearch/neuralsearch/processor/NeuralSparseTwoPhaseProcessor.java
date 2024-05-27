/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.SetOnce;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A SearchRequestProcessor to generate two-phase NeuralSparseQueryBuilder,
 * and add it to the Rescore of a searchRequest.
 */
@Setter
@Getter
public class NeuralSparseTwoPhaseProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "neural_sparse_two_phase_processor";
    public static final String UP_THRESHOLD = "UP_THRESHOLD";
    public static final String DOWN_THRESHOLD = "DOWN_THRESHOLD";
    private boolean enabled;
    private float ratio;
    private float windowExpansion;
    private int maxWindowSize;
    private static final String PARAMETER_KEY = "two_phase_parameter";
    private static final String RATIO_KEY = "prune_ratio";
    private static final String ENABLE_KEY = "enabled";
    private static final String EXPANSION_KEY = "expansion_rate";
    private static final String MAX_WINDOW_SIZE_KEY = "max_window_size";
    private static final boolean DEFAULT_ENABLED = true;
    private static final float DEFAULT_RATIO = 0.4f;
    private static final float DEFAULT_WINDOW_EXPANSION = 5.0f;
    private static final int DEFAULT_MAX_WINDOW_SIZE = 10000;
    private static final int DEFAULT_BASE_QUERY_SIZE = 10;
    private static final int MAX_WINDOWS_SIZE_LOWER_BOUND = 50;
    private static final float WINDOW_EXPANSION_LOWER_BOUND = 1.0f;
    private static final float RATIO_LOWER_BOUND = 0f;
    private static final float RATIO_UPPER_BOUND = 1f;

    protected NeuralSparseTwoPhaseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        boolean enabled,
        float ratio,
        float windowExpansion,
        int maxWindowSize
    ) {
        super(tag, description, ignoreFailure);
        this.enabled = enabled;
        if (ratio < RATIO_LOWER_BOUND || ratio > RATIO_UPPER_BOUND) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The two_phase_parameter.prune_ratio must be within [0, 1]. Received: %f", ratio)
            );
        }
        this.ratio = ratio;
        if (windowExpansion < WINDOW_EXPANSION_LOWER_BOUND) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The two_phase_parameter.expansion_rate must >= 1.0. Received: %f", windowExpansion)
            );
        }
        this.windowExpansion = windowExpansion;
        if (maxWindowSize < MAX_WINDOWS_SIZE_LOWER_BOUND) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The two_phase_parameter.max_window_size must >= 50. Received: %n" + maxWindowSize)
            );
        }
        this.maxWindowSize = maxWindowSize;
    }

    /**
     * Process the search request of neural_sparse_two_phase_processor
     * @param request the search request (which may have been modified by an earlier processor)
     * @return request the search request that add the two-phase rescore query of neural sparse query.
     */
    @Override
    public SearchRequest processRequest(final SearchRequest request) throws Exception {
        if (!enabled || ratio == 0f) {
            return request;
        }
        QueryBuilder queryBuilder = request.source().query();
        // Collect the nested NeuralSparseQueryBuilder in the whole query.
        Multimap<NeuralSparseQueryBuilder, Float> queryBuilderMap;
        queryBuilderMap = collectNeuralSparseQueryBuilder(queryBuilder, 1.0f);
        if (queryBuilderMap.isEmpty()) {
            return request;
        }
        // Make a nestedQueryBuilder which includes all the two-phase QueryBuilder.
        QueryBuilder nestedTwoPhaseQueryBuilder = getNestedQueryBuilderFromNeuralSparseQueryBuilderMap(queryBuilderMap);
        nestedTwoPhaseQueryBuilder.boost(getOriginQueryWeightAfterRescore(request.source()));
        // Add it to the rescorer.
        RescorerBuilder<QueryRescorerBuilder> twoPhaseRescorer = buildRescoreQueryBuilderForTwoPhase(nestedTwoPhaseQueryBuilder, request);
        request.source().addRescorer(twoPhaseRescorer);
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Based on ratio, split a Map into two map by the value.
     * @param queryTokens the queryTokens map, key is the token String, value is the score.
     * @param thresholdRatio The ratio that control how tokens map be split.
     * @return A map has two element, {[True, token map whose value above threshold],[False, token map whose value below threshold]}
     */
    public static Map<String, SetOnce<Map<String, Float>>> splitQueryTokensByRatioedMaxScoreAsThreshold(
        final Map<String, Float> queryTokens,
        final float thresholdRatio
    ) {

        float max = 0f;
        for (Float value : queryTokens.values()) {
            max = Math.max(value, max);
        }
        float threshold = max * thresholdRatio;

        Map<Boolean, Map<String, Float>> queryTokensByScore = queryTokens.entrySet()
            .stream()
            .collect(
                Collectors.partitioningBy(entry -> entry.getValue() >= threshold, Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

        SetOnce<Map<String, Float>> highScoreTokensSetOnce = new SetOnce<>(queryTokensByScore.get(Boolean.TRUE));
        SetOnce<Map<String, Float>> lowScoreTokensSetOnce = new SetOnce<>(queryTokensByScore.get(Boolean.FALSE));
        if (Objects.isNull(highScoreTokensSetOnce.get())) {
            throw new IllegalArgumentException("Query tokens cannot be null.");
        }
        return Map.of(UP_THRESHOLD, highScoreTokensSetOnce, DOWN_THRESHOLD, lowScoreTokensSetOnce);
    }

    /**
     * Factory to create NeuralSparseTwoPhaseProcessor, provide default parameter,
     *
     */
    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        @Override
        public NeuralSparseTwoPhaseProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException {

            boolean enabled = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, ENABLE_KEY, DEFAULT_ENABLED);
            Map<String, Object> twoPhaseConfigMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, PARAMETER_KEY);

            float ratio = DEFAULT_RATIO;
            float window_expansion = DEFAULT_WINDOW_EXPANSION;
            int max_window_size = DEFAULT_MAX_WINDOW_SIZE;
            if (Objects.nonNull(twoPhaseConfigMap)) {
                ratio = ((Number) twoPhaseConfigMap.getOrDefault(RATIO_KEY, ratio)).floatValue();
                window_expansion = ((Number) twoPhaseConfigMap.getOrDefault(EXPANSION_KEY, window_expansion)).floatValue();
                max_window_size = ((Number) twoPhaseConfigMap.getOrDefault(MAX_WINDOW_SIZE_KEY, max_window_size)).intValue();
            }

            return new NeuralSparseTwoPhaseProcessor(tag, description, ignoreFailure, enabled, ratio, window_expansion, max_window_size);
        }
    }

    private QueryBuilder getNestedQueryBuilderFromNeuralSparseQueryBuilderMap(
        final Multimap<NeuralSparseQueryBuilder, Float> queryBuilderFloatMap
    ) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        queryBuilderFloatMap.asMap().forEach((neuralSparseQueryBuilder, boosts) -> {
            float reduceBoost = boosts.stream().reduce(0.0f, Float::sum);
            final float finalBoost = neuralSparseQueryBuilder.boost() * reduceBoost;
            boolQueryBuilder.should(neuralSparseQueryBuilder.boost(finalBoost));
        });
        return boolQueryBuilder;
    }

    private float getOriginQueryWeightAfterRescore(final SearchSourceBuilder searchSourceBuilder) {
        if (Objects.isNull(searchSourceBuilder.rescores())) {
            return 1.0f;
        }
        return searchSourceBuilder.rescores()
            .stream()
            .map(rescorerBuilder -> ((QueryRescorerBuilder) rescorerBuilder).getQueryWeight())
            .reduce(1.0f, (a, b) -> a * b);
    }

    private Multimap<NeuralSparseQueryBuilder, Float> collectNeuralSparseQueryBuilder(final QueryBuilder queryBuilder, float baseBoost) {
        Multimap<NeuralSparseQueryBuilder, Float> result = ArrayListMultimap.create();

        if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            float updatedBoost = baseBoost * boolQueryBuilder.boost();
            for (QueryBuilder subQuery : boolQueryBuilder.should()) {
                Multimap<NeuralSparseQueryBuilder, Float> subResult = collectNeuralSparseQueryBuilder(subQuery, updatedBoost);
                result.putAll(subResult);
            }
        } else if (queryBuilder instanceof NeuralSparseQueryBuilder) {
            NeuralSparseQueryBuilder neuralSparseQueryBuilder = (NeuralSparseQueryBuilder) queryBuilder;
            float updatedBoost = baseBoost * neuralSparseQueryBuilder.boost();
            NeuralSparseQueryBuilder modifiedQueryBuilder = neuralSparseQueryBuilder.getCopyNeuralSparseQueryBuilderForTwoPhase(ratio);
            result.put(modifiedQueryBuilder, updatedBoost);
        }
        // We only support BoostQuery, BooleanQuery and NeuralSparseQuery now. For other compound query type which are not support now, will
        // do nothing and just quit.
        return result;
    }

    private RescorerBuilder<QueryRescorerBuilder> buildRescoreQueryBuilderForTwoPhase(
        final QueryBuilder nestedTwoPhaseQueryBuilder,
        final SearchRequest searchRequest
    ) {
        RescorerBuilder<QueryRescorerBuilder> twoPhaseRescorer = new QueryRescorerBuilder(nestedTwoPhaseQueryBuilder);
        int requestSize = searchRequest.source().size();
        int windowSize = (int) ((requestSize == -1 ? DEFAULT_BASE_QUERY_SIZE : requestSize) * windowExpansion);
        if (windowSize > maxWindowSize || windowSize < 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The two-phase window size of neural_sparse_two_phase_processor should be [0,%d], but get the value of %d",
                    maxWindowSize,
                    windowSize
                )
            );
        }
        twoPhaseRescorer.windowSize(windowSize);
        return twoPhaseRescorer;
    }
}