/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.util.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;


public class NeuralSparseTwoPhaseProcessorIT extends AbstractRollingUpgradeTestCase{

    private String sparseModelId = "";

    private static final String TEXT_1 = "Hello world a b";
    private static final String TEST_ENCODING_FIELD = "passage_embedding";

    private static final String TWO_PHASE_PIPELINE_NAME = "two_phase_processor";
    private static final String SPARSE_INGEST_PIPELINE_NAME = "nqep-nlp-ingest-pipeline-sparse";

    private static final String TEST_TEXT_FIELD = "passage_text";

    public void testNeuralSparseTwoPhaseProcessor_NeuralSparseSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        NeuralSparseQueryBuilder sparseEncodingQueryBuilderWithModelId = new NeuralSparseQueryBuilder().fieldName(TEST_ENCODING_FIELD)
            .queryText(TEXT_1);

        switch (getClusterType()) {
            case OLD:
                sparseModelId = uploadSparseEncodingModel();
                loadModel(sparseModelId);
                sparseEncodingQueryBuilderWithModelId.modelId(sparseModelId);
                createPipelineForSparseEncodingProcessor(sparseModelId, SPARSE_INGEST_PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_INGEST_PIPELINE_NAME
                );
                addSparseEncodingDoc(getIndexNameForTest(), "0", List.of(), List.of(), List.of(TEST_TEXT_FIELD), List.of(TEXT_1));
                createNeuralSparseTwoPhaseSearchProcessor(TWO_PHASE_PIPELINE_NAME);
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", TWO_PHASE_PIPELINE_NAME)
                );
                Object a = search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits");
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", "_none")
                );
                Object b = search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits");
                assertEquals(a,b);
                break;
            case MIXED:
                sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(sparseModelId);
                sparseEncodingQueryBuilderWithModelId.modelId(sparseModelId);
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", TWO_PHASE_PIPELINE_NAME)
                );
                Object c = search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits");
                updateIndexSettings(
                    getIndexNameForTest(),
                    Settings.builder().put("index.search.default_pipeline", "_none")
                );
                Object d = search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits");
                assertEquals(c,d);
                break;
            case UPGRADED:
                try {
                    sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_INGEST_PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                    loadModel(sparseModelId);
                    sparseEncodingQueryBuilderWithModelId.modelId(sparseModelId);
                    updateIndexSettings(
                        getIndexNameForTest(),
                        Settings.builder().put("index.search.default_pipeline", TWO_PHASE_PIPELINE_NAME)
                    );
                    Object e = search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits");
                    updateIndexSettings(
                        getIndexNameForTest(),
                        Settings.builder().put("index.search.default_pipeline", "_none")
                    );
                    Object f = search(getIndexNameForTest(), sparseEncodingQueryBuilderWithModelId, 1).get("hits");
                    assertEquals(e,f);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), SPARSE_INGEST_PIPELINE_NAME, sparseModelId, TWO_PHASE_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }

    }



}
