/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.python.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.script.ScriptContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * Unit tests for our plugins.
 */
public abstract class BasePythonTransformTest extends HydratorTestBase {

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("python-evaluator", "2.2.0-SNAPSHOT");

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Schema SCHEMA = Schema.recordOf(
    "data",
    Schema.Field.of("s", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("i", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("l", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("f", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
    Schema.Field.of("d", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

  protected static final List<StructuredRecord> INPUT_DEFAULT = ImmutableList.of(
    StructuredRecord.builder(SCHEMA)
      .set("s", "ab").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", true).build(),
    StructuredRecord.builder(SCHEMA)
      .set("s", "xy").set("i", -10).set("l", -10L).set("f", -10f).set("d", -10d).set("b", true).build(),
    StructuredRecord.builder(SCHEMA)
      .set("s", "a").set("i", 10).set("l", 10L).set("f", 10f).set("d", 10d).set("b", false).build(),
    StructuredRecord.builder(SCHEMA)
      .set("s", "").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", false).build());

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.

    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      ScriptContext.class,
                      PythonEvaluator.class,
                      WeakReferenceDelegatorClassLoader.class);
  }

  @Test
  public void testPrimitivesDecoded() throws Exception {
    String script = "def transform(record, emitter, context):\n" +
      "    emitter.emit(record)\n";

    Map<String, String> properties = ImmutableMap.of("script", script,
                                                     "executionMode", getExecutionMode(),
                                                     "pythonBinary", "python");

    List<StructuredRecord> outputRecords = runPythonEvaluatorPipeline(INPUT_DEFAULT, properties);

    Assert.assertEquals(new HashSet<>(outputRecords), new HashSet<>(INPUT_DEFAULT));
  }

  @Test
  public void testFilterRecords() throws Exception {
    String script = "def transform(record, emitter, context):\n" +
      "  if not record['l']:\n" +
      "    emitter.emit(record)";

    Map<String, String> properties = ImmutableMap.of("script", script,
                                                     "executionMode", getExecutionMode(),
                                                     "pythonBinary", "python");

    List<StructuredRecord> outputRecords = runPythonEvaluatorPipeline(INPUT_DEFAULT, properties);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(SCHEMA)
        .set("s", "ab").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", true).build(),
      StructuredRecord.builder(SCHEMA)
        .set("s", "").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", false).build());

    Assert.assertEquals(new HashSet<>(outputRecords), new HashSet<>(expected));
  }

  @Test
  public void testAddRecords() throws Exception {
    String script = "def transform(record, emitter, context):\n" +
      "  emitter.emit(record)\n" +
      "  record['s'] = 'value'\n" +
      "  emitter.emit(record)";

    Map<String, String> properties = ImmutableMap.of("script", script,
                                                     "executionMode", getExecutionMode(),
                                                     "pythonBinary", "python");

    List<StructuredRecord> outputRecords = runPythonEvaluatorPipeline(INPUT_DEFAULT, properties);

    ImmutableList expected = ImmutableList.builder().
      addAll(INPUT_DEFAULT).
      add(StructuredRecord.builder(SCHEMA)
          .set("s", "value").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", true).build()).
      add(StructuredRecord.builder(SCHEMA)
          .set("s", "value").set("i", -10).set("l", -10L).set("f", -10f).set("d", -10d).set("b", true).build()).
      add(StructuredRecord.builder(SCHEMA)
          .set("s", "value").set("i", 10).set("l", 10L).set("f", 10f).set("d", 10d).set("b", false).build()).
      add(StructuredRecord.builder(SCHEMA)
          .set("s", "value").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", false).build()).
      build();

    Assert.assertEquals(new HashSet<>(outputRecords), new HashSet<StructuredRecord>(expected));
  }

  @Test
  public void testModifyRecords() throws Exception {
    String script = "def transform(record, emitter, context):\n" +
      "  record['i'] *= 2\n" +
      "  emitter.emit(record)";

    Map<String, String> properties = ImmutableMap.of("script", script,
                                                     "executionMode", getExecutionMode(),
                                                     "pythonBinary", "python");

    List<StructuredRecord> outputRecords = runPythonEvaluatorPipeline(INPUT_DEFAULT, properties);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(SCHEMA)
        .set("s", "ab").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", true).build(),
      StructuredRecord.builder(SCHEMA)
        .set("s", "xy").set("i", -20).set("l", -10L).set("f", -10f).set("d", -10d).set("b", true).build(),
      StructuredRecord.builder(SCHEMA)
        .set("s", "a").set("i", 20).set("l", 10L).set("f", 10f).set("d", 10d).set("b", false).build(),
      StructuredRecord.builder(SCHEMA)
        .set("s", "").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", false).build());

    Assert.assertEquals(new HashSet<>(outputRecords), new HashSet<>(expected));
  }

  @Test
  public void testContext() throws Exception {
    String script = "def transform(record, emitter, context):\n" +
      "  context.getLogger().debug('outputing from python code!')\n" +
      "  context.getMetrics().count('some_metric', 1)\n" +
      "  emitter.emit(record)";

    Map<String, String> properties = ImmutableMap.of("script", script,
                                                     "executionMode", getExecutionMode(),
                                                     "pythonBinary", "python");

    List<StructuredRecord> outputRecords = runPythonEvaluatorPipeline(INPUT_DEFAULT, properties);

    Assert.assertEquals(new HashSet<>(outputRecords), new HashSet<>(INPUT_DEFAULT));
  }

  protected abstract String getExecutionMode();

  protected List<StructuredRecord> runPythonEvaluatorPipeline(List<StructuredRecord> input,
                                                            Map<String, String> pythonEvaluatorProperties)
    throws Exception {
    String inputName = "input" + UUID.randomUUID().toString();
    String outputName = "output" + UUID.randomUUID().toString();

    // create the pipeline config
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder().setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputName, SCHEMA)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputName)))
      .addStage(new ETLStage("fieldStats", new ETLPlugin("PythonEvaluator", "transform",
                                                         pythonEvaluatorProperties)))
      .addConnection("source", "fieldStats")
      .addConnection("fieldStats", "sink")
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testPipeline" + UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    DataSetManager<Table> inputManager = getDataset(inputName);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    DataSetManager<Table> outputManager = getDataset(outputName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    return outputRecords;
  }
}
