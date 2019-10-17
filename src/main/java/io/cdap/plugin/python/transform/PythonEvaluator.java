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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.LookupProvider;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.common.script.JavaTypeConverters;
import io.cdap.plugin.common.script.ScriptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Transforms records using custom Python code provided by the config.
 */
@Plugin(type = "transform")
@Name("PythonEvaluator")
@Description("Executes user-provided Python code that transforms one record into another.")
public class PythonEvaluator extends Transform<StructuredRecord, StructuredRecord> {
  private static final String EXECUTION_MODE_INTERPRETED = "Interpreted";

  private final Config config;
  private Schema schema;
  private StageMetrics metrics;
  private Logger logger;
  private ScriptContext scriptContext;
  private PythonExecutor pythonExecutor;

  /**
   * Configuration for the script transform.
   */
  public static class Config extends PluginConfig {
    private static final String SCRIPT = "script";
    private static final String SCHEMA = "schema";

    @Description("Python code defining how to transform one record into another. The script must implement a " +
      "function called 'transform', which takes as input a dictionary representing the input record, an emitter " +
      "object, and a context object (which contains CDAP metrics and logger). The emitter object can be used to emit " +
      "one or more key-value pairs to the next stage. It can also be used to emit errors. " +
      "For example:\n" +
      "'def transform(record, emitter, context):\n" +
      "  if record['count'] == 0:\n" +
      "    emitter.emitError({\"errorCode\":31, \"errorMsg\":\"Count is zero.\", \"invalidRecord\": record}\n" +
      "    return\n" +
      "  record['count'] *= 1024\n" +
      "  if(record['count'] < 0):\n" +
      "    context.getMetrics().count(\"negative.count\", 1)\n" +
      "    context.getLogger().debug(\"Received record with negative count\")\n" +
      "  emitter.emit(record)'\n" +
      "will scale the 'count' field by 1024.")
    private final String script;

    @Description("The schema of the output object. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    private final String schema;

    @Description("In interpreted mode Python code is executed via jvm, hence C based libs (e.g. numpy) and " +
      "Python3 syntax are not supported.\n" +
      "In native mode external Python process is started, which removes limitations. However, user " +
      "is required to install py4j library on all hosts. For Python 2 run \"pip install py4j\" to install it. " +
      "For Python 3 run \"pip3 install py4j\".")
    @Macro
    private String executionMode;

    @Description("Path to binary which will run the python code. E.g. /usr/bin/python3.\n" +
      "This value is only used in native mode.")
    @Nullable
    @Macro
    private String pythonBinary;

    @Description("PYTHONPATH environment variable. Allows to include libs from various locations." +
      "This value is only used in native mode.")
    @Nullable
    @Macro
    private String pythonPath;

    public Config(String script, String schema) {
      this.script = script;
      this.schema = schema;
      this.executionMode = EXECUTION_MODE_INTERPRETED;
    }

    public String getScript() {
      return script;
    }


    public String getPythonBinary() {
      return pythonBinary;
    }


    public String getPythonPath() {
      return pythonPath;
    }
  }

  // for unit tests, otherwise config is injected by plugin framework.
  public PythonEvaluator(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws IOException, InterruptedException,
    UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException,
    KeyStoreException, KeyManagementException {

    metrics = context.getMetrics();
    logger = LoggerFactory.getLogger(PythonEvaluator.class.getName() + " - Stage:" + context.getStageName());

    init(context);

    if (config.executionMode.equalsIgnoreCase(EXECUTION_MODE_INTERPRETED)) {
      pythonExecutor = new JythonPythonExecutor(config);
    } else {
      pythonExecutor = new Py4jPythonExecutor(config);
    }

    pythonExecutor.initialize(scriptContext);
  }

  @Override
  public void destroy() {
    pythonExecutor.destroy();
  }


  /**
   * Emitter to be used from within Python code
   */
  public final class PythonEmitter implements Emitter<Map> {

    private final Emitter<StructuredRecord> emitter;
    private final Schema schema;

    public PythonEmitter(Emitter<StructuredRecord> emitter, Schema schema) {
      this.emitter = emitter;
      this.schema = schema;
    }

    @Override
    public void emit(Map value) {
      emitter.emit(decode(value));
    }

    @Override
    public void emitAlert(Map<String, String> payload) {
      emitter.emitAlert(payload);
    }

    @Override
    public void emitError(InvalidEntry<Map> invalidEntry) {
      emitter.emitError(new InvalidEntry<>(invalidEntry.getErrorCode(), invalidEntry.getErrorMsg(),
                                           decode(invalidEntry.getInvalidRecord())));
    }

    public void emitError(Map invalidEntry) {
      emitter.emitError(new InvalidEntry<>((int) invalidEntry.get("errorCode"),
                                           (String) invalidEntry.get("errorMsg"),
                                           decode((Map) invalidEntry.get("invalidRecord"))));
    }

    private StructuredRecord decode(Map nativeObject) {
      return PythonObjectsEncoder.decodeRecord(nativeObject, schema);
    }
  }


  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    if (config.schema != null) {
      try {
        stageConfigurer.setOutputSchema(parseJson(config.schema));
      } catch (Exception e) {
        collector.addFailure(e.getMessage(), null).withStacktrace(e.getStackTrace()).withConfigProperty(Config.SCHEMA);
      }
    } else {
      stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
    }
    // try evaluating the script to fail application creation
    // if the script is invalid (only possible for interpreted mode)
    if (config.executionMode.equalsIgnoreCase(EXECUTION_MODE_INTERPRETED)) {
      try {
        new JythonPythonExecutor(config).initialize(null);
      } catch (IOException | InterruptedException e) {
        collector.addFailure("Could not compile script: " + e.getMessage(), null)
          .withStacktrace(e.getStackTrace()).withConfigProperty(Config.SCRIPT);
      }
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    List<String> inputFields = new ArrayList<>();
    List<String> outputFields = new ArrayList<>();
    Schema inputSchema = context.getInputSchema();
    if (SchemaValidator.canRecordLineage(inputSchema, "input")) {
      //noinspection ConstantConditions
      inputFields = inputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    }
    Schema outputSchema = context.getOutputSchema();
    if (SchemaValidator.canRecordLineage(outputSchema, "output")) {
      //noinspection ConstantConditions
      outputFields = outputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    }
    FieldOperation dataPrepOperation = new FieldTransformOperation("Python", config.script, inputFields, outputFields);
    context.record(Collections.singletonList(dataPrepOperation));
  }

  @Override
  public void transform(StructuredRecord input, final Emitter<StructuredRecord> emitter) {
    Emitter<Map> pythonEmitter = new PythonEvaluator.PythonEmitter(emitter,
                                                                   schema == null ? input.getSchema() : schema);
    pythonExecutor.transform(input, emitter, pythonEmitter, scriptContext);
  }

  private void init(@Nullable TransformContext context) {
    scriptContext = new ScriptContext(
      logger, metrics,
      new LookupProvider() {
        @Override
        public <T> Lookup<T> provide(String s, Map<String, String> map) {
          throw new UnsupportedOperationException("lookup is currently not supported.");
        }
      },
      null,
      new JavaTypeConverters() {
        @Override
        public Object mapToJSObject(Map<?, ?> map) {
          return null;
        }
      },
      context == null ? null : context.getArguments());

    if (config.schema != null) {
      schema = parseJson(config.schema);
    }
  }

  private Schema parseJson(String schema) {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
    }
  }

}

