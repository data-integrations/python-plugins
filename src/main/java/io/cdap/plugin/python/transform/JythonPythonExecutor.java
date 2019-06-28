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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.plugin.common.script.ScriptContext;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.core.PyException;
import org.python.core.PySyntaxError;
import org.python.util.PythonInterpreter;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

/**
 * Executes python code using Jython library
 */
public class JythonPythonExecutor implements PythonExecutor {
  private static final String INPUT_STRUCTURED_RECORD_VARIABLE_NAME = "dont_name_your_variable_this1";
  private static final String EMITTER_VARIABLE_NAME = "dont_name_your_variable_this2";
  private static final String CONTEXT_NAME = "dont_name_your_context_this";
  private final PythonEvaluator.Config config;

  private PythonInterpreter interpreter;
  private PyCode compiledScript;

  public JythonPythonExecutor(PythonEvaluator.Config config) {
    this.config = config;
  }

  @Override
  public void initialize(ScriptContext scriptContext) throws IOException, InterruptedException {
    interpreter = new PythonInterpreter();
    interpreter.set(CONTEXT_NAME, scriptContext);

    // doing this so that we can pass the 'input' record into the transform function.
    // that is, we want people to implement
    // def transform(input, emitter, context): ...
    // rather than def transform(): ...  and have them access the input, emitter, and context via global variables

    String script = String.format("%s\ntransform(%s, %s, %s)",
                                  config.getScript(), INPUT_STRUCTURED_RECORD_VARIABLE_NAME,
                                  EMITTER_VARIABLE_NAME, CONTEXT_NAME);
    try {
      compiledScript = interpreter.compile(script);
    } catch (PySyntaxError e) {
      if (Strings.isNullOrEmpty(e.getMessage())) {
        throw new RuntimeException(
          "Unknown syntax error occurred while interpreting python code. Please make sure syntax is correct.", e);
      }
      throw new RuntimeException(
        String.format("Syntax error occurred while interpreting python code: '%s'" +
                        "Please make sure syntax is correct.", e.getMessage()), e);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter,
                        Emitter<Map> pythonEmitter, ScriptContext scriptContext) {
    try {
      interpreter.set(INPUT_STRUCTURED_RECORD_VARIABLE_NAME,
                      PythonObjectsEncoder.encode(input, input.getSchema()));
      interpreter.set(EMITTER_VARIABLE_NAME, pythonEmitter);
      Py.runCode(compiledScript, interpreter.getLocals(), interpreter.getLocals());

    } catch (PyException e) {
      // Put stack trace as the exception message, because otherwise the information from PyException is lost.
      // PyException only exposes the actual cause (Python stack trace) if printStackTrace() is called on it.
      throw new IllegalArgumentException("Could not transform input.\n" + getStackTrace(e));
    }
  }

  private String getStackTrace(Throwable throwable) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.toString();
  }

  @Override
  public void destroy() {
    if (interpreter != null) {
      interpreter.cleanup();
    }
  }
}
