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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class PythonTransformNativeTest extends BasePythonTransformTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected String getExecutionMode() {
    return "Native";
  }

  @Test
  public void testImportThirdPartyLibrary() throws Exception {
    File tempPythonFile = temporaryFolder.newFile("pythonevaluator_test_lib.py");
    String folderPath = temporaryFolder.getRoot().getPath();

    try (PrintWriter out = new PrintWriter(tempPythonFile)) {
      out.write("def emit_with_print(emitter, record):\n" +
                  "  print \"emitting...\"\n" +
                  "  emitter.emit(record)");
    }

    String script = "from pythonevaluator_test_lib import emit_with_print\n" +
      "def transform(record, emitter, context):\n" +
      "  emit_with_print(emitter, record)\n";

    Map<String, String> properties = ImmutableMap.of("script", script,
                                                     "executionMode", getExecutionMode(),
                                                     "pythonBinary", "python",
                                                     "pythonPath", folderPath);

    List<StructuredRecord> outputRecords = runPythonEvaluatorPipeline(INPUT_DEFAULT, properties);

    Assert.assertEquals(new HashSet<>(outputRecords), new HashSet<>(INPUT_DEFAULT));
  }
}
