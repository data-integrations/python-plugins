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

package co.cask.hydrator.python.transform;

import co.cask.cdap.etl.api.Emitter;
import co.cask.hydrator.common.script.ScriptContext;

import java.util.Map;

/**
 * An interface which is implemented in python code (pythonEvaluator.py) using py4j.
 */
public interface Py4jTransport {
  /**
   * Tells python on which port py4j gateway is running.
   *
   * @param port
   */
  void initialize(Integer port);

  /**
   * Is impelemented by actual user transformation code.
   *
   * @param record
   * @param emitter       emitter to return data to jvm
   * @param scriptContext context containing information
   */
  void transform(Object record, Emitter<Map> emitter, ScriptContext scriptContext);
}
