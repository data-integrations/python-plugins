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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.hydrator.common.script.ScriptContext;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.Map;

/**
 * Executes python code
 */
public interface PythonExecutor {
  /**
   * Initialize interpreter to run transformations
   *
   * @param scriptContext Contains dataa which will be used by a python script
   * @throws IOException
   * @throws InterruptedException
   */
  void initialize(ScriptContext scriptContext) throws IOException, InterruptedException, UnrecoverableKeyException,
    CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException;

  /**
   * Run python transformation
   *
   * @param input         data about record to be transformed
   * @param emitter       cdap emmitter
   * @param pythonEmitter Emitter object used in python code to return data to java
   * @param scriptContext Contains dataa which will be used by a python script
   */
  void transform(StructuredRecord input, final Emitter<StructuredRecord> emitter,
                 Emitter<Map> pythonEmitter, ScriptContext scriptContext);

  /**
   * Handle gracefully and correctly closing of resources related interpreter.
   */
  void destroy();
}
