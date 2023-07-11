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
import com.google.common.io.Files;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
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

  @Test
  public void testSSLCertificateGeneration() throws UnrecoverableKeyException, CertificateException,
          NoSuchAlgorithmException, KeyStoreException, IOException {
    KeyStore ks = KeyStores.generatedCertKeyStore(10, "password");
    File pemFile = temporaryFolder.newFile("selfsigned.pem");
    KeyStores.generatePemFileFromKeyStore(ks, "password", pemFile);

    List<String> certFile = Files.readLines(pemFile, StandardCharsets.UTF_8);
    certFile.removeIf(String::isEmpty);
    // should contain 6 lines
    // -----BEGIN RSA PRIVATE KEY-----\n <encoded private key>\n -----END RSA PRIVATE KEY-----\n
    // -----BEGIN CERTIFICATE-----\n <encoded public key>\n -----END CERTIFICATE-----
    Assert.assertEquals(6, certFile.size());
    byte [] decodedPublicKey = Base64.getDecoder().decode(certFile.get(4));
    X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509")
            .generateCertificate(new ByteArrayInputStream(decodedPublicKey));

    Collection<List<?>> sans = cert.getSubjectAlternativeNames();
    // Should contain only 1 san (ip: 127.0.0.1)
    Assert.assertEquals(1, sans.size());
    Integer sanType = (Integer) ((List<?>) sans.toArray()[0]).get(0);
    Assert.assertEquals((Integer) 7, sanType); // Enum for IPAddress
    String sanValue = (String) ((List<?>) sans.toArray()[0]).get(1);
    Assert.assertEquals("127.0.0.1", sanValue);
  }
}
