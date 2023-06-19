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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.plugin.common.script.ScriptContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.GatewayServer;
import py4j.Py4JException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Executes python code using py4j python library.
 * Which involves starting python process as a server and than connecting java client to it.
 */
public class Py4jPythonExecutor implements PythonExecutor {
  private static final int PY4J_INIT_POLL_INTERVAL = 1;
  private static final int PY4J_INIT_TIMEOUT = 120;
  private static final int PYTHON_PROCESS_PORT_FILE_CREATED_INTERVAL = 1;
  private static final int PYTHON_PROCESS_PORT_FILE_CREATED_TIMEOUT = 120;
  private static final int PYTHON_PROCESS_STOP_TIMEOUT = 30;
  private static final int PYTHON_PROCESS_KILL_TIMEOUT = 10;
  private static final int CERT_VALIDITY_DAYS = 90;

  private static final String USER_CODE_PLACEHOLDER = "\\$\\{cdap\\.transform\\.function\\}";
  private static final String PYTHONPATH_ENV_VARIABLE_NAME = "PYTHONPATH";
  // in our case (generating via code) it does not make much sense to have a password for keystore
  private static final String PASSWORD_STRING = "";

  private static final Logger LOGGER = LoggerFactory.getLogger(Py4jPythonExecutor.class);

  private final PythonEvaluator.Config config;
  private final String pythonPath;
  private final String pythonBinary;
  private File pythonProcessOutputFile;
  private Process process;
  private GatewayServer gatewayServer;
  private Py4jTransport py4jTransport;
  private File transformTempDir;
  private KeyStore keyStore;

  public Py4jPythonExecutor(PythonEvaluator.Config config) {
    this.config = config;
    this.pythonBinary = config.getPythonBinary();

    String pythonPath = config.getPythonPath();
    if (pythonPath == null) {
      this.pythonPath = "";
    } else {
      this.pythonPath = pythonPath;
    }
  }

  private KeyStore generatePemFileAndKeyStore(String transformTempDirString) throws
    UnrecoverableKeyException, CertificateEncodingException,
    NoSuchAlgorithmException, KeyStoreException, IOException {

    char[] password = PASSWORD_STRING.toCharArray();
    KeyStore ks = KeyStores.generatedCertKeyStore(CERT_VALIDITY_DAYS, PASSWORD_STRING);

    File pemFile = new File(transformTempDirString, "selfsigned.pem");
    KeyStores.generatePemFileFromKeyStore(ks, PASSWORD_STRING, pemFile);

    return ks;
  }

  private File prepareTempFiles() throws IOException, UnrecoverableKeyException,
    CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException {
    InputStream url = getClass().getResourceAsStream("/pythonEvaluator.py");
    //String scriptText = new String(Files.readAllBytes(Paths.get(url.getPath())), StandardCharsets.UTF_8);
    String scriptText = IOUtils.toString(url, StandardCharsets.UTF_8);
    scriptText = scriptText.replaceAll(USER_CODE_PLACEHOLDER, config.getScript());

    Path transformTempDirPath = Files.createTempDirectory("transform");

    LOGGER.debug("Tmp folder is {}", transformTempDirPath);

    String transformTempDirString = transformTempDirPath.normalize().toString();
    transformTempDir = new File(transformTempDirString);

    keyStore = generatePemFileAndKeyStore(transformTempDirString);

    pythonProcessOutputFile = new File(transformTempDirString, "output.txt");
    FileUtils.touch(pythonProcessOutputFile);

    File tempPythonFile = new File(transformTempDirString, "transform.py");
    FileUtils.touch(tempPythonFile);

    try (PrintWriter out = new PrintWriter(tempPythonFile)) {
      out.write(scriptText);
    }

    return tempPythonFile;
  }

  @Override
  public void initialize(ScriptContext scriptContext) throws IOException,
    InterruptedException, UnrecoverableKeyException, CertificateEncodingException,
    NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

    File tempPythonFile = prepareTempFiles();
    File portFile = new File(transformTempDir, "port");

    //ProcessBuilder ps = new ProcessBuilder(pythonBinary, tempPythonFile.getPath());
    String pythonEnv = "python3 -m venv .venv; source .venv/bin/activate";
    String pythonCommand = pythonBinary + " " + tempPythonFile.getPath();
    ProcessBuilder ps = new ProcessBuilder("bash -c", pythonEnv, pythonCommand);
    Map<String, String> envs = ps.environment();
    envs.put(PYTHONPATH_ENV_VARIABLE_NAME, pythonPath);
    ps.directory(transformTempDir);
    ps.redirectErrorStream(true);
    ps.redirectOutput(pythonProcessOutputFile);
    process = ps.start();

    LOGGER.debug("Waiting for python process to respond with port...");

    try {
      Awaitility.with()
        .pollInterval(PYTHON_PROCESS_PORT_FILE_CREATED_INTERVAL, TimeUnit.SECONDS)
        .atMost(PYTHON_PROCESS_PORT_FILE_CREATED_TIMEOUT, TimeUnit.SECONDS)
        .await()
        .until(() -> (portFileCreated(portFile)));
    } catch (ConditionTimeoutException e) {
      String message = String.format("Port file was not created by python process in %d seconds.\n%s",
                                     PYTHON_PROCESS_PORT_FILE_CREATED_TIMEOUT, getProcessOutput());
      throw new RuntimeException(message);
    }

    Integer pythonPort = Files.lines(portFile.toPath()).map(s -> Integer.parseInt(s)).findFirst().get();

    LOGGER.debug("Python process port is: {}", pythonPort);

    SSLContext sslContext = SSLContext.getInstance("TLS");

    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    kmf.init(keyStore, PASSWORD_STRING.toCharArray());


    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(keyStore);

    // setup the HTTPS context and parameters
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);


    gatewayServer = new GatewayServer((Object) null, 0,
                                      InetAddress.getByName("localhost"), GatewayServer.DEFAULT_CONNECT_TIMEOUT,
                                      GatewayServer.DEFAULT_READ_TIMEOUT, null,
                                      new CallbackClient(pythonPort,
                                                         InetAddress.getByName(CallbackClient.DEFAULT_ADDRESS),
                                                         CallbackClient.DEFAULT_MIN_CONNECTION_TIME,
                                                         TimeUnit.SECONDS, sslContext.getSocketFactory()),
                                      sslContext.getServerSocketFactory());
    gatewayServer.start();


    Class[] entryClasses = new Class[]{Py4jTransport.class};
    ClassLoader execClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader pluginClassloader = Py4jTransport.class.getClassLoader();
    Thread.currentThread().setContextClassLoader(pluginClassloader);
    Object test = gatewayServer.getPythonServerEntryPoint(entryClasses);
    Thread.currentThread().setContextClassLoader(execClassLoader);
    LOGGER.error(test.getClass().toString());
    LOGGER.error(test.getClass().getClassLoader().getClass().toString());

    py4jTransport = (Py4jTransport) test;

    LOGGER.debug("Waiting for py4j gateway to start...");

    try {
      Awaitility.with()
        .pollInterval(PY4J_INIT_POLL_INTERVAL, TimeUnit.SECONDS)
        .atMost(PY4J_INIT_TIMEOUT, TimeUnit.SECONDS)
        .await()
        .until(() -> (initializedPythonProcess()));
    } catch (ConditionTimeoutException e) {
      String message = String.format("Connection to python process failed in %d seconds.\n%s",
                                     PY4J_INIT_TIMEOUT, getProcessOutput());
      throw new RuntimeException(message);
    }
  }

  @Override
  public void transform(StructuredRecord input, final Emitter<StructuredRecord> emitter,
                        Emitter<Map> pythonEmitter, ScriptContext scriptContext) {
    try {
      Object record = PythonObjectsEncoder.encode(input, input.getSchema());
      py4jTransport.transform(record, pythonEmitter, scriptContext);
    } catch (Exception e) {
      if (e instanceof Py4JException) {
        LOGGER.error(e.getMessage());
      }
      throw new IllegalArgumentException("Could not transform input.\n" + e.getMessage());
    }
  }

  @Override
  public void destroy() {
    if (gatewayServer != null) {
      gatewayServer.shutdown();
    }

    if (process == null) {
      return;
    }

    LOGGER.debug("Waiting for python process to finish");

    try {
      process.waitFor(PYTHON_PROCESS_STOP_TIMEOUT, TimeUnit.SECONDS);
      process.destroy(); // tell the process to stop
      process.waitFor(PYTHON_PROCESS_KILL_TIMEOUT, TimeUnit.SECONDS); // give it a chance to stop
      process.destroyForcibly();
    } catch (InterruptedException e) {
      int overallTimeout = PYTHON_PROCESS_STOP_TIMEOUT + PYTHON_PROCESS_KILL_TIMEOUT;
      LOGGER.warn("Python process was not able to stop in {} seconds.", overallTimeout);
    }

    LOGGER.info("Python process output:\n{}", getProcessOutput());

    try {
      FileUtils.deleteDirectory(transformTempDir);
    } catch (IOException e) {
      LOGGER.warn("Cannot delete tmp directory {}", transformTempDir, e);
    }

  }

  private String getProcessOutput() {
    try {
      return FileUtils.readFileToString(pythonProcessOutputFile, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOGGER.warn("Cannot read output of python process.\n", e);
    }
    return null;
  }

  private boolean portFileCreated(File portFile) {
    // usually happens if code has a syntax error
    if (!process.isAlive()) {
      throw new RuntimeException(String.format("Python process died too early.\n%s", getProcessOutput()));
    }

    return portFile.exists();
  }

  private boolean initializedPythonProcess() {
    try {
      py4jTransport.initialize(gatewayServer.getListeningPort());
      return true;
    } catch (py4j.Py4JException e) {
      return false;
    }
  }
}
