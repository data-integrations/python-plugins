'''
Copyright (c) 2019 Cask Data, Inc.

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import ssl

from threading import Thread
from py4j import java_gateway
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters

class PythonTransformTransportImpl(object):
  def initialize(self, port):
    self.gateway.gateway_parameters.port = port

  def transform(self, record, emitter, context):
    transform(record, emitter, context)

  class Java:
    implements = ["co.cask.hydrator.plugin.transform.PythonTransformTransport"]


${cdap.transform.function}


key_file = "selfsigned.pem"

client_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
client_ssl_context.verify_mode = ssl.CERT_REQUIRED
client_ssl_context.check_hostname = True
client_ssl_context.load_verify_locations(cafile=key_file)

server_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
server_ssl_context.load_cert_chain(key_file, password='')

# address must match cert, because we're checking hostnames
gateway_parameters = GatewayParameters(
  address='localhost',
  ssl_context=client_ssl_context)

transform_transport = PythonTransformTransportImpl()

gateway = JavaGateway(
  callback_server_parameters=CallbackServerParameters(port=0, ssl_context=server_ssl_context),
  gateway_parameters=gateway_parameters,
  python_server_entry_point=transform_transport)
transform_transport.gateway = gateway

with open("port", "w") as fp:
  fp.write(str(gateway.get_callback_server().get_listening_port()))

def on_server_connection_stopped(signal, sender, **params):
  gateway.shutdown()

java_gateway.server_connection_stopped.connect(on_server_connection_stopped)
