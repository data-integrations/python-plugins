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

transform_transport = PythonTransformTransportImpl()

gateway = JavaGateway(
  callback_server_parameters=CallbackServerParameters(port=0),
  python_server_entry_point=transform_transport)
transform_transport.gateway = gateway

with open("port", "w") as fp:
  fp.write(str(gateway.get_callback_server().get_listening_port()))

def on_server_connection_stopped(signal, sender, **params):
  gateway.shutdown()

java_gateway.server_connection_stopped.connect(on_server_connection_stopped)
