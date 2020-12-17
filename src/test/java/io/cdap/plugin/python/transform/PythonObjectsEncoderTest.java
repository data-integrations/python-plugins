/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Tests for {@link PythonObjectsEncoder}.
 */
public class PythonObjectsEncoderTest {

  @Test
  public void testCollections() {
    Schema schema = Schema.recordOf("x", Schema.Field.of("x", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord record = StructuredRecord.builder(schema).set("x", new String[] { "a", "b", "c" }).build();
    Map<String, Object> actual = PythonObjectsEncoder.encodeRecord(record, schema);
    Assert.assertEquals(Collections.singletonMap("x", Arrays.asList("a", "b", "c")), actual);

    record = StructuredRecord.builder(schema).set("x", Arrays.asList("a", "b", "c")).build();
    actual = PythonObjectsEncoder.encodeRecord(record, schema);
    Assert.assertEquals(Collections.singletonMap("x", Arrays.asList("a", "b", "c")), actual);

    record = StructuredRecord.builder(schema).set("x", new LinkedHashSet<>(Arrays.asList("a", "b", "c"))).build();
    actual = PythonObjectsEncoder.encodeRecord(record, schema);
    Assert.assertEquals(Collections.singletonMap("x", Arrays.asList("a", "b", "c")), actual);
  }
}
