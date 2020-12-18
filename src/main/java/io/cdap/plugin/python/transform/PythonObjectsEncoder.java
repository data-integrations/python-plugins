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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class, which has methods for encoding and decoding object between Java and Python.
 */
public class PythonObjectsEncoder {
  private static final Logger logger = LoggerFactory.getLogger(PythonObjectsEncoder.class.getName());

  public static Object encode(Object object, Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        return object;
      case ENUM:
        break;
      case ARRAY:
        return encodeArray(object, schema.getComponentSchema());
      case MAP:
        Schema keySchema = schema.getMapSchema().getKey();
        Schema valSchema = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        // noinspection unchecked
        return encodeMap((Map<Object, Object>) object, keySchema, valSchema);
      case RECORD:
        return encodeRecord((StructuredRecord) object, schema);
      case UNION:
        return encodeUnion(object, schema.getUnionSchemas());
    }

    throw new IllegalArgumentException("Unable to encode object with schema " + schema + " Reason: unsupported type.");
  }

  public static Map<String, Object> encodeRecord(StructuredRecord record, Schema schema) {
    Map<String, Object> map = new HashMap<>();
    for (Schema.Field field : schema.getFields()) {
      map.put(field.getName(), encode(record.get(field.getName()), field.getSchema()));
    }
    return map;
  }

  public static Object encodeUnion(Object object, List<Schema> unionSchemas) {
    for (Schema schema : unionSchemas) {
      try {
        return encode(object, schema);
      } catch (Exception e) {
        // could be ok, just move on and try the next schema
        logger.warn("Could not encode part of union with schema " + schema, e);
      }
    }

    throw new RuntimeException("Unable to encode union with schema " + unionSchemas);
  }

  public static Map<Object, Object> encodeMap(Map<Object, Object> map, Schema keySchema, Schema valSchema) {
    Map<Object, Object> encoded = new HashMap<>();
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      encoded.put(encode(entry.getKey(), keySchema), encode(entry.getValue(), valSchema));
    }
    return encoded;
  }

  public static List<Object> encodeArray(Object list, Schema componentSchema) {
    List<Object> encoded;
    if (list instanceof Collection) {
      Collection<Object> valuesList = (Collection<Object>) list;
      encoded = new ArrayList<>(valuesList.size());
      for (Object value : valuesList) {
        encoded.add(encode(value, componentSchema));
      }
    } else {
      int length = Array.getLength(list);
      encoded = Lists.newArrayListWithCapacity(length);
      for (int i = 0; i < length; i++) {
        encoded.add(encode(Array.get(list, i), componentSchema));
      }
    }
    return encoded;
  }

  public static Object decode(Object object, Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        return decodeSimpleType(object, schema);
      case ENUM:
        break;
      case ARRAY:
        return decodeArray((List) object, schema.getComponentSchema());
      case MAP:
        Schema keySchema = schema.getMapSchema().getKey();
        Schema valSchema = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        //noinspection unchecked
        return decodeMap((Map<Object, Object>) object, keySchema, valSchema);
      case RECORD:
        return decodeRecord((Map) object, schema);
      case UNION:
        return decodeUnion(object, schema.getUnionSchemas());
    }

    throw new IllegalArgumentException("Unable to decode object with schema " + schema + " Reason: unsupported type.");
  }

  public static StructuredRecord decodeRecord(Map nativeObject, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object fieldVal = nativeObject.get(fieldName);
      builder.set(fieldName, decode(fieldVal, field.getSchema()));
    }
    return builder.build();
  }

  @SuppressWarnings("RedundantCast")
  public static Object decodeSimpleType(Object object, Schema schema) {
    Schema.Type type = schema.getType();
    switch (type) {
      case NULL:
        return null;
      case INT:
        return (Integer) object;
      case LONG:
        if (object instanceof BigInteger) {
          return ((BigInteger) object).longValue();
        }
        return ((Number) object).longValue();
      case FLOAT:
        return ((Number) object).floatValue();
      case BYTES:
        return (byte[]) object;
      case DOUBLE:
        // cast so that if it's not really a double it will fail. This is possible for unions,
        // where we don't know what the actual type of the object should be.
        return ((Number) object).doubleValue();
      case BOOLEAN:
        return (Boolean) object;
      case STRING:
        return (String) object;
    }
    throw new IllegalArgumentException("Unable to decode object with schema " + schema + " Reason: unsupported type.");
  }

  public static Map<Object, Object> decodeMap(Map<Object, Object> object, Schema keySchema, Schema valSchema) {
    Map<Object, Object> output = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : object.entrySet()) {
      output.put(decode(entry.getKey(), keySchema), decode(entry.getValue(), valSchema));
    }
    return output;
  }

  public static List<Object> decodeArray(List nativeArray, Schema componentSchema) {
    List<Object> arr = Lists.newArrayListWithCapacity(nativeArray.size());
    for (Object arrObj : nativeArray) {
      arr.add(decode(arrObj, componentSchema));
    }
    return arr;
  }

  public static Object decodeUnion(Object object, List<Schema> schemas) {
    for (Schema schema : schemas) {
      try {
        return decode(object, schema);
      } catch (Exception e) {
        // could be ok, just move on and try the next schema
        logger.warn("Could not decode part of union with schema " + schema, e);
      }
    }

    throw new RuntimeException("Unable to decode union with schema " + schemas);
  }
}
