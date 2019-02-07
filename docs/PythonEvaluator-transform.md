# Python Evaluator Transform


Description
-----------
Executes user-provided python code that transforms one record into zero or more records.
Each input record is converted into a dictionary which can be directly accessed in
python. The transform expects to receive a dictionary as input, which it can
process and emit zero or more transformed dictionaries, or emit an error dictionary using the provided emitter object.

Configuration
-------------
**Script:** Python code defining how to transform one record into another. The script must
implement a function called ``'transform'``, which takes as input a Python dictionary (representing
the input record), an emitter object, and a context object (which contains CDAP metrics and logger).
The script can then use the emitter object to emit transformed Python dictionaries.

For example:

``'def transform(record, emitter, context): record['count'] *= 1024; emitter.emit(record)'``

will scale the ``'count'`` field of ``record`` by 1024.

**Execution Mode:** How to execute the python code. Supported modes are:
_Interpreted mode_:
Python code is executed via jvm, hence C based libs (e.g. numpy) and Python3 syntax are not supported.

_Native mode:_
Python is run natively, so any python version and libraries installed on executors can be used.
Native mode requires that python is available on every node in your cluster.
It also requires that the py4j library is available on every node.
To install py4j with Python 2, run ``pip install py4j``. To install py4j with Python 3, run ``pip3 install py4j``.

**Python Binary:** Path to binary which will run the python code. E.g. ``/usr/bin/python3``.
This value is only used in native mode.

**PYTHONPATH:** PYTHONPATH environment variable. Allows to include libs from various locations.
This value is only used in native mode.


**schema:** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.


Example
-------
The transform checks each record's ``'subtotal'`` field: if the ``'subtotal'`` is negative, it emits an error;
else, it calculates the ``'tax'`` and ``'total'`` fields based on the ``'subtotal'``, and then returns a record
as a Python dictionary containing those three fields, with the error records written to the configured error dataset:

    {
        "name": "PythonEvaluator",
        "type": "transform",
        "properties": {
            "script": "def transform(record, emitter, context):
                     if (record['subtotal'] < 0):
                       emitter.emitError({
                         'errorCode': 10,
                         'errorMsg': 'subtotal is less than 0',
                         'invalidRecord': record,
                       })
                     else:
                       taxrate = float(context.getArguments().get('taxrate'))
                       tax = record['subtotal'] * taxrate
                       if (tax > 1000.0):
                         context.getMetrics().count('tax.above.1000', 1)
                       emitter.emit({
                         'subtotal': record['subtotal'],
                         'tax': tax,
                         'total': record['subtotal'] + tax,
                       })
                  ",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"expanded\",
                \"fields\":[
                    {\"name\":\"subtotal\",\"type\":\"double\"},
                    {\"name\":\"tax\",\"type\":\"double\"},
                    {\"name\":\"total\",\"type\":\"double\"}
                ]
            }"
        }
    }
