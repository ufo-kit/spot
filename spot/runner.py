import os
import re
import json
import hashlib
import subprocess
import shlex
import datetime
import time
import itertools
import jinja2
from marshmallow import Schema, fields, post_load
from marshmallow.exceptions import ValidationError


DATA_HOME = os.environ.get('XDG_DATA_HOME', os.path.expanduser('~/.local/share'))
DATA_DIR = os.path.abspath(os.path.join(DATA_HOME, 'spot'))


class LoadError(ValueError):
    pass


class ExecutionError(RuntimeError):
    pass


class Converter(object):
    def convert(self, value):
        raise NotImplementedError


class SimpleConverter(Converter):
    def __init__(self, t):
        self._type = t

    def convert(self, value):
        try:
            return [self._type(value)]
        except ValueError:
            raise ExecutionError("Cannot convert `{}' to type {}".format(value, str(self._type)))


class RangeConverter(SimpleConverter):
    def __init__(self, t):
        super(RangeConverter, self).__init__(t)

    def convert(self, value):
        import numpy as np

        if ':' not in value:
            return super(RangeConverter, self).convert(value)

        parts = value.split(':')

        if len(parts) != 3:
            raise ExecutionError("Interval must be start:stop:num")

        start = super(RangeConverter, self).convert(parts[0])
        end = super(RangeConverter, self).convert(parts[1])

        try:
            num = int(parts[2])
        except ValueError:
            raise ExecutionError("Interval number must be of type int")

        return list(np.linspace(start, end, num))


class FactSchema(Schema):
    runner_uid = fields.Str(required=True)
    version = fields.Str(required=True)
    start = fields.Str(required=True)
    steps = fields.List(fields.Str(), required=True)

    @post_load
    def make(self, data):
        fact = Fact(data['runner_uid'], data['version'])
        fact.start = data['start']
        fact.steps = data['start']
        return fact


class Fact(object):
    def __init__(self, runner_uid, version):
        self.runner_uid = runner_uid
        self.version = version
        self.start = str(datetime.datetime.now())
        self.steps = []

    def append(self, command, time, success):
        self.steps.append(dict(runner=self.runner_uid, command=command, time=time, success=success))

    def to_dict(self):
        return dict(start=self.start, steps=self.steps)


class ParameterField(fields.Field):
    converters = {
        'str': SimpleConverter(str),
        'int': SimpleConverter(int),
        'float': RangeConverter(float),
        'path': SimpleConverter(str)
    }

    def _serialize(self, value, attr, obj):
        return '{}:{}'.format(value[0], value[1])

    def _deserialize(self, value, attr, data):
        try:
            name, type_name = value.split(':')
        except ValueError as e:
            raise ValidationError("Wrong parameter specification of `{}'".format(value))

        if not type_name in self.converters:
            raise ValidationError("No type conversion for `{}' found".format(type_name))

        return (name, type_name, self.converters[type_name])


class RunnerSchema(Schema):
    version = fields.Str(required=True)
    version_command = fields.Str(load_from='version-command', dump_to='version-command', required=True)
    parameters = fields.List(ParameterField, required=True)
    run_commands = fields.List(fields.Str(), load_from='run-commands', dump_to='run-commands', required=True)

    @post_load
    def make(self, data):
        return Runner(data['version'], data['version_command'], data['parameters'], data['run_commands'])


class Runner(object):
    def __init__(self, version, version_command, parameters, run_commands):
        self.version = version
        self.version_command = version_command
        self.parameters = parameters
        self.run_commands = run_commands

    @property
    def uid(self):
        s = self.version_command + '&' + self.version + \
                '&'.join(self.run_commands) + \
                '&'.join(x + '&' + y for x, y, _ in self.parameters)
        return hashlib.sha256(s).hexdigest()

    @property
    def command_version(self):
        """Return the version of the used executable."""
        return subprocess.check_output(self.version_command, shell=True).strip()

    def execute(self, parameters):
        """
        Execute the runner with given parameters.

        Args:
            parameters: A dictionary mapping parameter names to values. All
                parameters given in the runner description must be given.

        Returns: A Result object.
        """
        def backtickify(l):
            return ("`{}`".format(x) for x in l)

        expected = dict((x, y) for x, _, y in self.parameters)
        expected_keys = set(expected.keys())
        provided_keys = set(parameters.keys())

        not_provided = expected_keys - provided_keys

        if not_provided:
            raise ExecutionError("{} not provided".format(", ".join(backtickify(not_provided))))

        superflous = provided_keys - expected_keys

        if superflous:
            raise ExecutionError("don't know {}".format(", ".join(backtickify(superflous))))

        converted = {k: expected[k].convert(v) for k, v in parameters.items()}

        def fixed_parameters(parameters):
            for elem in itertools.product(*parameters.values()):
                yield dict(zip(parameters.keys(), elem))

        def execute(fixed):
            fact = Fact(self.uid, self.version)

            for template in self.run_commands:
                command = jinja2.Template(template).render(**fixed)

                start = time.time()
                proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, error = proc.communicate()
                success = proc.returncode == 0

                # if not success:
                #     raise ExecutionError("Command failed: {}".format(error.strip()))

                elapsed = time.time() - start
                fact.append(command, elapsed, success)

            return fact

        return [execute(fixed).to_dict() for fixed in fixed_parameters(converted)]


def list_all():
    return sorted((os.path.splitext(x)[0] for x in os.listdir(DATA_DIR)))


def _read_recursively(name, version=None):
    with open(os.path.join(DATA_DIR, name + '.json')) as f:
        data = json.load(f)

        if 'extends' in data:
            parent = _read_recursively(data['extends'])
            data.update(parent)

        return data


def load(name):
    try:
        data = _read_recursively(name)
    except IOError as e:
        raise LoadError("could not load `{}': {}".format(name, str(e)))

    schema = RunnerSchema()
    result = schema.load(data)

    for key, errors in result.errors.items():
        raise LoadError("could not load `{}`: problem with `{}': {}".format(name, key, ' '.join(errors)))

    return result.data
