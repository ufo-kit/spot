import os
import re
import json
import hashlib
import subprocess
import shlex
import datetime
import time
import jinja2


DATA_HOME = os.environ.get('XDG_DATA_HOME', os.path.expanduser('~/.local/share'))
DATA_DIR = os.path.abspath(os.path.join(DATA_HOME, 'spot'))


class LoadError(ValueError):
    pass


class ExecutionError(RuntimeError):
    pass


class Converter(object):
    def convert(self, value):
        raise NotImplementedError


class StringConverter(object):
    def convert(self, value):
        return value


class SimpleConverter(Converter):
    def __init__(self, t):
        self._type = t

    def convert(self, value):
        return self._type(value)


class Fact(object):
    def __init__(self, version):
        self.version = version
        self.start = str(datetime.datetime.now())
        self.steps = []

    def append(self, command, time, success):
        self.steps.append(dict(command=command, time=time, success=success))

    def to_dict(self):
        return dict(start=self.start, steps=self.steps)


class Runner(object):
    converters = {
        'str': StringConverter(),
        'int': SimpleConverter(int),
        'float': SimpleConverter(float),
        'path': StringConverter()
    }

    def __init__(self, data):
        self.uid = digest(data)
        self.version_command = data['version-command']
        self.expected = {}
        self.templates = data['run-commands']

        for description in data['parameters']:
            name, type_name = description.split(':')

            if type_name in self.converters:
                self.expected[name] = self.converters[type_name]

    @property
    def version(self):
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

        expected_keys = set(self.expected.keys())
        provided_keys = set(parameters.keys())

        not_provided = expected_keys - provided_keys

        if not_provided:
            raise ExecutionError("{} not provided".format(", ".join(backtickify(not_provided))))

        superflous = provided_keys - expected_keys

        if superflous:
            raise ExecutionError("don't know {}".format(", ".join(backtickify(superflous))))

        converted = {}

        try:
            for k, v in parameters.items():
                converted[k] = self.expected[k].convert(v)
        except ValueError as e:
            raise ExecutionError("Wrong parameter type for `{}`: {}".format(k, str(e)))

        fact = Fact(self.version)

        for template in self.templates:
            command = jinja2.Template(template).render(**converted)

            start = time.time()
            proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = proc.communicate()
            success = proc.returncode == 0

            if not success:
                raise ExecutionError("Command failed: {}".format(error.strip()))

            elapsed = time.time() - start
            fact.append(command, elapsed, success)

        return fact


def list_all():
    return sorted((os.path.splitext(x)[0] for x in os.listdir(DATA_DIR)))


def _read_recursively(name, version=None):
    with open(os.path.join(DATA_DIR, name + '.json')) as f:
        data = json.load(f)

        if 'extends' in data:
            parent = _read_recursively(data['extends'])
            data.update(parent)

        return data


def verify(data):
    for key in ('version-command', 'run-commands', 'parameters', 'version'):
        if not key in data:
            raise LoadError("`{}' key not specified".format(key))


def digest(data):
    s = data['version-command'] + data['version'] + \
        '+'.join(data['run-commands']) + '+'.join(data['parameters'])
    return hashlib.sha256(s).hexdigest()


def load_data(name):
    try:
        data = _read_recursively(name)
    except IOError as e:
        raise LoadError("could not load `{}': {}".format(name, str(e)))

    try:
        verify(data)
    except LoadError as e:
        raise LoadError("could not load `{}': {}".format(name, e))

    return data


def load(name):
    return Runner(load_data(name))
