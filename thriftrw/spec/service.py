# Copyright (c) 2015 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import absolute_import, unicode_literals, print_function

from collections import namedtuple

from thriftrw.compile.exceptions import ThriftCompilerError

from .spec_mapper import type_spec_or_ref
from .struct import StructTypeSpec, FieldSpec
from .union import UnionTypeSpec


__all__ = [
    'ServiceSpec', 'FunctionSpec', 'ServiceFunction',
]


class FunctionArgsSpec(StructTypeSpec):
    """Represents the parameters of a service function.

    The parameters of a function implicitly form a struct which contains the
    parameters as its fields, which are optional by default.
    """

    # TODO is it worth exposing FunctionArgsSpec and FunctionResultSpec with
    # their own attributes?

    @classmethod
    def compile(cls, parameters, service_name, function_name):
        """Compiles a parameter list into a FunctionArgsSpec.

        :param parameters:
            Collection of ``thriftrw.idl.Field`` objects.
        :param str service_name:
            Name of the service under which the function was defined.
        :param str function_name:
            Name of the function whose parameter list is represented by this
            object.
        """
        args_name = str('%s_%s_request' % (service_name, function_name))
        param_specs = [
            FieldSpec.compile(
                field=param,
                struct_name=args_name,
                require_requiredness=False,
            ) for param in parameters
        ]

        return cls(args_name, param_specs)


class FunctionResultSpec(UnionTypeSpec):
    """Represents the result of a service function.

    The return value of a function and the exceptions raised by it implicitly
    form a union which contains the return value at field ID ``0`` and the
    exceptions on the remaining field IDs.
    """

    @classmethod
    def compile(cls, return_type, exceptions, service_name, function_name):
        """Compiles information from the AST into a FunctionResultSpec.

        :param return_type:
            A ``thriftrw.idl.Type`` representing the return type or None if
            the function doesn't return anything.
        :param exceptions:
            Collection of ``thriftrw.idl.Field`` objects representing raised
            by the function.
        :param str service_name:
            Name of the service under which the function was defined.
        :param str function_name:
            Name of the function whose result this object represents.
        """
        result_name = str('%s_%s_response' % (service_name, function_name))

        result_specs = []
        if return_type is not None:
            result_specs.append(
                FieldSpec(
                    id=0,
                    name='success',
                    spec=type_spec_or_ref(return_type),
                    required=False,
                    default_value=None
                )
            )

        exceptions = exceptions or []
        for exc in exceptions:
            result_specs.append(
                FieldSpec.compile(
                    field=exc,
                    struct_name=result_name,
                    require_requiredness=False,
                )
            )

        return cls(result_name, result_specs, allow_empty=True)


class FunctionSpec(object):
    """Specification of a single function on a service.

    The ``surface`` for a FunctionSpec is a :py:class:`ServiceFunction`
    object. Unlike the ``surface`` for other specs, a FunctionSpec's surface
    is attached to the service class as a class attribute.
    """

    __slots__ = ('name', 'args_spec', 'result_spec', 'linked', 'surface')

    def __init__(self, name, args_spec, result_spec):
        #: Name of the function.
        self.name = name

        #: TypeSpec specifying the arguments accepted by this function as a
        #: struct.
        self.args_spec = args_spec

        #: TypeSpec specifying the output of this function as a union of the
        #: return type and the exceptions raised by the function.
        #:
        #: The return type of the function (if any) is a field in the union
        #: with field ID 0 and name 'success'.
        self.result_spec = result_spec

        self.linked = False
        self.surface = None

    @classmethod
    def compile(cls, func, service_name):
        if func.oneway:
            raise ThriftCompilerError(
                'Function "%s.%s" is oneway. '
                'Oneway functions are not supported by thriftrw.'
                % (service_name, func.name)
            )

        args_spec = FunctionArgsSpec.compile(
            parameters=func.parameters,
            service_name=service_name,
            function_name=func.name,
        )

        result_spec = FunctionResultSpec.compile(
            return_type=func.return_type,
            exceptions=func.exceptions,
            service_name=service_name,
            function_name=func.name,
        )

        return cls(func.name, args_spec, result_spec)

    def link(self, scope):
        if not self.linked:
            self.linked = True
            self.args_spec = self.args_spec.link(scope)
            self.result_spec = self.result_spec.link(scope)
            self.surface = ServiceFunction(
                self.name,
                self.args_spec.surface,
                self.result_spec.surface,
            )
        return self

    def __str__(self):
        return 'FunctionSpec(name=%r, args_spec=%r, result_spec=%r)' % (
            self.name, self.args_spec, self.result_spec
        )

    __repr__ = __str__


class ServiceSpec(object):
    """Spec for a single service.

    The ``surface`` for a ``ServiceSpec`` is a class that has the following
    attributes:

    ``service_spec``
        Reference back to the service spec.

    And a reference to one :py:class:`ServiceFunction` object for each
    function defined in the service.
    """

    __slots__ = ('name', 'functions', 'parent', 'linked', 'surface')

    def __init__(self, name, functions, parent):
        #: Name of the service.
        self.name = name

        #: Collection of :py:class:`FunctionSpec` objects.
        self.functions = functions

        #: ServiceSpec of the parent service or None if this service does not
        #: inherit from anything.
        self.parent = parent

        self.linked = False
        self.surface = None

    @classmethod
    def compile(cls, service):
        functions = []
        names = set()

        for func in service.functions:
            if func.name in names:
                raise ThriftCompilerError(
                    'Function "%s.%s" cannot be defined. '
                    'That name is already taken.'
                    % (service.name, func.name)
                )
            names.add(func.name)
            functions.append(
                FunctionSpec.compile(func, service.name)
            )

        return cls(service.name, functions, service.parent)

    def link(self, scope):
        if not self.linked:
            self.linked = True

            if self.parent is not None:
                if self.parent not in scope.service_specs:
                    raise ThriftCompilerError(
                        'Service "%s" inherits from unknown service "%s"'
                        % (self.name, self.parent)
                    )
                self.parent = scope.service_specs[self.parent].link(scope)

            self.functions = [func.link(scope) for func in self.functions]
            self.surface = service_cls(self, scope)

        return self

    def __str__(self):
        return 'ServiceSpec(name=%r, functions=%r, parent=%r)' % (
            self.name, self.functions, self.parent
        )

    __repr__ = __str__


class ServiceFunction(namedtuple('ServiceFunction', 'name request response')):
    """Represents a single function on a service.

    ``name``
        Name of the function.
    ``request``
        Class representing requests for this function.
    ``response``
        Class representing responses for this function.
    """


def service_cls(service_spec, scope):
    """Generates a class from the given service spec.

    :param ServiceSpec service_spec:
        Specification of the service.
    :param scope:
        Compilation scope.
    """
    parent_cls = object
    if service_spec.parent is not None:
        parent_cls = service_spec.parent.surface

    service_dct = {}
    for function in service_spec.functions:
        service_dct[function.name] = function.surface

    service_dct['service_spec'] = service_spec
    service_dct['__slots__'] = ()

    return type(str(service_spec.name), (parent_cls,), service_dct)
