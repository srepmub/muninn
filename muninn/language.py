#
# Copyright (C) 2014-2019 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import string_types as basestring

import copy
import datetime
import re
import uuid

import muninn.geometry as geometry

from muninn.enum import Enum
from muninn.exceptions import *
from muninn.function import Prototype, FunctionTable
from muninn.schema import *
from muninn.visitor import Visitor

#
# Table of all supported operators and functions.
#
function_table = FunctionTable()

#
# Logical operators.
#
function_table.add(Prototype("not", (Boolean,), Boolean))
function_table.add(Prototype("and", (Boolean, Boolean), Boolean))
function_table.add(Prototype("or", (Boolean, Boolean), Boolean))

#
# Comparison operators.
#
function_table.add(Prototype("==", (Long, Long), Boolean))
function_table.add(Prototype("==", (Long, Integer), Boolean))
function_table.add(Prototype("==", (Integer, Long), Boolean))
function_table.add(Prototype("==", (Integer, Integer), Boolean))
function_table.add(Prototype("==", (Real, Real), Boolean))
function_table.add(Prototype("==", (Real, Long), Boolean))
function_table.add(Prototype("==", (Long, Real), Boolean))
function_table.add(Prototype("==", (Real, Integer), Boolean))
function_table.add(Prototype("==", (Integer, Real), Boolean))
function_table.add(Prototype("==", (Boolean, Boolean), Boolean))
function_table.add(Prototype("==", (Text, Text), Boolean))
function_table.add(Prototype("==", (Timestamp, Timestamp), Boolean))
function_table.add(Prototype("==", (UUID, UUID), Boolean))

function_table.add(Prototype("!=", (Long, Long), Boolean))
function_table.add(Prototype("!=", (Long, Integer), Boolean))
function_table.add(Prototype("!=", (Integer, Long), Boolean))
function_table.add(Prototype("!=", (Integer, Integer), Boolean))
function_table.add(Prototype("!=", (Real, Real), Boolean))
function_table.add(Prototype("!=", (Real, Long), Boolean))
function_table.add(Prototype("!=", (Long, Real), Boolean))
function_table.add(Prototype("!=", (Real, Integer), Boolean))
function_table.add(Prototype("!=", (Integer, Real), Boolean))
function_table.add(Prototype("!=", (Boolean, Boolean), Boolean))
function_table.add(Prototype("!=", (Text, Text), Boolean))
function_table.add(Prototype("!=", (Timestamp, Timestamp), Boolean))
function_table.add(Prototype("!=", (UUID, UUID), Boolean))

function_table.add(Prototype("<", (Long, Long), Boolean))
function_table.add(Prototype("<", (Long, Integer), Boolean))
function_table.add(Prototype("<", (Integer, Long), Boolean))
function_table.add(Prototype("<", (Integer, Integer), Boolean))
function_table.add(Prototype("<", (Real, Real), Boolean))
function_table.add(Prototype("<", (Real, Long), Boolean))
function_table.add(Prototype("<", (Long, Real), Boolean))
function_table.add(Prototype("<", (Real, Integer), Boolean))
function_table.add(Prototype("<", (Integer, Real), Boolean))
function_table.add(Prototype("<", (Text, Text), Boolean))
function_table.add(Prototype("<", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype(">", (Long, Long), Boolean))
function_table.add(Prototype(">", (Long, Integer), Boolean))
function_table.add(Prototype(">", (Integer, Long), Boolean))
function_table.add(Prototype(">", (Integer, Integer), Boolean))
function_table.add(Prototype(">", (Real, Real), Boolean))
function_table.add(Prototype(">", (Real, Long), Boolean))
function_table.add(Prototype(">", (Long, Real), Boolean))
function_table.add(Prototype(">", (Real, Integer), Boolean))
function_table.add(Prototype(">", (Integer, Real), Boolean))
function_table.add(Prototype(">", (Text, Text), Boolean))
function_table.add(Prototype(">", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype("<=", (Long, Long), Boolean))
function_table.add(Prototype("<=", (Long, Integer), Boolean))
function_table.add(Prototype("<=", (Integer, Long), Boolean))
function_table.add(Prototype("<=", (Integer, Integer), Boolean))
function_table.add(Prototype("<=", (Real, Real), Boolean))
function_table.add(Prototype("<=", (Real, Long), Boolean))
function_table.add(Prototype("<=", (Long, Real), Boolean))
function_table.add(Prototype("<=", (Real, Integer), Boolean))
function_table.add(Prototype("<=", (Integer, Real), Boolean))
function_table.add(Prototype("<=", (Text, Text), Boolean))
function_table.add(Prototype("<=", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype(">=", (Long, Long), Boolean))
function_table.add(Prototype(">=", (Long, Integer), Boolean))
function_table.add(Prototype(">=", (Integer, Long), Boolean))
function_table.add(Prototype(">=", (Integer, Integer), Boolean))
function_table.add(Prototype(">=", (Real, Real), Boolean))
function_table.add(Prototype(">=", (Real, Long), Boolean))
function_table.add(Prototype(">=", (Long, Real), Boolean))
function_table.add(Prototype(">=", (Real, Integer), Boolean))
function_table.add(Prototype(">=", (Integer, Real), Boolean))
function_table.add(Prototype(">=", (Text, Text), Boolean))
function_table.add(Prototype(">=", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype("~=", (Text, Text), Boolean))

function_table.add(Prototype("+", (Long,), Long))
function_table.add(Prototype("+", (Integer,), Integer))
function_table.add(Prototype("+", (Real,), Real))

function_table.add(Prototype("-", (Long,), Long))
function_table.add(Prototype("-", (Integer,), Integer))
function_table.add(Prototype("-", (Real,), Real))

function_table.add(Prototype("+", (Long, Long), Long))
function_table.add(Prototype("+", (Long, Integer), Long))
function_table.add(Prototype("+", (Integer, Long), Long))
function_table.add(Prototype("+", (Integer, Integer), Integer))
function_table.add(Prototype("+", (Real, Real), Real))
function_table.add(Prototype("+", (Real, Long), Real))
function_table.add(Prototype("+", (Long, Real), Real))
function_table.add(Prototype("+", (Real, Integer), Real))
function_table.add(Prototype("+", (Integer, Real), Real))

function_table.add(Prototype("-", (Long, Long), Long))
function_table.add(Prototype("-", (Long, Integer), Long))
function_table.add(Prototype("-", (Integer, Long), Long))
function_table.add(Prototype("-", (Integer, Integer), Integer))
function_table.add(Prototype("-", (Real, Real), Real))
function_table.add(Prototype("-", (Real, Long), Real))
function_table.add(Prototype("-", (Long, Real), Real))
function_table.add(Prototype("-", (Real, Integer), Real))
function_table.add(Prototype("-", (Integer, Real), Real))

function_table.add(Prototype("*", (Long, Long), Long))
function_table.add(Prototype("*", (Long, Integer), Long))
function_table.add(Prototype("*", (Integer, Long), Long))
function_table.add(Prototype("*", (Integer, Integer), Integer))
function_table.add(Prototype("*", (Real, Real), Real))
function_table.add(Prototype("*", (Real, Long), Real))
function_table.add(Prototype("*", (Long, Real), Real))
function_table.add(Prototype("*", (Real, Integer), Real))
function_table.add(Prototype("*", (Integer, Real), Real))

function_table.add(Prototype("/", (Long, Long), Long))
function_table.add(Prototype("/", (Long, Integer), Long))
function_table.add(Prototype("/", (Integer, Long), Long))
function_table.add(Prototype("/", (Integer, Integer), Integer))
function_table.add(Prototype("/", (Real, Real), Real))
function_table.add(Prototype("/", (Real, Long), Real))
function_table.add(Prototype("/", (Long, Real), Real))
function_table.add(Prototype("/", (Real, Integer), Real))
function_table.add(Prototype("/", (Integer, Real), Real))

function_table.add(Prototype("-", (Timestamp, Timestamp), Real))

#
# Functions.
#
function_table.add(Prototype("covers", (Geometry, Geometry), Boolean))
function_table.add(Prototype("covers", (Timestamp, Timestamp, Timestamp, Timestamp), Boolean))
function_table.add(Prototype("intersects", (Geometry, Geometry), Boolean))
function_table.add(Prototype("intersects", (Timestamp, Timestamp, Timestamp, Timestamp), Boolean))
function_table.add(Prototype("is_defined", (Long,), Boolean))
function_table.add(Prototype("is_defined", (Integer,), Boolean))
function_table.add(Prototype("is_defined", (Real,), Boolean))
function_table.add(Prototype("is_defined", (Boolean,), Boolean))
function_table.add(Prototype("is_defined", (Text,), Boolean))
function_table.add(Prototype("is_defined", (Timestamp,), Boolean))
function_table.add(Prototype("is_defined", (UUID,), Boolean))
function_table.add(Prototype("is_defined", (Geometry,), Boolean))
function_table.add(Prototype("is_source_of", (UUID,), Boolean))
function_table.add(Prototype("is_derived_from", (UUID,), Boolean))
function_table.add(Prototype("has_tag", (Text,), Boolean))
function_table.add(Prototype("now", (), Timestamp))


class TokenType(Enum):
    _items = ("TEXT", "UUID", "TIMESTAMP", "REAL", "INTEGER", "BOOLEAN", "NAME", "OPERATOR", "END")


class Token(object):
    def __init__(self, type_, value=None):
        self.type_ = type_
        self.value = value

    def __repr__(self):
        return "Token(type_ = TokenType.%s, value = %r)" % (TokenType.to_string(self.type_), self.value)


class TokenStream(object):
    _sub_patterns = \
        (
            r"""\"(?:[^\\"]|\\.)*\"""",                                      # Text literals
            r"""\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{0,6})?)?""",   # Timestamp literals
            r"""[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""",  # UUID literals
            r"""\d+(?:\.\d*(?:[eE][+-]?\d+)?|[eE][+-]?\d+)""",               # Real literals
            r"""\d+""",                                                      # Integer literals
            r"""true|false""",                                               # Boolean literals
            r"""[a-zA-Z]\w*""",                                              # Names
            r"""<=|>=|==|!=|~=|[*<>@(),.+-/]"""                              # Operators and delimiters
        )

    _pattern = r"""(?:%s)""" % ("|".join(["(%s)" % sub_pattern for sub_pattern in _sub_patterns]))
    _re_token = re.compile(_pattern)

    _re_datemin = re.compile(r"0000-00-00(?:T00:00:00(?:\.0{0,6})?)?$")
    _re_datemax = re.compile(r"9999-99-99(?:T99:99:99(?:\.9{0,6})?)?$")

    def __init__(self, text):
        self.text = text
        self.at_end = not self.text
        self.token_start_position, self.token_end_position = 0, 0
        self.next()

    def next(self):
        if self.at_end:
            raise Error("char %d: unexpected end of input" % (self.token_start_position + 1))

        self.token = self._next_token()
        return self.token

    def test(self, types, values=None):
        return False if not self._test_token_types(types) else (values is None or self._test_token_values(values))

    def accept(self, types, values=None):
        if not self.test(types, values):
            return False

        self.next()
        return True

    def expect(self, types, values=None):
        if not self.test(types, values):
            if self.token.type_ == TokenType.END:
                raise Error("char %d: unexpected end of input" % (self.token_start_position + 1))
            else:
                if self.token.value is None:
                    token_str = TokenType.to_string(self.token.type_)
                else:
                    token_str = "\"%s\"" % self.token.value

                expected_str = self._types_to_string(types) if values is None else self._values_to_string(values)
                raise Error("char %d: expected %s, got %s" % (self.token_start_position + 1, expected_str, token_str))

        token = self.token
        self.next()
        return token

    def _types_to_string(self, types):
        try:
            strings = map(TokenType.to_string, types)
        except TypeError:
            return TokenType.to_string(types)

        return "%s%s" % ("" if len(strings) == 1 else "one of: ", ", ".join(strings))

    def _values_to_string(self, values):
        if isinstance(values, basestring):
            return "\"%s\"" % values

        try:
            strings = ["\"%s\"" % value for value in values]
        except TypeError:
            return "\"%s\"" % values

        return "%s%s" % ("" if len(strings) == 1 else "one of: ", ", ".join(strings))

    def _test_token_types(self, types):
        try:
            return self.token.type_ in types
        except TypeError:
            return self.token.type_ == types

    def _test_token_values(self, values):
        if isinstance(values, basestring):
            return self.token.value == values

        try:
            return self.token.value in values
        except TypeError:
            return self.token.value == values

    def _next_token(self):
        self.token_start_position = self._skip_white_space(self.token_end_position)

        if self.token_start_position == len(self.text):
            self.at_end = True
            return Token(TokenType.END)

        match_object = self._re_token.match(self.text, self.token_start_position)
        if match_object is None:
            raise Error("char %d: syntax error: \"%s\"" % (self.token_start_position + 1,
                        self.text[self.token_start_position:]))

        self.token_start_position, self.token_end_position = match_object.span()
        text, timestamp, uuid_, real, integer, boolean, name, operator = match_object.groups()

        if text is not None:
            return Token(TokenType.TEXT, string_unescape(text[1:-1]))

        if uuid_ is not None:
            return Token(TokenType.UUID, uuid.UUID(uuid_))

        if timestamp is not None:
            return Token(TokenType.TIMESTAMP, self._parse_timestamp(timestamp))

        if real is not None:
            return Token(TokenType.REAL, float(real))

        if integer is not None:
            return Token(TokenType.INTEGER, int(integer))

        if boolean is not None:
            return Token(TokenType.BOOLEAN, boolean == "true")

        if name is not None:
            return Token(TokenType.NAME, name)

        if operator is not None:
            return Token(TokenType.OPERATOR, operator)

        raise Error("char %d: syntax error: \"%s\"" % (self.token_start_position + 1, match_object.group()))

    def _skip_white_space(self, start):
        while start < len(self.text) and self.text[start].isspace():
            start += 1
        return start

    def _parse_timestamp(self, timestamp):
        if self._re_datemin.match(timestamp) is not None:
            return datetime.datetime.min

        if self._re_datemax.match(timestamp) is not None:
            return datetime.datetime.max

        for format_string in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.datetime.strptime(timestamp, format_string)
            except ValueError:
                pass

        raise Error("char %d: invalid timestamp: \"%s\"" % (self.token_start_position + 1, timestamp))


class AbstractSyntaxTreeNode(object):
    pass


class Literal(AbstractSyntaxTreeNode):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.value)


class Name(AbstractSyntaxTreeNode):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.value)


class ParameterReference(AbstractSyntaxTreeNode):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.name)


class FunctionCall(AbstractSyntaxTreeNode):
    def __init__(self, name, *args):
        self.name = name
        self.arguments = list(args)

    def __str__(self):
        if not self.arguments:
            return "(%s %s)" % (type(self).__name__, self.name)
        return "(%s %s %s)" % (type(self).__name__, self.name, " ".join(map(str, self.arguments)))


def parse_sequence(stream, parse_item_function):
    stream.expect(TokenType.OPERATOR, "(")
    if stream.accept(TokenType.OPERATOR, ")"):
        return []

    sequence = [parse_item_function(stream)]
    while stream.accept(TokenType.OPERATOR, ","):
        sequence.append(parse_item_function(stream))
    stream.expect(TokenType.OPERATOR, ")")
    return sequence


def parse_geometry_sequence(stream, parse_item_function):
    if stream.accept(TokenType.NAME, "EMPTY"):
        return []

    stream.expect(TokenType.OPERATOR, "(")
    sequence = [parse_item_function(stream)]
    while stream.accept(TokenType.OPERATOR, ","):
        sequence.append(parse_item_function(stream))
    stream.expect(TokenType.OPERATOR, ")")
    return sequence


def parse_signed_coordinate(stream):
    if stream.accept(TokenType.OPERATOR, "-"):
        token = stream.expect((TokenType.INTEGER, TokenType.REAL))
        return -float(token.value)

    stream.accept(TokenType.OPERATOR, "+")
    token = stream.expect((TokenType.INTEGER, TokenType.REAL))
    return float(token.value)


def parse_point_raw(stream):
    return geometry.Point(parse_signed_coordinate(stream),
                          parse_signed_coordinate(stream))


def parse_point(stream):
    stream.expect(TokenType.OPERATOR, "(")
    point = parse_point_raw(stream)
    stream.expect(TokenType.OPERATOR, ")")
    return point


def parse_line_string(stream):
    return geometry.LineString(parse_geometry_sequence(stream, parse_point_raw))


def parse_linear_ring(stream):
    points = parse_geometry_sequence(stream, parse_point_raw)
    if len(points) == 0:
        return geometry.LinearRing()

    if len(points) < 4:
        raise Error("char %d: linear ring should be empty or should contain >= 4 points" % stream.token_start_position)

    if points[-1] != points[0]:
        raise Error("char %d: linear ring should be closed" % stream.token_start_position)

    return geometry.LinearRing(points[:-1])


def parse_polygon(stream):
    return geometry.Polygon(parse_geometry_sequence(stream, parse_linear_ring))


def parse_multi_point(stream):
    return geometry.MultiPoint(parse_geometry_sequence(stream, parse_point))


def parse_multi_line_string(stream):
    return geometry.MultiLineString(parse_geometry_sequence(stream, parse_line_string))


def parse_multi_polygon(stream):
    return geometry.MultiPolygon(parse_geometry_sequence(stream, parse_polygon))


def parse_atom(stream):
    # Sub-expression.
    if stream.accept(TokenType.OPERATOR, "("):
        sub_expression = parse_expression(stream)
        stream.expect(TokenType.OPERATOR, ")")
        return sub_expression

    # Parameter reference.
    if stream.accept(TokenType.OPERATOR, "@"):
        name_token = stream.expect(TokenType.NAME)
        return ParameterReference(name_token.value)

    # Geometry literal, function call, or name.
    if stream.test(TokenType.NAME):
        name_token = stream.expect(TokenType.NAME)

        # Geometry literals.
        if name_token.value == "POINT":
            return Literal(parse_point(stream))
        elif name_token.value == "LINESTRING":
            return Literal(parse_line_string(stream))
        elif name_token.value == "POLYGON":
            return Literal(parse_polygon(stream))
        elif name_token.value == "MULTIPOINT":
            return Literal(parse_multi_point(stream))
        elif name_token.value == "MULTILINESTRING":
            return Literal(parse_multi_line_string(stream))
        elif name_token.value == "MULTIPOLYGON":
            return Literal(parse_multi_polygon(stream))

        # Function call.
        if stream.test(TokenType.OPERATOR, "("):
            return FunctionCall(name_token.value, *parse_sequence(stream, parse_expression))

        # Name (possibly qualified).
        parts = [name_token.value]
        while stream.accept(TokenType.OPERATOR, "."):
            name_token = stream.expect(TokenType.NAME)
            parts.append(name_token.value)
        return Name(".".join(parts))

    # Literal.
    token = stream.expect((TokenType.TEXT, TokenType.TIMESTAMP, TokenType.UUID, TokenType.REAL, TokenType.INTEGER,
                           TokenType.BOOLEAN))
    return Literal(token.value)


def parse_term(stream):
    if stream.test(TokenType.OPERATOR, ("+", "-")):
        operator_token = stream.expect(TokenType.OPERATOR, ("+", "-"))
        return FunctionCall(operator_token.value, parse_term(stream))
    return parse_atom(stream)


def parse_arithmetic_expression(stream):
    lhs = parse_term(stream)
    if stream.test(TokenType.OPERATOR, ("+", "-", "*", "/")):
        operator_token = stream.expect(TokenType.OPERATOR, ("+", "-", "*", "/"))
        return FunctionCall(operator_token.value, lhs, parse_arithmetic_expression(stream))
    return lhs


def parse_comparison(stream):
    lhs = parse_arithmetic_expression(stream)
    if stream.test(TokenType.OPERATOR, ("<", ">", "==", ">=", "<=", "!=", "~=")):
        operator_token = stream.expect(TokenType.OPERATOR, ("<", ">", "==", ">=", "<=", "!=", "~="))
        return FunctionCall(operator_token.value, lhs, parse_comparison(stream))
    return lhs


def parse_not_expression(stream):
    if stream.accept(TokenType.NAME, "not"):
        return FunctionCall("not", parse_not_expression(stream))
    return parse_comparison(stream)


def parse_and_expression(stream):
    lhs = parse_not_expression(stream)
    if stream.accept(TokenType.NAME, "and"):
        return FunctionCall("and", lhs, parse_and_expression(stream))
    return lhs


def parse_or_expression(stream):
    lhs = parse_and_expression(stream)
    if stream.accept(TokenType.NAME, "or"):
        return FunctionCall("or", lhs, parse_or_expression(stream))
    return lhs


def parse_expression(stream):
    return parse_or_expression(stream)


def _literal_type(literal):
    for type in (Text, Timestamp, UUID, Boolean, Integer, Long, Real, Geometry):
        try:
            type.validate(literal)
        except ValueError:
            pass
        else:
            return type

    raise Error("unable to determine type of literal value: %r" % literal)


class SemanticAnalysis(Visitor):
    def __init__(self, namespace_schemas, parameters):
        super(SemanticAnalysis, self).__init__()
        self._namespace_schemas = namespace_schemas
        self._parameters = parameters

    def visit_Literal(self, visitable):
        visitable.type = _literal_type(visitable.value)

    def visit_Name(self, visitable):
        split_name = visitable.value.split(".")

        if len(split_name) == 1:
            namespace, name = "core", split_name[0]
        elif len(split_name) == 2:
            namespace, name = split_name
        else:
            raise Error("invalid property name: \"%s\"" % visitable.value)

        try:
            schema = self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"" % namespace)

        try:
            type = schema[name]
        except KeyError:
            raise Error("no property: \"%s\" defined within namespace: \"%s\"" % (name, namespace))

        visitable.value = "%s.%s" % (namespace, name)
        visitable.type = type

    def visit_ParameterReference(self, visitable):
        try:
            value = self._parameters[visitable.name]
        except KeyError:
            raise Error("no value for parameter: \"%s\"" % visitable.name)

        visitable.value = value
        visitable.type = _literal_type(value)

    def visit_FunctionCall(self, visitable):
        # Resolve the type of the function arguments.
        for argument in visitable.arguments:
            self.visit(argument)

        prototype = Prototype(visitable.name, [argument.type for argument in visitable.arguments])

        try:
            prototypes = function_table.resolve(prototype)
        except KeyError:
            prototypes = []

        if not prototypes:
            raise Error("undefined function: \"%s\"" % prototype)

        if len(prototypes) > 1:
            raise InternalError("cannot uniquely resolve function: \"%s\"" % prototype)

        prototype = prototypes[0]
        visitable.prototype = prototype
        visitable.type = prototype.return_type

    def visit_AbstractSyntaxTreeNode(self, visitable):
        if not hasattr(visitable, "type"):
            raise InternalError("encountered abstract syntax tree node without type attribute: %s" %
                                type(visitable).__name__)

    def default(self, visitable):
        raise InternalError("unsupported abstract syntax tree node type: %s" % type(visitable).__name__)


def parse(text):
    stream = TokenStream(text)
    abstract_syntax_tree = parse_expression(stream)
    if not stream.test(TokenType.END):
        raise Error("char %d: extra characters after expression: \"%s\"" % (stream.token_start_position + 1,
                                                                            text[stream.token_start_position:]))
    return abstract_syntax_tree


def analyze(abstract_syntax_tree, namespace_schemas={}, parameters={}):
    annotated_syntax_tree = copy.deepcopy(abstract_syntax_tree)
    SemanticAnalysis(namespace_schemas, parameters).visit(annotated_syntax_tree)
    return annotated_syntax_tree


def parse_and_analyze(text, namespace_schemas={}, parameters={}):
    return analyze(parse(text), namespace_schemas, parameters)


def string_unescape(text):
    '''
    Unescape special characters in a string.
    Python2 and 3 compatible, uses the native string type.
    In python2, the same effect can also be achieved with `string.decode("string-escape")`
    '''
    text = str(text)  # make sure we are using the native string type
    regex = re.compile('\\\\(\\\\|[\'"abfnrtv])')
    translator = {
        '\\': '\\',
        "'": "'",
        '"': '"',
        'a': '\a',
        'b': '\b',
        'f': '\f',
        'n': '\n',
        'r': '\r',
        't': '\t',
        'v': '\v',
    }

    def _replace(m):
        c = m.group(1)
        return translator[c]

    result = regex.sub(_replace, text)
    return result
