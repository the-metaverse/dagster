import re
import string
from collections import namedtuple
from enum import Enum
from typing import NamedTuple, Set

import pytest

from dagster import _seven as seven
from dagster._check import ParameterCheckError, inst_param, set_param
from dagster._serdes.errors import DeserializationError, SerdesUsageError, SerializationError
from dagster._serdes.serdes import (
    DefaultEnumSerializer,
    DefaultNamedTupleSerializer,
    EnumSerializer,
    WhitelistMap,
    _deserialize_json,
    _serialize_dagster_namedtuple,
    _whitelist_for_serdes,
    deserialize_json_to_dagster_namedtuple,
    deserialize_value,
    pack_inner_value,
    register_serdes_enum_fallbacks,
    register_serdes_tuple_fallbacks,
    serialize_value,
    unpack_inner_value,
)
from dagster._serdes.utils import create_snapshot_id, hash_str


def test_deserialize_value_ok():
    unpacked_tuple = deserialize_value('{"foo": "bar"}')
    assert unpacked_tuple
    assert unpacked_tuple["foo"] == "bar"


def test_deserialize_json_non_namedtuple():
    with pytest.raises(DeserializationError, match="was not expected type"):
        deserialize_json_to_dagster_namedtuple('{"foo": "bar"}')


@pytest.mark.parametrize("bad_obj", [1, None, False])
def test_deserialize_json_invalid_types(bad_obj):
    with pytest.raises(ParameterCheckError):
        deserialize_json_to_dagster_namedtuple(bad_obj)


def test_deserialize_empty_set():
    assert set() == deserialize_value(serialize_value(set()))
    assert frozenset() == deserialize_value(serialize_value(frozenset()))


def test_descent_path():
    class Foo(NamedTuple):
        bar: int

    with pytest.raises(SerializationError, match=re.escape("Descent path: <root:dict>.a.b[2].c")):
        serialize_value({"a": {"b": [{}, {}, {"c": Foo(1)}]}})

    test_map = WhitelistMap.create()
    blank_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class Fizz(NamedTuple):
        buzz: int

    ser = _serialize_dagster_namedtuple(
        {"a": {"b": [{}, {}, {"c": Fizz(1)}]}}, whitelist_map=test_map
    )

    with pytest.raises(DeserializationError, match=re.escape("Descent path: <root:dict>.a.b[2].c")):
        _deserialize_json(ser, whitelist_map=blank_map)


def test_forward_compat_serdes_new_field_with_default():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class Quux(namedtuple("_Quux", "foo bar")):
        def __new__(cls, foo, bar):
            return super(Quux, cls).__new__(cls, foo, bar)  # pylint: disable=bad-super-call

    assert test_map.has_tuple_entry("Quux")
    klass, _, _ = test_map.get_tuple_entry("Quux")
    assert klass is Quux

    quux = Quux("zip", "zow")

    serialized = _serialize_dagster_namedtuple(quux, whitelist_map=test_map)

    # pylint: disable=function-redefined
    @_whitelist_for_serdes(whitelist_map=test_map)
    class Quux(namedtuple("_Quux", "foo bar baz")):  # pylint: disable=bad-super-call
        def __new__(cls, foo, bar, baz=None):
            return super(Quux, cls).__new__(cls, foo, bar, baz=baz)

    assert test_map.has_tuple_entry("Quux")

    klass, _, _ = test_map.get_tuple_entry("Quux")
    assert klass is Quux

    deserialized = _deserialize_json(serialized, whitelist_map=test_map)

    assert deserialized != quux
    assert deserialized.foo == quux.foo
    assert deserialized.bar == quux.bar
    assert deserialized.baz is None


def test_forward_compat_serdes_new_enum_field():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class Corge(Enum):
        FOO = 1
        BAR = 2

    assert test_map.has_enum_entry("Corge")

    corge = Corge.FOO

    packed = pack_inner_value(corge, whitelist_map=test_map, descent_path="")

    # pylint: disable=function-redefined
    @_whitelist_for_serdes(whitelist_map=test_map)
    class Corge(Enum):
        FOO = 1
        BAR = 2
        BAZ = 3

    unpacked = unpack_inner_value(packed, whitelist_map=test_map, descent_path="")

    assert unpacked != corge
    assert unpacked.name == corge.name
    assert unpacked.value == corge.value


def test_serdes_enum_backcompat():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class Corge(Enum):
        FOO = 1
        BAR = 2

    assert test_map.has_enum_entry("Corge")

    corge = Corge.FOO

    packed = pack_inner_value(corge, whitelist_map=test_map, descent_path="")

    class CorgeBackCompatSerializer(DefaultEnumSerializer):
        @classmethod
        def value_from_storage_str(cls, storage_str, klass):
            if storage_str == "FOO":
                value = "FOO_FOO"
            else:
                value = storage_str

            return super().value_from_storage_str(value, klass)

    # pylint: disable=function-redefined
    @_whitelist_for_serdes(whitelist_map=test_map, serializer=CorgeBackCompatSerializer)
    class Corge(Enum):
        BAR = 2
        BAZ = 3
        FOO_FOO = 4

    unpacked = unpack_inner_value(packed, whitelist_map=test_map, descent_path="")

    assert unpacked != corge
    assert unpacked == Corge.FOO_FOO


def test_backward_compat_serdes():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class Quux(namedtuple("_Quux", "foo bar baz")):
        def __new__(cls, foo, bar, baz):
            return super(Quux, cls).__new__(cls, foo, bar, baz)  # pylint: disable=bad-super-call

    quux = Quux("zip", "zow", "whoopie")

    serialized = _serialize_dagster_namedtuple(quux, whitelist_map=test_map)

    # pylint: disable=function-redefined
    @_whitelist_for_serdes(whitelist_map=test_map)
    class Quux(namedtuple("_Quux", "foo bar")):  # pylint: disable=bad-super-call
        def __new__(cls, foo, bar):
            return super(Quux, cls).__new__(cls, foo, bar)

    deserialized = _deserialize_json(serialized, whitelist_map=test_map)

    assert deserialized != quux
    assert deserialized.foo == quux.foo
    assert deserialized.bar == quux.bar
    assert not hasattr(deserialized, "baz")


def test_forward_compat():
    old_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=old_map)
    class Quux(namedtuple("_Quux", "bar baz")):
        def __new__(cls, bar, baz):
            return super().__new__(cls, bar, baz)

    # new version has a new field with a new type
    new_map = WhitelistMap.create()

    # pylint: disable=function-redefined
    @_whitelist_for_serdes(whitelist_map=new_map)
    class Quux(namedtuple("_Quux", "foo bar baz")):
        def __new__(cls, foo, bar, baz):
            return super().__new__(cls, foo, bar, baz)

    @_whitelist_for_serdes(whitelist_map=new_map)
    class Foo(namedtuple("_Foo", "wow")):
        def __new__(cls, wow):
            return super().__new__(cls, wow)

    new_quux = Quux(foo=Foo("wow"), bar="bar", baz="baz")

    # write from new
    serialized = _serialize_dagster_namedtuple(new_quux, whitelist_map=new_map)

    # read from old, foo ignored
    deserialized = _deserialize_json(serialized, whitelist_map=old_map)
    assert deserialized.bar == "bar"
    assert deserialized.baz == "baz"


def serdes_test_class(klass):
    test_map = WhitelistMap.create()

    return _whitelist_for_serdes(whitelist_map=test_map)(klass)


def test_wrong_first_arg():
    with pytest.raises(SerdesUsageError) as exc_info:

        @serdes_test_class
        class NotCls(namedtuple("NotCls", "field_one field_two")):
            def __new__(not_cls, field_two, field_one):
                return super(NotCls, not_cls).__new__(field_one, field_two)

    assert (
        str(exc_info.value)
        == 'For namedtuple NotCls: First parameter must be _cls or cls. Got "not_cls".'
    )


def test_incorrect_order():
    with pytest.raises(SerdesUsageError) as exc_info:

        @serdes_test_class
        class WrongOrder(namedtuple("WrongOrder", "field_one field_two")):
            def __new__(cls, field_two, field_one):
                return super(WrongOrder, cls).__new__(field_one, field_two)

    assert str(exc_info.value) == (
        "For namedtuple WrongOrder: "
        "Params to __new__ must match the order of field declaration "
        "in the namedtuple. Declared field number 1 in the namedtuple "
        'is "field_one". Parameter 1 in __new__ method is "field_two".'
    )


def test_missing_one_parameter():
    with pytest.raises(SerdesUsageError) as exc_info:

        @serdes_test_class
        class MissingFieldInNew(namedtuple("MissingFieldInNew", "field_one field_two field_three")):
            def __new__(cls, field_one, field_two):
                return super(MissingFieldInNew, cls).__new__(field_one, field_two, None)

    assert str(exc_info.value) == (
        "For namedtuple MissingFieldInNew: "
        "Missing parameters to __new__. You have declared fields in "
        "the named tuple that are not present as parameters to the "
        "to the __new__ method. In order for both serdes serialization "
        "and pickling to work, these must match. Missing: ['field_three']"
    )


def test_missing_many_parameters():
    with pytest.raises(SerdesUsageError) as exc_info:

        @serdes_test_class
        class MissingFieldsInNew(
            namedtuple("MissingFieldsInNew", "field_one field_two field_three, field_four")
        ):
            def __new__(cls, field_one, field_two):
                return super(MissingFieldsInNew, cls).__new__(field_one, field_two, None, None)

    assert str(exc_info.value) == (
        "For namedtuple MissingFieldsInNew: "
        "Missing parameters to __new__. You have declared fields in "
        "the named tuple that are not present as parameters to the "
        "to the __new__ method. In order for both serdes serialization "
        "and pickling to work, these must match. Missing: ['field_three', 'field_four']"
    )


def test_extra_parameters_must_have_defaults():
    with pytest.raises(SerdesUsageError) as exc_info:

        @serdes_test_class
        class OldFieldsWithoutDefaults(
            namedtuple("OldFieldsWithoutDefaults", "field_three field_four")
        ):
            # pylint:disable=unused-argument
            def __new__(
                cls,
                field_three,
                field_four,
                # Graveyard Below
                field_one,
                field_two,
            ):
                return super(OldFieldsWithoutDefaults, cls).__new__(field_three, field_four)

    assert str(exc_info.value) == (
        "For namedtuple OldFieldsWithoutDefaults: "
        'Parameter "field_one" is a parameter to the __new__ '
        "method but is not a field in this namedtuple. "
        "The only reason why this should exist is that "
        "it is a field that used to exist (we refer to "
        "this as the graveyard) but no longer does. However "
        "it might exist in historical storage. This parameter "
        "existing ensures that serdes continues to work. However "
        "these must come at the end and have a default value for pickling to work."
    )


def test_extra_parameters_have_working_defaults():
    @serdes_test_class
    class OldFieldsWithDefaults(namedtuple("OldFieldsWithDefaults", "field_three field_four")):
        # pylint:disable=unused-argument
        def __new__(
            cls,
            field_three,
            field_four,
            # Graveyard Below
            none_field=None,
            falsey_field=0,
            another_falsey_field="",
            value_field="klsjkfjd",
        ):
            return super(OldFieldsWithDefaults, cls).__new__(field_three, field_four)


def test_set():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class HasSets(namedtuple("_HasSets", "reg_set frozen_set")):
        def __new__(cls, reg_set, frozen_set):
            set_param(reg_set, "reg_set")
            inst_param(frozen_set, "frozen_set", frozenset)
            return super(HasSets, cls).__new__(cls, reg_set, frozen_set)

    foo = HasSets({1, 2, 3, "3"}, frozenset([4, 5, 6, "6"]))

    serialized = _serialize_dagster_namedtuple(foo, whitelist_map=test_map)
    foo_2 = _deserialize_json(serialized, whitelist_map=test_map)
    assert foo == foo_2

    # verify that set elements are serialized in a consistent order so that
    # equal objects always have a consistent serialization / snapshot ID
    big_foo = HasSets(set(string.ascii_lowercase), frozenset(string.ascii_lowercase))

    snap_id = hash_str(_serialize_dagster_namedtuple(big_foo, whitelist_map=test_map))
    roundtrip_snap_id = hash_str(
        _serialize_dagster_namedtuple(
            _deserialize_json(
                _serialize_dagster_namedtuple(big_foo, whitelist_map=test_map),
                whitelist_map=test_map,
            ),
            whitelist_map=test_map,
        )
    )
    assert snap_id == roundtrip_snap_id


def test_persistent_tuple():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class Alphabet(namedtuple("_Alphabet", "a b c")):
        def __new__(cls, a, b, c):
            return super(Alphabet, cls).__new__(cls, a, b, c)

    foo = Alphabet(a="A", b="B", c="C")
    serialized = _serialize_dagster_namedtuple(foo, whitelist_map=test_map)
    foo_2 = _deserialize_json(serialized, whitelist_map=test_map)
    assert foo == foo_2


def test_from_storage_dict():
    old_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=old_map)
    class MyThing(NamedTuple):
        orig_name: str

    serialized_old = _serialize_dagster_namedtuple(MyThing("old"), whitelist_map=old_map)

    class CompatSerializer(DefaultNamedTupleSerializer):
        @classmethod
        def value_from_storage_dict(
            cls, storage_dict, klass, args_for_class, whitelist_map, descent_path
        ):
            # simplified demo of field renaming
            return klass(storage_dict.get("orig_name") or storage_dict.get("new_name"))

    new_map = WhitelistMap.create()

    @_whitelist_for_serdes(
        whitelist_map=new_map, serializer=CompatSerializer
    )  # pylint: disable=function-redefined
    class MyThing(NamedTuple):
        new_name: str

    deser_old_val = _deserialize_json(serialized_old, whitelist_map=new_map)

    assert deser_old_val.new_name == "old"

    serialized_new = _serialize_dagster_namedtuple(MyThing("new"), whitelist_map=new_map)
    deser_new_val = _deserialize_json(serialized_new, whitelist_map=new_map)
    assert deser_new_val.new_name == "new"


def test_from_unpacked():
    test_map = WhitelistMap.create()

    class CompatSerializer(DefaultNamedTupleSerializer):
        @classmethod
        def value_from_unpacked(cls, unpacked_dict, klass):
            return DeprecatedAlphabet.legacy_load(unpacked_dict)

    @_whitelist_for_serdes(whitelist_map=test_map, serializer=CompatSerializer)
    class DeprecatedAlphabet(namedtuple("_DeprecatedAlphabet", "a b c")):
        def __new__(cls, a, b, c):
            raise Exception("DeprecatedAlphabet is deprecated")

        @classmethod
        def legacy_load(cls, storage_dict):
            # instead of the DeprecatedAlphabet, directly invoke the namedtuple constructor
            return super().__new__(
                cls,
                storage_dict.get("a"),
                storage_dict.get("b"),
                storage_dict.get("c"),
            )

    serialized = '{"__class__": "DeprecatedAlphabet", "a": "A", "b": "B", "c": "C"}'

    nt = _deserialize_json(serialized, whitelist_map=test_map)
    assert isinstance(nt, DeprecatedAlphabet)


def test_skip_when_empty():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class SameSnapshotTuple(namedtuple("_Tuple", "foo")):
        def __new__(cls, foo):
            return super(SameSnapshotTuple, cls).__new__(cls, foo)  # pylint: disable=bad-super-call

    old_tuple = SameSnapshotTuple(foo="A")
    old_serialized = _serialize_dagster_namedtuple(old_tuple, whitelist_map=test_map)
    old_snapshot = hash_str(old_serialized)

    # Without setting skip_when_empty, the ID changes

    @_whitelist_for_serdes(whitelist_map=test_map)  # pylint: disable=function-redefined
    class SameSnapshotTuple(namedtuple("_Tuple", "foo bar")):
        def __new__(cls, foo, bar=None):
            return super(SameSnapshotTuple, cls).__new__(  # pylint: disable=bad-super-call
                cls, foo, bar
            )

    new_tuple_without_serializer = SameSnapshotTuple(foo="A")
    new_snapshot_without_serializer = hash_str(
        _serialize_dagster_namedtuple(new_tuple_without_serializer, whitelist_map=test_map)
    )

    assert new_snapshot_without_serializer != old_snapshot

    # By setting a custom serializer and skip_when_empty, the snapshot stays the same
    # as long as the new field is None

    class SkipWhenEmptySerializer(DefaultNamedTupleSerializer):
        @classmethod
        def skip_when_empty(cls) -> Set[str]:
            return {"bar"}

    @_whitelist_for_serdes(
        whitelist_map=test_map, serializer=SkipWhenEmptySerializer
    )  # pylint: disable=function-redefined
    class SameSnapshotTuple(namedtuple("_Tuple", "foo bar")):
        def __new__(cls, foo, bar=None):
            return super(SameSnapshotTuple, cls).__new__(  # pylint: disable=bad-super-call
                cls, foo, bar
            )

    for bar_val in [None, [], {}, set()]:
        new_tuple = SameSnapshotTuple(foo="A", bar=bar_val)
        new_snapshot = hash_str(_serialize_dagster_namedtuple(new_tuple, whitelist_map=test_map))

        assert old_snapshot == new_snapshot

        rehydrated_tuple = _deserialize_json(old_serialized, whitelist_map=test_map)
        assert rehydrated_tuple.foo == "A"
        assert rehydrated_tuple.bar is None

    new_tuple_with_bar = SameSnapshotTuple(foo="A", bar="B")
    assert new_tuple_with_bar.foo == "A"
    assert new_tuple_with_bar.bar == "B"


def test_to_storage_value():
    test_map = WhitelistMap.create()

    class MySerializer(DefaultNamedTupleSerializer):
        @classmethod
        def value_to_storage_dict(cls, value, whitelist_map, descent_path):
            return DefaultNamedTupleSerializer.value_to_storage_dict(
                SubstituteAlphabet(value.a, value.b, value.c),
                test_map,
                descent_path,
            )

    @_whitelist_for_serdes(whitelist_map=test_map, serializer=MySerializer)
    class DeprecatedAlphabet(namedtuple("_DeprecatedAlphabet", "a b c")):
        def __new__(cls, a, b, c):
            return super(DeprecatedAlphabet, cls).__new__(cls, a, b, c)

    @_whitelist_for_serdes(whitelist_map=test_map)
    class SubstituteAlphabet(namedtuple("_SubstituteAlphabet", "a b c")):
        def __new__(cls, a, b, c):
            return super(SubstituteAlphabet, cls).__new__(cls, a, b, c)

    nested = DeprecatedAlphabet(None, None, "_C")
    deprecated = DeprecatedAlphabet("A", "B", nested)
    serialized = _serialize_dagster_namedtuple(deprecated, whitelist_map=test_map)
    alphabet = _deserialize_json(serialized, whitelist_map=test_map)
    assert not isinstance(alphabet, DeprecatedAlphabet)
    assert isinstance(alphabet, SubstituteAlphabet)
    assert not isinstance(alphabet.c, DeprecatedAlphabet)
    assert isinstance(alphabet.c, SubstituteAlphabet)


def test_long_int():
    test_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=test_map)
    class NumHolder(NamedTuple):
        num: int

    x = NumHolder(98765432109876543210)
    ser_x = _serialize_dagster_namedtuple(x, test_map)
    roundtrip_x = _deserialize_json(ser_x, test_map)
    assert x.num == roundtrip_x.num


def test_enum_backcompat():
    test_env = WhitelistMap.create()

    class MyEnumSerializer(EnumSerializer):
        @classmethod
        def value_from_storage_str(cls, storage_str, klass):
            return getattr(klass, storage_str)

        @classmethod
        def value_to_storage_str(cls, value, whitelist_map, descent_path):
            val_as_str = str(value)
            actual_enum_val = val_as_str.split(".")[1:]
            backcompat_name = (
                "OldEnum"  # Simulate changing the storage name to some legacy backcompat name
            )
            return ".".join([backcompat_name, *actual_enum_val])

    @_whitelist_for_serdes(test_env, serializer=MyEnumSerializer)
    class MyEnum(Enum):
        RED = "color.red"
        BLUE = "color.red"

    # Ensure that serdes roundtrip preserves value
    register_serdes_enum_fallbacks({"OldEnum": MyEnum}, whitelist_map=test_env)

    my_enum = MyEnum("color.red")
    enum_json = serialize_value(my_enum, whitelist_map=test_env)
    result = _deserialize_json(enum_json, test_env)
    assert result == my_enum

    # ensure that "legacy" environment can correctly interpret enum stored under legacy name.
    legacy_env = WhitelistMap.create()

    @_whitelist_for_serdes(legacy_env)
    class OldEnum(Enum):
        RED = "color.red"
        BLUE = "color.blue"

    result = _deserialize_json(enum_json, legacy_env)
    old_enum = OldEnum("color.red")
    assert old_enum == result


def test_namedtuple_backcompat():
    old_map = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=old_map)
    class OldThing(NamedTuple):
        old_name: str

        def get_id(self):
            json_rep = _serialize_dagster_namedtuple(self, whitelist_map=old_map)
            return hash_str(json_rep)

    # create the old things
    old_thing = OldThing("thing")
    old_thing_id = old_thing.get_id()
    old_thing_serialized = _serialize_dagster_namedtuple(old_thing, old_map)

    new_map = WhitelistMap.create()

    class ThingSerializer(DefaultNamedTupleSerializer):
        @classmethod
        def value_from_storage_dict(
            cls, storage_dict, klass, args_for_class, whitelist_map, descent_path
        ):
            raw_dict = {
                key: unpack_inner_value(value, whitelist_map, f"{descent_path}.{key}")
                for key, value in storage_dict.items()
            }
            # typical pattern is to use the same serialization format from an old field and passing
            # it in as a new field
            return klass(
                **{key: value for key, value in raw_dict.items() if key in args_for_class},
                new_name=raw_dict.get("old_name"),
            )

        @classmethod
        def value_to_storage_dict(
            cls,
            value,
            whitelist_map,
            descent_path,
        ):
            storage = super().value_to_storage_dict(value, whitelist_map, descent_path)
            name = storage.get("new_name") or storage.get("old_name")
            if "new_name" in storage:
                del storage["new_name"]
            # typical pattern is to use the same serialization format
            # it in as a new field
            storage["old_name"] = name
            storage["__class__"] = "OldThing"  # persist using old class name
            return storage

    @_whitelist_for_serdes(whitelist_map=new_map, serializer=ThingSerializer)
    class NewThing(NamedTuple):
        new_name: str

        def get_id(self):
            json_rep = _serialize_dagster_namedtuple(self, whitelist_map=new_map)
            return hash_str(json_rep)

    # exercising the old serialization format
    register_serdes_tuple_fallbacks({"OldThing": NewThing}, whitelist_map=new_map)

    new_thing = NewThing("thing")
    new_thing_id = new_thing.get_id()
    new_thing_serialized = _serialize_dagster_namedtuple(new_thing, new_map)

    assert new_thing_id == old_thing_id
    assert new_thing_serialized == old_thing_serialized

    # ensure that the new serializer can correctly interpret old serialized data
    old_thing_deserialized = _deserialize_json(old_thing_serialized, new_map)
    assert isinstance(old_thing_deserialized, NewThing)
    assert old_thing_deserialized.get_id() == new_thing_id

    # ensure that the new things serialized can still be read by old code
    new_thing_deserialized = _deserialize_json(new_thing_serialized, old_map)
    assert isinstance(new_thing_deserialized, OldThing)
    assert new_thing_deserialized.get_id() == old_thing_id


def test_namedtuple_name_map():

    wmap = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=wmap)
    class Thing(NamedTuple):
        name: str

    wmap.register_serialized_name("Thing", "SerializedThing")
    thing = Thing("foo")

    thing_serialized = _serialize_dagster_namedtuple(thing, wmap)
    assert seven.json.loads(thing_serialized)["__class__"] == "SerializedThing"

    with pytest.raises(DeserializationError):
        _deserialize_json(thing_serialized, wmap)

    wmap.register_deserialized_name("SerializedThing", "Thing")
    assert _deserialize_json(thing_serialized, wmap) == thing


def test_whitelist_storage_name():

    wmap = WhitelistMap.create()

    @_whitelist_for_serdes(whitelist_map=wmap, storage_name="SerializedThing")
    class Thing(NamedTuple):
        name: str

    assert wmap.get_serialized_name("Thing") == "SerializedThing"
    assert wmap.get_deserialized_name("SerializedThing") == "Thing"
