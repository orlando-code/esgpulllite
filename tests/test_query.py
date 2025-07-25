import pytest

from esgpulllite.exceptions import AlreadySetFacet
from esgpulllite.models import Query, Tag

from .utils import dict_equals_ignore


def test_empty_asdict():
    assert dict_equals_ignore(
        Query().asdict(), {}, ignore_keys=["added_at", "updated_at"]
    )


def test_clone_is_deepcopy():
    query = Query(selection=dict(project="CMIP6"))
    clone = query.clone()
    assert dict_equals_ignore(
        query.asdict(),
        clone.asdict(),
        ignore_keys=["added_at", "updated_at"],
    )
    clone.selection.variable_id = "tas"
    assert not dict_equals_ignore(
        query.asdict(),
        clone.asdict(),
        ignore_keys=["added_at", "updated_at"],
    )


def test_combine():
    # various ways to create subqueries
    a = Query(selection=dict(project="CMIP6"))
    b = Query(selection=dict(mip_era="CMIP6"))
    c = Query(options=dict(distrib=None))
    d = Query(options=dict(distrib=True))
    ab_dict = (a << b).selection.asdict()
    ba_dict = (b << a).selection.asdict()
    abcd_dict = (a << b << c << d).asdict()
    dcba_dict = (d << c << b << a).asdict()
    assert dict_equals_ignore(ab_dict, ba_dict, ignore_keys=["added_at", "updated_at"])
    assert dict_equals_ignore(
        ab_dict,
        dict(project="CMIP6", mip_era="CMIP6"),
        ignore_keys=["added_at", "updated_at"],
    )
    assert dict_equals_ignore(
        abcd_dict,
        dict(selection=ab_dict, options=dict(distrib=True)),
        ignore_keys=["added_at", "updated_at"],
    )
    assert dict_equals_ignore(
        dcba_dict,
        dict(selection=ab_dict, options=dict(distrib=None)),
        ignore_keys=["added_at", "updated_at"],
    )


def test_combine_raise():
    a = Query(selection=dict(project="CMIP5"))
    b = Query(selection=dict(project="CMIP6"))
    with pytest.raises(AlreadySetFacet):
        a << b


def test_combine_removes_require():
    a = Query(selection=dict(project="CMIP6"))
    a.compute_sha()
    b = Query(require=a.sha, selection=dict(variable_id="tas"))
    ab = a << b
    ba = b << a
    assert ab.require is None
    assert ba.require == a.sha


def test_set_tags_raise():
    query = Query()
    with pytest.raises(TypeError):
        query.tags = "tag"
    with pytest.raises(TypeError):
        query.tags = Tag(name="tag")
    with pytest.raises(AttributeError):
        query.tags = ["tag"]
        query.compute_sha()


def test_set_tags_ok():
    query = Query()
    query.tags.append(Tag(name="tag"))
    query_dict = query.asdict()
    assert dict_equals_ignore(
        query_dict, {"tags": "tag"}, ignore_keys=["added_at", "updated_at"]
    )
    query_copy = Query(**query_dict)
    query.compute_sha()
    query_copy.compute_sha()
    assert query.tags[0].sha == query_copy.tags[0].sha
