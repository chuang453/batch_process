from utils import nested_dicts as nd


def test_set_get_setdefault_single_and_nested():
    d = {}
    # single key
    nd.set_dict_data(d, 'k', 1)
    assert nd.get_dict_data(d, 'k') == 1
    assert nd.setdefault_dict_data(d, 'k', 0) == 1

    # nested
    nd.set_dict_data(d, ['a', 'b', 'c'], 5)
    assert nd.get_dict_data(d, ['a', 'b', 'c']) == 5
    # get missing returns default
    assert nd.get_dict_data(d, ['a', 'x'], default=None) is None
    # setdefault creates path when missing
    ret = nd.setdefault_dict_data(d, ['a', 'z'], [])
    assert isinstance(ret, list)
    assert nd.get_dict_data(d, ['a', 'z']) == []


def test_delete_and_cleanup():
    d = {}
    nd.set_dict_data(d, ['f1', 't1'], {'rows': [1]})
    nd.set_dict_data(d, ['f1', 't2'], {'rows': [2]})
    # remove t1
    assert nd.delete_dict_data(d, ['f1', 't1']) is True
    # t2 should still exist
    assert nd.get_dict_data(d, ['f1', 't2']) == {'rows': [2]}
    # remove t2, parent f1 should be cleaned up
    assert nd.delete_dict_data(d, ['f1', 't2']) is True
    assert nd.get_dict_data(d, ['f1']) == None

    # deleting non-existent returns False
    assert nd.delete_dict_data(d, ['no', 'such']) is False


def test_list_dict_keys_and_prefix():
    d = {}
    nd.set_dict_data(d, ['a', 'b'], 1)
    nd.set_dict_data(d, ['a', 'c', 'd'], 2)
    nd.set_dict_data(d, ['x'], 3)

    all_paths = nd.list_dict_keys(d)
    # convert to tuples for easier membership checks
    tup = [tuple(p) for p in all_paths]
    assert ('a', 'b') in tup
    assert ('a', 'c', 'd') in tup
    assert ('x', ) in tup or ['x'] in all_paths

    # prefix listing
    a_paths = nd.list_dict_keys(d, ['a'])
    atup = [tuple(p) for p in a_paths]
    assert ('a', 'b') in atup
    assert ('a', 'c', 'd') in atup

    print(d)
    print(all_paths)


def test_nonlist_key_delete_and_getdefault_behavior():
    d = {'k': 10}
    assert nd.get_dict_data(d, 'k') == 10
    assert nd.setdefault_dict_data(d, 'k', 0) == 10
    assert nd.delete_dict_data(d, 'k') is True
    assert nd.get_dict_data(d, 'k') is None


def test_flatten_and_unflatten_roundtrip():
    d = {'a': {'b': 1, 'c': {'d': 2}}, 'x': {}}
    flat = nd.flatten_dict(d)
    # expected flat keys
    assert ("a", "b") in flat
    assert ("a", "c", "d") in flat
    assert ("x", ) in flat

    # reconstruct
    recon = nd.unflatten_dict(flat)
    assert recon == d


def test_flatten_unflatten_serializers():
    d = {'a': {'b': 1, 'c': {'d': 2}}, 'x': {}}

    # json serializer -> keys are JSON strings
    flat_json = nd.flatten_dict(d, serializer='json')
    # keys should be JSON strings
    for k in flat_json.keys():
        assert isinstance(k, str)
        # decodable as list
        decoded = nd.unflatten_dict({k: flat_json[k]}, serializer='json')
        # unflattening single key should embed value correctly
        # collect reconstructed and merge

    recon_json = nd.unflatten_dict(flat_json, serializer='json')
    assert recon_json == d

    # sep serializer -> keys are sep-joined strings
    flat_sep = nd.flatten_dict(d, serializer='sep', sep='|')
    for k in flat_sep.keys():
        assert isinstance(k, str)
        assert '|' in k or k == 'x'
    recon_sep = nd.unflatten_dict(flat_sep, serializer='sep', sep='|')
    assert recon_sep == d

    # auto should infer json and sep styles
    recon_auto_json = nd.unflatten_dict(flat_json, serializer='auto', sep='|')
    assert recon_auto_json == d
    recon_auto_sep = nd.unflatten_dict(flat_sep, serializer='auto', sep='|')
    assert recon_auto_sep == d
