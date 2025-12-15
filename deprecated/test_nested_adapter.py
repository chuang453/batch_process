import json
from utils.adapters.nested_table_adapter import NestedDictTableAdapter


def test_basic_crud_and_persistence(tmp_path):
    a = NestedDictTableAdapter()

    fp = ["data", "file1.txt"]
    # use list path for keys
    fp_str = fp

    # initially empty
    assert a.list_tables() == []

    # add a table
    rows = [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
    a.add_table(fp_str, "measure", ["tbl1"], rows)

    # get back
    got = a.get_table(fp_str, "measure", ["tbl1"])
    assert got == rows

    # list_tables returns the triple
    listed = a.list_tables()
    assert len(listed) == 1
    assert listed[0][0] == fp
    assert listed[0][1] == "measure"
    assert listed[0][2] == ["tbl1"]

    # get all subtables for a given file
    subt = a.get_tables_for_file(fp)
    assert "measure" in subt
    pairs = subt["measure"]
    found = False
    for key, data in pairs:
        if key == ["tbl1"]:
            assert data == rows
            found = True
            break
    assert found

    # query rows with predicate
    q = a.query_rows(fp_str, "measure", ["tbl1"], lambda r: r.get("x") == 3)
    assert q == [{"x": 3, "y": 4}]

    # save to file and load into a new adapter
    save_file = tmp_path / "store.json"
    a.save_to_file(save_file)

    b = NestedDictTableAdapter()
    b.load_from_file(save_file)
    assert b.get_table(fp_str, "measure", ["tbl1"]) == rows

    # delete table
    assert b.delete_table(fp_str, "measure", ["tbl1"]) is True
    assert b.list_tables() == []
