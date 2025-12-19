from pathlib import Path
from decorators.processor import ProcessingContext
from processors.file_ops import set_path_name_dict


def test_set_path_name_dict_assigns_depth_default(tmp_path):
    # create a root and a subdirectory with one file
    root = tmp_path / "root"
    root.mkdir()
    sub = root / "sub"
    sub.mkdir()
    f1 = sub / "file1.txt"
    f1.write_text("hello")

    ctx = ProcessingContext()
    ctx.root_path = root

    # call function under test
    res = set_path_name_dict(sub, ctx)
    assert res and res.get('status') == 'success'

    # categories should have been set for the file with default depth
    cats = ctx.get_data(['categories', str(f1)], None)
    assert cats is not None
    assert isinstance(cats, list)
    # relative to root: root/sub/file1 -> rel.parts = ('sub','file1.txt') => depth = 1
    assert cats == ['depth_1']

    # labels should contain the filename
    labels = ctx.get_data(['labels', str(f1)], None)
    assert labels == [f1.name]

    # category_label_map must be populated for the item and map depth->label
    cmap = ctx.get_data(['category_label_map', str(f1)], None)
    assert cmap is not None
    assert isinstance(cmap, dict)
    assert cmap.get('depth_1') == f1.name
