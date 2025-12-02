import textwrap
from pathlib import Path

from decorators.processor import ProcessingContext
from processors.file_ops import set_path_name_dict


def test_set_path_name_dict_parses_and_sets_labels_and_categories(tmp_path):
    # Arrange: create a folder with files, a mapping file and a category file
    folder = tmp_path / "myfolder"
    folder.mkdir()
    (folder / "a.txt").write_text("alpha")
    (folder / "b.txt").write_text("beta")

    # mapping file: mapping filenames to friendly names
    mapping = textwrap.dedent("""
    # comment line
    a.txt NewA
    b.txt NewB
    """)
    (folder / "_dict.txt").write_text(mapping, encoding='utf-8')

    # category files (one or more .cate files)
    (folder / "cat1.cate").write_text("category1")

    ctx = ProcessingContext()

    # Act
    res = set_path_name_dict(folder,
                             ctx,
                             _dict_file='_dict.txt',
                             force=True,
                             category_suffix='.cate')

    # Assert: parsing result
    assert res['status'] == 'success'
    assert res['entries_parsed'] == 2
    # stored mapping
    stored = ctx.get_data(['file_ops', 'path_name_dict', str(folder)])
    assert stored.get('a.txt') == 'NewA'
    assert stored.get('b.txt') == 'NewB'

    # labels for individual files should be lists and contain the mapped name as last element
    lab_a = ctx.get_data(['labels', str(folder / 'a.txt')])
    assert isinstance(lab_a, list)
    assert lab_a[-1] == 'NewA'

    lab_b = ctx.get_data(['labels', str(folder / 'b.txt')])
    assert lab_b[-1] == 'NewB'

    # categories should include the cat1 stem
    cats_a = ctx.get_data(['categories', str(folder / 'a.txt')])
    assert 'cat1' in cats_a
