"""Tests for coframe.diagnostics.run_checks — post-load descriptor validation.

Uses a PluginsManager populated via merge_dicts (so $plugin attribution is
real) and stub tables exposing only what the checks read: effective_columns
with .name, and .attributes.
"""
import pytest

from coframe.plugins import PluginsManager
from coframe.diagnostics import run_checks


class FakeCol:
    def __init__(self, name):
        self.name = name
        self.attributes = {}


class FakeTable:
    def __init__(self, *cols):
        self.effective_columns = [FakeCol(c) for c in cols]
        self.attributes = {}


class FakeApp:
    def __init__(self, pm, tables):
        self.pm = pm
        self.tables = tables


@pytest.fixture
def app():
    pm = PluginsManager()
    pm.merge_dicts({
        'pages': {
            'book_list': {
                'title': 'Books',
                'content': {'$ref': 'views.book_list_view'},
            },
            'broken': {
                'content': {'$ref': 'views.nope'},
            },
        },
        'views': {
            'book_list_view': {
                'type': 'table',
                'source': {'model': 'Book'},
                'columns': [
                    {'field': 'title'},
                    {'field': 'ghost'},
                    {'field': 'Publisher.name'},
                    {'field': 'Publisher.ghost'},
                    {'field': '$props.computed'},
                    {'field': "COUNT(DISTINCT Author.id) as n_authors"},
                ],
                'actions': {'row': [
                    {'id': 'edit', 'action': 'stack_push', 'panel': 'missing_page'},
                    {'id': 'ok', 'action': 'stack_push', 'panel': 'book_list'},
                ]},
            },
            'bad_model_view': {
                'type': 'form',
                'source': {'model': 'Nope'},
                'fields': [{'name': 'x'}],
            },
            'form_view': {
                'type': 'form',
                'source': {'model': 'Book'},
                'fields': [
                    {'name': 'title'},
                    {'group': 'Extra', 'fields': [{'name': 'phantom'}]},
                ],
            },
        },
    }, 'testplugin')

    tables = {
        'Book': FakeTable('id', 'title'),
        'Publisher': FakeTable('id', 'name'),
    }
    return FakeApp(pm, tables)


def by_code(issues, code):
    return [i for i in issues if i['code'] == code]


def test_ref_unresolved(app):
    issues = by_code(run_checks(app), 'ref-unresolved')
    assert len(issues) == 1
    assert issues[0]['severity'] == 'error'
    assert issues[0]['path'] == 'pages.broken.content'
    assert "'views.nope'" in issues[0]['message']


def test_model_missing(app):
    issues = by_code(run_checks(app), 'model-missing')
    assert len(issues) == 1
    assert issues[0]['path'] == 'views.bad_model_view.source.model'
    # No field checks on a view whose model doesn't exist
    fields = by_code(run_checks(app), 'field-unknown')
    assert not any('bad_model_view' in i['path'] for i in fields)


def test_field_unknown(app):
    issues = by_code(run_checks(app), 'field-unknown')
    paths = {i['path'] for i in issues}
    # Direct unknown field, unknown joined column, unknown group field
    assert 'views.book_list_view.columns[ghost]' in paths
    assert 'views.book_list_view.columns[Publisher.ghost]' in paths
    assert 'views.form_view.fields[Extra][phantom]' in paths
    # Valid fields, joined fields, $-values and SQL expressions raise nothing
    assert not any('[title]' in p or '[Publisher.name]' in p or '$props' in p
                   or 'COUNT' in p
                   for p in paths)


def test_panel_missing(app):
    issues = by_code(run_checks(app), 'panel-missing')
    assert len(issues) == 1
    assert "'missing_page'" in issues[0]['message']


def test_panel_auto_generated_is_valid(app):
    # 'book_form' is not an explicit page but auto-resolves from table Book
    app.pm.data['views']['book_list_view']['actions']['row'][0]['panel'] = 'book_form'
    assert not by_code(run_checks(app), 'panel-missing')


def test_view_orphan(app):
    issues = by_code(run_checks(app), 'view-orphan')
    orphans = {i['path'] for i in issues}
    assert 'views.bad_model_view' in orphans
    assert 'views.form_view' in orphans
    assert 'views.book_list_view' not in orphans  # referenced by book_list


def test_plugin_attribution(app):
    for issue in run_checks(app):
        assert issue['plugin'] == 'testplugin'


def test_merge_issues_included(app):
    app.pm.add_issue('warning', 'merge-anchor-missing', 'pages.x',
                     "anchor 'y' not found", 'p2')
    issues = by_code(run_checks(app), 'merge-anchor-missing')
    assert len(issues) == 1


def test_merge_overlap_collected():
    pm = PluginsManager()
    pm.merge_dicts({'config': {'theme': 'light'}}, 'p1')
    pm.merge_dicts({'config': {'theme': 'dark'}}, 'p2')
    overlaps = [i for i in pm.issues if i['code'] == 'merge-overlap']
    assert len(overlaps) == 1
    assert overlaps[0]['path'] == 'config.theme'
    assert overlaps[0]['plugin'] == 'p2'


def test_add_issue_dedupes():
    pm = PluginsManager()
    pm.add_issue('info', 'x', 'a.b', 'msg', 'p')
    pm.add_issue('info', 'x', 'a.b', 'msg', 'p')
    assert len(pm.issues) == 1


def test_generic_descriptor_sections_are_checked():
    """Sections other than pages/views (e.g. menus/menu_items) are validated
    uniformly: $ref and stack_push targets are checked wherever they appear."""
    pm = PluginsManager()
    pm.merge_dicts({
        'menus': {'main': {'label': 'Main'}},
        'menu_items': {
            'books': {'label': 'Books', 'action': 'stack_push', 'panel': 'book_list'},
            'ghost': {'label': 'Ghost', 'action': 'stack_push', 'panel': 'no_such_page'},
            'bad_ref': {'content': {'$ref': 'views.nope'}},
        },
    }, 'menuplugin')
    app = FakeApp(pm, {'Book': FakeTable('id', 'title')})

    issues = run_checks(app)
    # stack_push to an auto-generatable page (book_list ← Book) is fine;
    # to an unknown page it is flagged.
    panel_missing = by_code(issues, 'panel-missing')
    assert {i['path'] for i in panel_missing} == {'menu_items.ghost.panel'}
    assert any(i['path'] == 'menu_items.bad_ref.content' and i['plugin'] == 'menuplugin'
               for i in by_code(issues, 'ref-unresolved'))


def test_db_sections_not_walked_as_descriptors():
    """tables/types/schemas are DB/type sections, not descriptor trees."""
    from coframe.diagnostics import _descriptor_sections
    pm = PluginsManager()
    pm.merge_dicts({'tables': {}, 'types': {}, 'schemas': {}, 'menus': {}}, 'p')
    assert _descriptor_sections(pm) == ['menus']
