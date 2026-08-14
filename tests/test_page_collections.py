"""The seam: what a page declares may be written.

A save arrives with a page id, and the server reads its own descriptor to learn
which collections exist, in which table, and through which foreign key. These
tests cover the reading — the walk over a resolved page and the map it produces —
because everything the save endpoint refuses, it refuses on the strength of it.

Two consumers, one walk: `get_page` uses it to fill `view.source.model` from the
node that declares it, `save_tree` uses the map. They cannot diverge, and the
tests check both ends of that claim.
"""
import pytest

import coframe.utils
from coframe.plugins import PluginsManager
from coframe.pages import Collection, page_aggregate, resolve_collections
from coframe.endpoint_panels import get_page


class FakeApp:
    def __init__(self, pm):
        self.pm = pm
        self.tables = {}


def make_app(pages, plugin='bench'):
    pm = PluginsManager()
    pm.config = {}
    pm.merge_dicts({'pages': pages}, plugin)
    return FakeApp(pm)


def form_page(*layout, model='Book'):
    """A page whose content is a form carrying `layout`."""
    return {
        'title': 'Book',
        'content': {
            'type': 'form',
            'source': {'model': model},
            'layout': list(layout),
        },
    }


def authors_node(**overrides):
    node = {
        'type': 'collection',
        'id': 'authors',
        'model': 'BookAuthor',
        'fk': 'book_id',
        'view': {'type': 'table', 'columns': [{'field': 'author_id'}]},
    }
    node.update(overrides)
    return node


# ── The walk ────────────────────────────────────────────────────────────────

def test_node_fills_the_model_of_its_view():
    """The table is a fact of persistence: the node declares it, the view gets it."""
    page = form_page(authors_node())
    collections = resolve_collections(page, 'book_form')

    node = page['content']['layout'][0]
    assert node['view']['source']['model'] == 'BookAuthor'
    assert collections['authors'].model == 'BookAuthor'
    assert collections['authors'].fk == 'book_id'


def test_a_view_that_reads_another_table_is_an_error_naming_both():
    page = form_page(authors_node(view={'type': 'table', 'source': {'model': 'Author'}}))

    with pytest.raises(ValueError) as exc:
        resolve_collections(page, 'book_form')

    assert 'BookAuthor' in str(exc.value) and 'Author' in str(exc.value)


def test_a_view_that_agrees_is_left_alone():
    page = form_page(authors_node(view={'type': 'table', 'source': {'model': 'BookAuthor'}}))
    assert resolve_collections(page, 'book_form')['authors'].model == 'BookAuthor'


def test_the_error_names_the_plugin_that_wrote_the_node():
    """Errors run before metadata is stripped, so they can still say where to look."""
    page = form_page(authors_node(**{'fk': None, '$plugin': 'library'}))

    with pytest.raises(ValueError) as exc:
        resolve_collections(page, 'book_form')

    assert 'library' in str(exc.value) and 'book_form' in str(exc.value)


@pytest.mark.parametrize('missing', ['id', 'model', 'fk'])
def test_a_node_without_its_essentials_is_refused(missing):
    node = authors_node()
    del node[missing]

    with pytest.raises(ValueError):
        resolve_collections(form_page(node), 'book_form')


def test_two_collections_with_the_same_id_collide():
    page = form_page(authors_node(), authors_node(model='Loan', fk='book_id'))

    with pytest.raises(ValueError, match='Duplicate'):
        resolve_collections(page, 'book_form')


def test_a_node_is_found_however_deep_the_layout_puts_it():
    """A collection is a layout node like `section` or `row`: the layout decides."""
    page = form_page({
        'type': 'tabs',
        'tabs': [
            {'label': 'Data', 'layout': [{'type': 'section', 'columns': []}]},
            {'label': 'Authors', 'layout': [authors_node()]},
        ],
    })

    assert 'authors' in resolve_collections(page, 'book_form')


def test_the_view_of_a_node_is_not_searched_for_more_nodes():
    """A collection under a collection is declared in the row form, not in the grid."""
    page = form_page(authors_node(view={
        'type': 'table',
        'layout': [{'type': 'collection', 'id': 'smuggled',
                    'model': 'User', 'fk': 'x_id'}],
    }))

    assert set(resolve_collections(page, 'book_form')) == {'authors'}


def test_the_row_form_defaults_to_the_model_form():
    collections = resolve_collections(form_page(authors_node()), 'book_form')
    assert collections['authors'].form == 'BookAuthor_form'

    declared = resolve_collections(form_page(authors_node(form='ba_row')), 'book_form')
    assert declared['authors'].form == 'ba_row'


def test_domain_and_defaults_travel_with_the_node():
    node = authors_node(domain=[{'usage': 'shipping'}], defaults={'usage': 'shipping'})
    coll = resolve_collections(form_page(node), 'partner_form')['authors']

    assert coll.domain == [{'usage': 'shipping'}]
    assert coll.defaults == {'usage': 'shipping'}


# ── The map ─────────────────────────────────────────────────────────────────

def test_the_aggregate_carries_the_root_model_and_its_collections():
    app = make_app({'book_form': form_page(authors_node())})
    aggregate = page_aggregate(app, 'book_form')

    assert aggregate.model == 'Book'
    assert isinstance(aggregate.collections['authors'], Collection)


def test_the_tree_belongs_to_the_page_and_not_to_the_table():
    """Two forms on one table: with the node it is an aggregate, without it a row.

    The flat case is the general one with no collections — the recursion stopping
    at the first step — which is why the same endpoint serves both.
    """
    app = make_app({
        'book_form': form_page(authors_node()),
        'book_quick_form': form_page({'type': 'section', 'columns': []}),
    })

    assert set(page_aggregate(app, 'book_form').collections) == {'authors'}

    flat = page_aggregate(app, 'book_quick_form')
    assert flat.model == 'Book'
    assert flat.collections == {}


def test_a_page_that_names_no_model_cannot_be_written():
    app = make_app({'odd_form': {'content': {'type': 'form'}}})

    with pytest.raises(ValueError, match='no model'):
        page_aggregate(app, 'odd_form')


def test_an_unknown_page_is_not_a_tree():
    with pytest.raises(ValueError, match='not found'):
        page_aggregate(make_app({}), 'nowhere_form')


def test_the_tree_spans_pages_through_the_row_form():
    """Grandchildren come from the row form of a collection — recursion, one map."""
    app = make_app({
        'book_form': form_page(authors_node(form='ba_row')),
        'ba_row': form_page({'type': 'collection', 'id': 'remarks',
                             'model': 'Remark', 'fk': 'ba_id'}, model='BookAuthor'),
    })

    authors = page_aggregate(app, 'book_form').collections['authors']
    assert authors.collections['remarks'].model == 'Remark'


def test_a_row_form_nobody_wrote_stops_the_descent_quietly():
    """Which page opens a row is a question of interface, not a reason to refuse."""
    app = make_app({'book_form': form_page(authors_node(form='not_written_yet'))})

    assert page_aggregate(app, 'book_form').collections['authors'].collections == {}


def test_a_row_form_that_exists_and_is_broken_still_raises():
    app = make_app({
        'book_form': form_page(authors_node(form='ba_row')),
        'ba_row': form_page({'type': 'collection', 'id': 'remarks', 'model': 'Remark'},
                            model='BookAuthor'),
    })

    with pytest.raises(ValueError, match='fk'):
        page_aggregate(app, 'book_form')


def test_recursion_stops_where_the_path_repeats():
    """The contacts of a partner are partners: expanding forever describes nothing."""
    app = make_app({
        'partner_form': form_page({'type': 'collection', 'id': 'contacts',
                                   'model': 'Partner', 'fk': 'parent_id',
                                   'form': 'partner_form'}, model='Partner'),
    })

    contacts = page_aggregate(app, 'partner_form').collections['contacts']
    assert contacts.collections == {}


# ── The other consumer ──────────────────────────────────────────────────────

def test_get_page_hands_the_client_the_completed_descriptor(monkeypatch):
    app = make_app({'book_form': form_page(authors_node())})
    monkeypatch.setattr(coframe.utils, 'get_app', lambda: app)

    result = get_page({'id': 'book_form'})
    node = result['data']['content']['layout'][0]

    assert result['status'] == 'success'
    assert node['view']['source']['model'] == 'BookAuthor'


def test_get_page_reports_a_broken_node_instead_of_serving_it(monkeypatch):
    app = make_app({'book_form': form_page(authors_node(fk=None))})
    monkeypatch.setattr(coframe.utils, 'get_app', lambda: app)

    result = get_page({'id': 'book_form'})

    assert result['code'] == 400
    assert 'fk' in result['message']
