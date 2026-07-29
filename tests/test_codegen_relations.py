"""Tests for the relationship half of model generation.

A foreign key in YAML produces three things: the column, and a *pair* of Python
attributes to navigate it — `Loan.book` and `Book.loans`. Only the column exists
in SQL; the two attributes are named by the generator, and naming them from the
target table (as the first implementation did) breaks as soon as a table
references itself, or references the same target twice: both attributes land on
the same name and SQLAlchemy cannot tell which column carries the join.

The tests below check the generated text where the rule is what matters, and let
SQLAlchemy configure the mappers where the proof is that the model actually
works — the text can look plausible and still not map.
"""
import importlib.util

import pytest
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, configure_mappers

import coframe.utils
from coframe.db import DB, Base
from coframe.plugins import PluginsManager
from coframe.source import Generator


@pytest.fixture(autouse=True)
def fresh_registry():
    """Each test maps its own classes onto the shared declarative Base."""
    yield
    Base.registry.dispose()
    Base.metadata.clear()


def generate(tmp_path, monkeypatch, tables):
    """Write a one-plugin app declaring `tables`, and return the generated source."""
    plugin = tmp_path / 'plugins' / 'app'
    plugin.mkdir(parents=True)
    (plugin / 'config.yaml').write_text(yaml.safe_dump({'name': 'app', 'version': '0.0.1'}))
    (plugin / 'model.yaml').write_text(yaml.safe_dump({'tables': tables}))

    cfg = tmp_path / 'config.yaml'
    cfg.write_text(yaml.safe_dump({'name': 'test', 'plugins': ['plugins']}))
    monkeypatch.chdir(tmp_path)

    manager = PluginsManager()
    manager.load_config(str(cfg))
    coframe.utils.register_standard_handlers(manager)
    manager.load_plugins()

    db = DB()
    db.calc_db(manager)

    out = tmp_path / 'generated_model.py'
    Generator(db).generate(filename=str(out))
    return out.read_text()


def load(tmp_path, source):
    """Import the generated source and let SQLAlchemy configure the mappers."""
    path = tmp_path / 'generated_model.py'
    path.write_text(source)
    spec = importlib.util.spec_from_file_location('generated_model', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    configure_mappers()
    return module


def pk():
    return {'name': 'id', 'type': 'Integer', 'primary_key': True, 'autoincrement': True}


def table(*columns, name=None):
    t = {'columns': [pk(), *columns]}
    if name:
        t['name'] = name
    return t


def line(source, attribute):
    """The generated line defining `attribute`, for asserting on one statement."""
    for text in source.splitlines():
        if text.strip().startswith(f"{attribute}:"):
            return text.strip()
    raise AssertionError(f"no attribute '{attribute}' in generated source:\n{source}")


# ── naming ────────────────────────────────────────────────────────────────────

def test_plain_foreign_key_names_the_relation_after_the_column(tmp_path, monkeypatch):
    """`book_id` -> `book` on Loan, and the plural table name back on Book."""
    source = generate(tmp_path, monkeypatch, {
        'Book': table(name='books'),
        'Loan': table({'name': 'book_id', 'foreign_key': {'target': 'Book.id'}}, name='loans'),
    })

    assert "book: Mapped['Book'] = relationship('Book', foreign_keys='Loan.book_id'" in line(source, 'book')
    assert "back_populates='loans'" in line(source, 'book')
    assert "loans: Mapped[List['Loan']] = relationship('Loan', foreign_keys='Loan.book_id'" in line(source, 'loans')
    assert "back_populates='book'" in line(source, 'loans')
    load(tmp_path, source)


def test_relation_name_cuts_at_the_last_underscore(tmp_path, monkeypatch):
    """Any suffix works — the rule is the cut, not a list of known suffixes."""
    source = generate(tmp_path, monkeypatch, {
        'Cliente': table(name='clienti'),
        'Ordine': table(
            {'name': 'codice_esterno_fk', 'foreign_key': {'target': 'Cliente.id'}}, name='ordini'),
    })

    assert 'codice_esterno' in source
    line(source, 'codice_esterno')  # relation exists under the cut name
    load(tmp_path, source)


def test_column_without_suffix_falls_back_to_the_target_table(tmp_path, monkeypatch):
    """Legacy names have nothing to cut; the target table names the relation."""
    source = generate(tmp_path, monkeypatch, {
        'Cliente': table(name='clienti'),
        'Ordine': table({'name': 'codcli', 'foreign_key': {'target': 'Cliente.id'}}, name='ordini'),
    })

    assert "cliente: Mapped['Cliente']" in line(source, 'cliente')
    load(tmp_path, source)


def test_explicit_names_win(tmp_path, monkeypatch):
    """`relation:`/`backref:` are the escape hatch for names worth choosing."""
    source = generate(tmp_path, monkeypatch, {
        'Book': table(name='books'),
        'Loan': table({'name': 'book_id', 'foreign_key': {
            'target': 'Book.id', 'relation': 'volume', 'backref': 'prestiti'}}, name='loans'),
    })

    assert "back_populates='prestiti'" in line(source, 'volume')
    assert "back_populates='volume'" in line(source, 'prestiti')
    assert 'ForeignKey' in source and 'relation=' not in source  # never a ForeignKey kwarg
    load(tmp_path, source)


# ── self reference ────────────────────────────────────────────────────────────

def test_self_reference_emits_remote_side(tmp_path, monkeypatch):
    """Both sides are the same class: only remote_side says which one is the 'one'."""
    source = generate(tmp_path, monkeypatch, {
        'Partner': table({'name': 'parent_id', 'nullable': True,
                          'foreign_key': {'target': 'Partner.id'}}, name='partners'),
    })

    assert "remote_side='Partner.id'" in line(source, 'parent')
    assert "remote_side" not in line(source, 'partners')  # the collection side must not have it
    load(tmp_path, source)


def test_two_self_references_stay_distinct(tmp_path, monkeypatch):
    """The case that could not be generated at all: parent_id + merged_into_id."""
    source = generate(tmp_path, monkeypatch, {
        'Partner': table(
            {'name': 'name', 'type': 'String', 'length': 80},
            {'name': 'parent_id', 'nullable': True, 'foreign_key': {'target': 'Partner.id'}},
            {'name': 'merged_into_id', 'nullable': True, 'foreign_key': {'target': 'Partner.id'}},
            name='partners'),
    })

    # Two forward attributes, and two back attributes suffixed to stay apart
    assert "foreign_keys='Partner.parent_id'" in line(source, 'parent')
    assert "foreign_keys='Partner.merged_into_id'" in line(source, 'merged_into')
    assert "back_populates='parent'" in line(source, 'partners_parent')
    assert "back_populates='merged_into'" in line(source, 'partners_merged_into')

    module = load(tmp_path, source)
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        head = module.Partner(name='Rossi SRL')
        branch = module.Partner(name='Rossi SRL - sede', parent=head)
        dup = module.Partner(name='Rossi Srl', merged_into=head)
        session.add_all([head, branch, dup])
        session.flush()

        assert branch.parent.name == 'Rossi SRL'
        assert [p.name for p in head.partners_parent] == ['Rossi SRL - sede']
        assert [p.name for p in head.partners_merged_into] == ['Rossi Srl']


def test_soft_self_reference_spells_out_the_join(tmp_path, monkeypatch):
    """No constraint to infer from, and no way to guess the direction: both needed."""
    source = generate(tmp_path, monkeypatch, {
        'Partner': table({'name': 'parent_id', 'nullable': True,
                          'foreign_key': {'target': 'Partner.id', 'constraint': False}},
                         name='partners'),
    })

    assert 'ForeignKey' not in line(source, 'parent_id')  # soft: no DB-level constraint
    assert "primaryjoin='Partner.parent_id == Partner.id'" in line(source, 'parent')
    assert "remote_side='Partner.id'" in line(source, 'parent')
    load(tmp_path, source)


# ── several paths between the same two tables ─────────────────────────────────

def test_two_foreign_keys_to_the_same_target(tmp_path, monkeypatch):
    """`ship_to_id`/`bill_to_id`: distinct forward names, suffixed back names."""
    source = generate(tmp_path, monkeypatch, {
        'Partner': table(name='partners'),
        'Order': table(
            {'name': 'ship_to_id', 'foreign_key': {'target': 'Partner.id'}},
            {'name': 'bill_to_id', 'foreign_key': {'target': 'Partner.id'}},
            name='orders'),
    })

    assert "foreign_keys='Order.ship_to_id'" in line(source, 'ship_to')
    assert "foreign_keys='Order.bill_to_id'" in line(source, 'bill_to')
    assert "back_populates='ship_to'" in line(source, 'orders_ship_to')
    assert "back_populates='bill_to'" in line(source, 'orders_bill_to')
    load(tmp_path, source)


def test_foreign_keys_in_opposite_directions(tmp_path, monkeypatch):
    """Two FK paths between the same tables, one per table.

    Nothing here is ambiguous *within* a table, which is why `foreign_keys` is
    emitted unconditionally: without it SQLAlchemy refuses the join on relations
    neither table declared as special.
    """
    source = generate(tmp_path, monkeypatch, {
        'Partner': table({'name': 'default_order_id', 'nullable': True,
                          'foreign_key': {'target': 'Order.id'}}, name='partners'),
        'Order': table({'name': 'partner_id', 'nullable': True,
                        'foreign_key': {'target': 'Partner.id'}}, name='orders'),
    })

    load(tmp_path, source)  # the assertion is that configure_mappers() succeeds


# ── refusals ──────────────────────────────────────────────────────────────────

def test_relation_clashing_with_a_column_is_refused(tmp_path, monkeypatch):
    """A relationship overwriting a column in the class body must never be silent."""
    with pytest.raises(ValueError, match="both a column and"):
        generate(tmp_path, monkeypatch, {
            'Partner': table(name='partners'),
            'Order': table(
                {'name': 'partner', 'type': 'String', 'length': 20},
                {'name': 'partner_id', 'foreign_key': {'target': 'Partner.id'}},
                name='orders'),
        })


def test_two_relations_claiming_one_name_are_refused(tmp_path, monkeypatch):
    """`payment_primary` and `payment_secondary` both cut down to `payment`."""
    with pytest.raises(ValueError, match="is claimed by"):
        generate(tmp_path, monkeypatch, {
            'Payment': table(name='payments'),
            'Order': table(
                {'name': 'payment_primary', 'foreign_key': {'target': 'Payment.id'}},
                {'name': 'payment_secondary', 'foreign_key': {'target': 'Payment.id'}},
                name='orders'),
        })


def test_the_refusal_can_be_answered_with_explicit_names(tmp_path, monkeypatch):
    """The error is an instruction, so the instruction has to work."""
    source = generate(tmp_path, monkeypatch, {
        'Payment': table(name='payments'),
        'Order': table(
            {'name': 'payment_primary', 'foreign_key': {
                'target': 'Payment.id', 'relation': 'primary_payment'}},
            {'name': 'payment_secondary', 'foreign_key': {
                'target': 'Payment.id', 'relation': 'secondary_payment'}},
            name='orders'),
    })

    line(source, 'primary_payment')
    line(source, 'secondary_payment')
    load(tmp_path, source)
