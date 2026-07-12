# Coframe Plugin Model — Reference Manual

*This manual describes how plugins define, extend, and override data models in Coframe.
It is written for application developers building on top of the framework.*

*Last revised: 2026-07-12. Living document — sections marked* (planned) *or* (future) *are not yet implemented.*

Coframe is designed around a single workflow: **configure, build, deploy**.
An application developer starts from a skeleton project, writes plugins that declare
data models and UI descriptors in YAML, wires a minimal set of configuration files,
runs `pnpm build`, and ships. The framework generates the database schema, the API
endpoints, and the UI from those declarations — no boilerplate, no repetition.

The plugin model described in this manual is the mechanism that makes this possible:
multiple independent plugins contribute fragments of the same data model, and the
framework merges them into a coherent whole at startup.

---

## Table of Contents

1. [How the Plugin System Works](#1-how-the-plugin-system-works)
2. [The Merge Algorithm](#2-the-merge-algorithm) — identity keys, smart merge, the `$` metadata convention
3. [Types](#3-types) — primitive, inheritance, composite/mixin, virtual columns, query behaviors
4. [Tables](#4-tables)
5. [Pages and Panels](#5-pages-and-panels)
6. [Data Source](#6-data-source)
7. [Table View](#7-table-view)
8. [Form View](#8-form-view)
9. [Menu](#9-menu)
10. Other View Types *(future: kanban, cards, calendar)*
11. Locale and Help *(future)*
12. Roles and Permissions *(future)*
13. Workflows *(future)*

---

## 1. How the Plugin System Works

### 1.1 What Is a Plugin?

A **plugin** is a directory containing at least a `config.yaml` file.
Everything else in the directory is optional: additional YAML files declare data
(models, panels, types…), Python files provide endpoint handlers and business logic.

```
plugins/
  common/
    config.yaml        ← required: name, version, depends_on
    model.yaml         ← types and tables contributed by this plugin
  books/
    config.yaml
    model.yaml
    panels.yaml
    plugin.py          ← custom endpoints
    data/              ← seed data, client components, docs, and anything else
```

The `data/` subdirectory (and any other subdirectory) is ignored by the plugin
loader — it is a convention for organising plugin-owned assets that the plugin
itself is responsible for consuming.

### 1.2 Application Configuration and Plugin Paths

The application's entry point is a `config.yaml` file that lists one or more
plugin root directories:

```yaml
# config.yaml
name: mytestapp
plugins:
  - plugins           # flat plugins live here: plugins/common, plugins/users, …
  - plugins/libapp    # sub-group: plugins/libapp/library, plugins/libapp/books, …
```

Each listed path is scanned **one level deep**, without recursion: only direct
subdirectories that contain a `config.yaml` are recognised as plugins.
Directories without `config.yaml` are silently skipped.

This means that if you organise related plugins under a container folder, that
folder must be listed explicitly in `config.yaml`:

```yaml
plugins:
  - plugins           # discovers: plugins/common, plugins/users
  - plugins/vendor1   # discovers: plugins/vendor1/p1, plugins/vendor1/p2
                      # (plugins/vendor1 itself has no config.yaml — it is just a container)
```

Each listed path is also added to the Python import path at startup, so `.py`
files inside plugins can use plain `import` statements. Listing paths explicitly
gives you a precise, readable map of which directories are active for both plugin
discovery and Python imports — no implicit recursion, no surprises.

### 1.3 Plugin Identity and Dependencies

Each plugin has a unique **name** (defaults to the directory name, overridable in
`config.yaml`). Plugin names must be unique across the entire application.

```yaml
# plugins/library/config.yaml
name: library
version: 1.0.0
depends_on: [common, books]
```

**Naming convention.** In collaborative or multi-vendor projects, using a
`vendor_module` prefix avoids accidental collisions between independently
developed plugins (e.g. `acme_hr`, `acme_crm`). This is a convention, not
enforced by the framework. Dot notation (`vendor.module`) should be avoided
because the dot is Python's package separator and causes import errors.

The framework resolves dependencies using a topological sort (Kahn's algorithm).
Plugins are loaded in dependency order: a base plugin is always merged before
any plugin that depends on it.

Circular dependencies and missing dependencies are caught at startup and raise
a clear error.

### 1.4 Load and Merge Sequence

When `PluginsManager.load_plugins()` is called:

1. All configured plugin paths are scanned one level deep.
2. Dependency order is computed via topological sort.
3. Each plugin's YAML files (except `config.yaml`) are loaded and merged
   into a single global `data` dict in dependency order.
4. The last writer wins for scalar values; for dicts and lists the merge
   rules described in Chapter 2 apply.

The final `data` dict is the single source of truth for the entire application.

### 1.5 Sections and the Agnostic Core

The top-level keys of `data` are called **sections** (or *roots*). The pivotal
design decision is that the merge engine is **agnostic**: it does not know what
a `table`, a `view`, or a `menu` *is*. It merges generic dicts and lists,
applying the same rules (Chapter 2) to every section. Meaning is assigned
**downstream** — by the model generator (`types`, `tables`), by the descriptor
endpoints (`get_page`, `get_menu`), by the querybuilder — never by the merge
itself.

Two consequences follow, and they are the backbone of the whole system:

- **Sections are open-ended.** Adding a new kind of descriptor — the `menus`
  section, a future `chrome` or `quickbar` — requires **no change to the merge
  engine**. You declare the section in a plugin and write the consumer that
  reads it. The diagnostics validator and dumper likewise walk every non-DB
  section uniformly, with no hardcoded list of names.
- **Each root has its own rules.** A section decides how its lists are keyed
  (identity key, § 2.2), whether it registers a custom merge handler, and how
  its resolved tree is interpreted by its consumer. `tables` are keyed by column
  `name`; `menu_items` are a flat collection assembled into a tree by `parent`;
  `pages`/`views` are namespace-wrapped by plugin. Same engine, different rules
  per root.

Sections currently defined:

| Section | Role | Merge model | Namespace-wrapped |
|---------|------|-------------|-------------------|
| `types` | reusable column types | dict merge | no |
| `tables` | database tables | dict merge | no |
| `schemas` | non-DB typed schemas | dict merge | no |
| `pages` | page descriptors | dict merge | **yes** (by plugin name) |
| `views` | view descriptors | dict merge | **yes** (by plugin name) |
| `menus` | menu roots (Chapter 9) | dict merge | no |
| `menu_items` | menu entries, flat (Chapter 9) | dict merge | no |

The framework's generic tooling makes exactly **one** distinction: DB/type
sections (`types`, `tables`, `schemas`) feed model generation, everything else
is a **descriptor** consumed by the UI. Beyond that split, all sections are
treated the same.

**Namespace-wrapped sections** are explained in Chapter 5.

---

## 2. The Merge Algorithm

### 2.1 Dictionary Merge

When two plugins contribute to the same dict key, the merge is recursive:

- A **scalar value** (string, int, bool) in the new plugin overwrites the base.
- A **dict value** is merged recursively — existing keys are updated, new keys
  are added, nothing is deleted unless explicitly overridden.
- A **list value** follows the smart-merge rules described below.

Every merged dict receives a `$plugin` metadata key recording which plugin last
touched it. This key is for internal use and is stripped before data is sent to
the frontend.

### 2.2 List Merge — Identity Keys

Plain list concatenation (`base + new`) is correct for scalar sequences but
wrong for lists of named objects: if two plugins both provide a list of columns,
you want the second plugin to *modify* the first plugin's column, not add a
duplicate.

Coframe resolves this by detecting an **identity key** on list items.
The identity key is the first recognised key found among the items:

| Priority | Key | Typical use |
|----------|-----|-------------|
| 1st | `id` | panels, tabs, action buttons |
| 2nd | `name` | table columns, form fields |
| 3rd | `field` | view columns |
| 4th | `group` | form field groups |

If no identity key is found (pure scalar lists or unrecognised dicts),
the list falls back to **plain append without duplicates**.

### 2.3 Smart Merge Operations

Given that an identity key is present, the following operations are available:

#### Replace (default)

An item in the new plugin with the same identity as an existing item is
deep-merged into it — the new values override specific properties while
leaving the rest intact.

```yaml
# base plugin
columns:
  - name: price
    type: Money

# derived plugin — changes only the label
columns:
  - name: price
    label: "Sale Price"
```

Result: `{name: price, type: Money, label: "Sale Price"}`

#### Remove

Set `$remove: true` to delete an item from the resolved list.

```yaml
columns:
  - name: internal_code
    $remove: true
```

#### Positional insert (`$after` / `$before`)

A *new* item (one whose identity is not in the base list) can be inserted at
a specific position relative to an existing sibling.

```yaml
columns:
  - name: discount
    type: Price
    $after: price        # insert immediately after the "price" column
```

If the anchor is not found at merge time, the item is appended and a warning
is logged.

> **Note:** `$remove`, `$after`, and `$before` are merge-time directives.
> They are consumed by the merge algorithm and never appear in the resolved
> descriptor delivered to the frontend.

### 2.4 Scalar Lists (Plain Append)

Lists whose items have no identity key — such as `toolbar`, `order_by`,
`joins`, `pass` — are merged by appending new items without duplicating
existing ones. There is currently no removal mechanism for scalar list items.

### 2.5 Metadata Keys and the `$` Convention

Any dict key beginning with `$` is **framework metadata**, not user data.
The prefix marks a key that the framework injects or interprets, and guarantees
it can never collide with a user-defined field, column, or descriptor id. The
convention is applied consistently: wherever the framework iterates the keys of
a merged section it skips `$`-prefixed keys (`startswith('$')`); wherever it
serialises a descriptor for the frontend it strips them.

`$` tokens fall into two families that share only the sigil.

**A. Merge-time metadata** — injected or consumed while plugin data is merged.
These are part of the plugin model:

| Key | Meaning |
|-----|---------|
| `$plugin` | Attribution: which plugin last contributed this node. Injected by the merge (§ 2.1); drives default cascades and diagnostics, enables fail-loud conflict detection; stripped before the frontend. |
| `$ref` | Reference: replace this node with the object at the given path, merging sibling keys on top. Resolved server-side — define once, place many (Chapter 5). |
| `$remove` / `$after` / `$before` | Smart-merge list directives (§ 2.3): delete an item, or insert a new one at a position. Consumed by the merge; never delivered. |

**B. Request-time placeholders** — written in descriptors but resolved *per
request* against runtime state, not during the merge:

| Token | Meaning |
|-------|---------|
| `$props.*` | Bind to props passed via `stack.push` (§ 6.4). |
| `$trigger.*` | Bind to the payload of the event that fired the load (§ 5.5, § 6.4). |
| `$or` / `$and` | Logical operators in the `db` endpoint's filter DSL — an API-request concept, not plugin YAML. AND is the implicit default (sibling conditions are AND-ed); `$or`/`$and` are needed only to build explicit groups, e.g. `(A OR B) AND (C OR D)`. |

Family A shapes the descriptor tree at startup; Family B is interpreted when a
request arrives. Neither ever reaches the frontend as a literal `$` key:
attribution is stripped, refs are expanded, directives are consumed,
placeholders are substituted.

---

## 3. Types

### 3.1 Purpose

The `types` section defines **reusable column types**: named bundles of
SQLAlchemy attributes, widget hints, labels, and help text. A type is
referenced by its name in a table column definition and propagates automatically
to any form or view that renders that column.

Types serve two goals:

1. **DRY data modelling** — avoid repeating `{base: String, length: 32}`
   everywhere.
2. **Semantic naming** — `type: Name` communicates intent better than
   `type: String, length: 100, index: True`, and carries UI metadata (widget,
   label, help) that forms and views inherit without extra configuration.

### 3.2 Primitive Base Types

The `base` property maps to a SQLAlchemy column type. Recognised base types:

| Base | SQLAlchemy type |
|------|----------------|
| `String` | `String(length)` |
| `Integer` | `Integer` |
| `Boolean` | `Boolean` |
| `Text` | `Text` |
| `Date` | `Date` |
| `DateTime` | `DateTime` |
| `Numeric` | `Numeric(precision, scale)` |
| `JSON` | `JSON` |

### 3.3 Type Inheritance

A type can extend another type using `base: <TypeName>`:

```yaml
types:

  ShortStr:
    base: String
    length: 32

  SKU:
    base: ShortStr   # inherits length: 32
    nullable: false

  Name:
    base: String
    nullable: false
    index: true
```

Inheritance is resolved at model-generation time by recursively merging
parent properties before applying the child's own properties.

### 3.4 Composite Types (Mixins)

A type that declares a `columns` list is a **composite type** (mixin).
It expands into multiple physical columns when applied to a table via `mixins:`.

```yaml
types:

  TimeStamp:
    columns:
      - name: created_at
        type: DateTime
        default: datetime.now()
        nullable: false
      - name: updated_at
        type: DateTime
        default: datetime.now()
        nullable: false

  Address:
    columns:
      - name: address
        type: String
        nullable: true
      - name: city
        type: String
        nullable: true
      - name: country
        type: String
        nullable: true
```

A table applies a mixin via the `mixins` key (see Chapter 4).

Composite types support a `prefix` option at application time so the same
mixin can be applied multiple times without column name collisions:

```yaml
tables:
  User:
    columns:
      - name: home_address
        type: Address
        prefix: home_     # → home_address, home_city, home_country
      - name: work_address
        type: Address
        prefix: work_     # → work_address, work_city, work_country
```

#### Mixin Python inheritance

When the plugin that defines a mixin type also contains a Python class with the
same name in its `model.py`, the generated mixin class automatically inherits
from that Python class. This is how a mixin carries both **column definitions**
(YAML) and **behaviour** (Python methods, class attributes) in a single unit.

```python
# plugins/common/model.py
class Archivable:
    _cf_archive_field = 'active'   # protocol attribute read by the querybuilder

    def archive(self):
        setattr(self, self._cf_archive_field, False)

    def unarchive(self):
        setattr(self, self._cf_archive_field, True)
```

```yaml
# plugins/common/model.yaml
types:
  Archivable:
    columns:
      - name: active
        type: Boolean
        default: true
        nullable: false
```

The framework generates:

```python
# generated model.py (do not edit)
class Archivable(plugins.common.model.Archivable):
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

class Author(Base, Archivable, ...):
    ...
```

Any table that declares `mixins: [Archivable]` inherits both the `active`
column and the `archive()`/`unarchive()` methods.

### 3.5 Virtual Columns

A column declared with `virtual: true` is **not mapped to the database**.
It represents a computed value implemented as a SQLAlchemy `@hybrid_property`
in the plugin's `model.py`.

```yaml
# books/model.yaml
tables:
  Author:
    columns:
      - name: full_name
        type: String
        virtual: true
        editable: false
        label: "Full Name"
```

```python
# books/model.py
from sqlalchemy.ext.hybrid import hybrid_property

class Author:
    @hybrid_property
    def full_name(self):
        return f"{self.first_name} {self.last_name}"

    @full_name.expression
    def full_name(cls):
        return cls.first_name + ' ' + cls.last_name
```

Virtual columns:

- Are **excluded** from model generation (`mapped_column` is not emitted).
- Are **included** in `effective_columns` — visible to the frontend via
  `get_server_config` and serialised in API responses.
- Are **queryable** in `select`, `filters`, and `order_by` when the
  `@expression` decorator is present (the querybuilder resolves them via
  `getattr(model_class, field_name)`).
- Are always **read-only** — setting `editable: false` is recommended to
  make the intent explicit.

Virtual columns may also be declared on mixin types, following the same rules.

### 3.6 Query Behaviors

A **query behavior** is a class that the querybuilder applies automatically
to every query on a matching model. Behaviors are registered at startup via
`app.add_query_behavior()` and are completely decoupled from the querybuilder itself.

```python
# fastapi-server.py
from plugins.common.model import Archivable
coframe_app.add_query_behavior(Archivable)
```

A behavior class implements two classmethods:

| Method | Signature | Description |
|--------|-----------|-------------|
| `applies_to` | `(cls, model_class) → bool` | Return True if behavior should apply to this model |
| `apply` | `(cls, model_class, query_def, query) → query` | Modify the SQLAlchemy query object |

The `Archivable` behavior adds `WHERE active = True` to every query on a model
that carries `_cf_archive_field`, unless the caller passes `include_archived: true`
or already filters on the archive field explicitly:

```python
api.endpoint('query', { 'table': 'Author' })
# → WHERE active = True  (implicit)

api.endpoint('query', { 'table': 'Author', 'include_archived': True })
# → no filter on active  (opt-out)

api.endpoint('query', { 'table': 'Author', 'filters': { 'active': False } })
# → WHERE active = False  (explicit — behavior defers to caller)
```

This pattern is generic: any mixin can define its own behavior and register it
at startup. Examples: tenant isolation, soft-delete with `deleted_at`, row-level
visibility rules.

### 3.7 UI Hints on Types

Types can carry UI hints that propagate automatically to form widgets:

| Property | Meaning |
|----------|---------|
| `widget` | Widget override (`password`, `email`, `textarea`, `combobox`, …) |
| `label` | Human-readable column label (used in forms and tables) |
| `help` | Tooltip / contextual help text shown in forms |

```yaml
types:
  Email:
    base: String
    widget: email
    validate: email_validator
    label: "Email Address"
```

### 3.8 Merge Behaviour for Types

`types` is a plain dict. The merge algorithm applies dict-merge semantics:

- A derived plugin can **add a new type** by declaring a new key.
- A derived plugin can **override specific properties** of an existing type
  by declaring the same key with only the properties to change.
- A derived plugin can **redefine a type entirely** by declaring it with a
  full definition — the recursive merge still applies, so only listed
  properties change.

There is no positional ordering for types (they are a dict, not a list).

---

## 4. Tables

### 4.1 Purpose

The `tables` section defines the **database schema**: one entry per SQLAlchemy
model, describing columns, foreign keys, many-to-many relationships, indexes,
and mixins.

At startup, the framework reads the merged `tables` data and **automatically
generates the SQLAlchemy model classes** — no hand-written ORM code is needed.
The generated models are used directly for database creation, migrations, and
all CRUD operations exposed by the built-in endpoints.

### 4.2 Table Definition

```yaml
tables:

  Book:
    name: books              # physical table name (defaults to snake_case of key)
    label: "Book"            # human-readable name
    help: "Library catalogue entry"
    tags: [anag]             # arbitrary tags for grouping/filtering
    mixins: [TimeStamp]      # composite types applied as column sets

    columns:
      - name: id
        type: ID             # shorthand for Integer PK autoincrement

      - name: title
        type: Description    # String, not null, index

      - name: publisher_id
        type: Integer
        nullable: true
        foreign_key:
          target: Publisher.id
          ondelete: "SET NULL"

    indexes:
      - name: idx_book_title
        columns: [title, publication_date]
        description: "Search by title and year"
```

### 4.3 Table Properties

| Property | Default | Description |
|----------|---------|-------------|
| `name` | snake_case of key | Physical SQL table name |
| `label` | key name | Display name used in UI |
| `help` | — | Description shown in admin views |
| `tags` | `[]` | Arbitrary list of category tags |
| `mixins` | `[]` | Composite types whose columns are injected |

### 4.4 Column Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Column name (identity key for merge) |
| `type` | string | Type name from `types` section, or base type |
| `nullable` | bool | Whether the column allows NULL (default: true) |
| `default` | any | Default value or Python expression |
| `unique` | bool | Unique constraint |
| `index` | bool | Single-column index |
| `label` | string | UI label (overrides type-level label) |
| `help` | string | UI tooltip |
| `widget` | string | UI widget override |
| `prefix` | string | Column name prefix when expanding a composite type |
| `foreign_key` | dict | FK definition: `target`, `ondelete`, `onupdate`, `constraint` (hard/soft — see § 4.5) |
| `length` | int | String length (for String-based types) |
| `precision` / `scale` | int | Numeric precision |

### 4.5 Foreign Keys

```yaml
- name: author_id
  type: Integer
  nullable: false
  foreign_key:
    target: Author.id          # Table.column notation
    ondelete: "CASCADE"        # SQL ON DELETE action
    onupdate: "SET NULL"       # SQL ON UPDATE action
```

Any key other than `target`/`constraint` is forwarded verbatim to SQLAlchemy's
`ForeignKey()` (so `ondelete`, `onupdate`, … work as-is).

#### Hard vs soft foreign keys (`constraint`)

By default a `foreign_key` emits a real **DB-level constraint**: the database
rejects a value with no matching parent row (`IntegrityError`), and referential
integrity is guaranteed. This is the right default.

Set `constraint: false` for a **soft** foreign key — a navigable relationship
with **no DB constraint**, so the column may hold values that reference nothing
(unknown or late-bound codes, dirty imports, references into a domain not yet
modelled):

```yaml
- name: author_id
  type: Integer
  nullable: true
  foreign_key:
    target: Author.id
    constraint: false          # soft: relationship kept, no DB constraint
```

- **Hard** (default): `ForeignKey` constraint in the DDL; orphan values are
  rejected at write time — a dangling FK cannot exist by construction.
- **Soft**: no constraint. The relationship still works (`book.author` returns
  the row if it exists, `None` otherwise); the raw id is stored even when it
  points nowhere. Because SQLAlchemy can no longer infer the join, the generator
  emits it explicitly (`primaryjoin` + `foreign_keys`).

> **Note on SQLite.** SQLite does not enforce foreign-key constraints unless
> `PRAGMA foreign_keys=ON` is set per connection — which Coframe does not set
> today. So on the SQLite dev database even *hard* FKs are not currently
> enforced; the distinction bites on PostgreSQL/MySQL. Soft FKs behave the same
> everywhere: never enforced, by design.

### 4.6 Many-to-Many Relationships

A junction table is declared with a `many_to_many` key instead of (or in
addition to) regular columns:

```yaml
  BookAuthor:
    name: books_authors
    columns:
      - name: notes
        type: String
    many_to_many:
      target1:
        table: Book.id
        column: book_id
      target2:
        table: Author.id
        column: author_id
```

### 4.7 Indexes

Compound indexes are declared at table level under `indexes`. Each index item
has `name` as its identity key for merge purposes.

```yaml
indexes:
  - name: idx_author_fullname
    columns: [first_name, last_name]
    unique: false
    description: "Search by full name"
```

### 4.8 Mixins

`mixins` is a list of composite type names. The framework expands each mixin
by injecting its columns into the table's column list at model-generation time.

```yaml
tables:
  LibraryUser:
    mixins: [TimeStamp, Credentials]
    columns:
      - name: id
        type: ID
      - name: name
        type: Name
```

Result: `id`, `name`, then all columns from `TimeStamp`, then all from
`Credentials` — in declaration order.

The generated SQLAlchemy class inherits from each mixin class, which in turn
inherits from the plugin's Python class if one exists (see § 3.4). This means
mixin columns, methods, and protocol attributes are all available on the table
model at runtime.

`effective_columns` — the full column list used by the API and the frontend —
includes mixin columns (real) and mixin virtual columns, in addition to the
table's own columns. Panel descriptors and `get_server_config` always reflect
the complete set.

### 4.9 Merge Behaviour for Tables

`tables` is a dict keyed by table name (the `Book`, `Author`, etc. keys).
Merge semantics:

- A plugin can **add a new table** by declaring a new key.
- A plugin can **add columns to an existing table** by declaring the same table
  key with only the new columns — the `columns` list is smart-merged by `name`.
- A plugin can **modify a column** (change label, widget, nullability…) by
  declaring an item with the same `name` and only the properties to change.
- A plugin can **remove a column** with `$remove: true`.
- A plugin can **add an index** by appending to `indexes` (smart-merged by `name`).
- Top-level table properties (`label`, `tags`, etc.) follow scalar override rules.

#### Example: extending a base table

```yaml
# base plugin (common)
tables:
  User:
    mixins: [Credentials]
    columns:
      - name: id
        type: ID
      - name: name
        type: Name

# derived plugin (hr)
tables:
  User:
    columns:
      - name: employee_id    # new column appended
        type: ShortStr
        unique: true
        nullable: true
        $after: name         # insert after "name"
      - name: department
        type: String
        length: 50
        nullable: true
```

The resulting `User` table has: `id`, `name`, `employee_id`, `department`,
plus everything from the `Credentials` mixin.

### 4.10 The `types` / `tables` Interaction

Types and tables interact through the type resolution chain:

```
column.type  →  types[name]  →  base type  →  SQLAlchemy column
```

A derived plugin that changes a type definition automatically affects **all**
columns of that type across all tables, without touching the table definitions.
This makes types a powerful customisation lever.

---

## 5. Pages and Panels

### 5.1 Two Concepts, One Hierarchy

The navigation layer is built from two distinct concepts:

- **Page** — a navigable unit pushed onto the application stack. A page has a
  title, a main `content` area and optionally one or more split `panels`.
  A page is what the user "opens".

- **Panel** — a split area within a page, loaded in response to events fired by
  the main view (a row click, a form load). A panel has a position (`right`,
  `bottom`), a size, and its own content — typically a view (see Chapter 6).

A page's `content` and each panel's content reference **views** — self-contained
data descriptors described in Chapter 6.

```
stack
 └── page: book_detail
      ├── content: book_edit_view        ← view (Chapter 6)
      └── panels:
           └── panel: authors            ← split area, pos: right
                └── content: book_authors_view   ← view (Chapter 6)
```

### 5.2 Merge Model

`pages` and `views` follow the same flat dict merge model as `types` and
`tables`. There is no namespace wrapping: a plugin that declares `book_list`
contributes to the same `book_list` key as any other plugin that declares it.

This means a derived plugin can extend any page or view declared by a base
plugin — adding a column, inserting a panel, changing a policy — using exactly
the same merge semantics described in Chapter 2.

```yaml
# base plugin (libapp) — declares the page
pages:
  book_list:
    title: Books
    content:
      $ref: "views.book_list_view"

# derived plugin (hr) — adds a column to the view
views:
  book_list_view:
    columns:
      - field: department
        title: Department
        $after: title
```

**Collision avoidance.** Two unrelated plugins that happen to declare the same
page id would be merged together, which is almost certainly wrong. The
`vendor_module` naming convention (§ 1.3) prevents this: `libapp_book_list`
and `hr_book_list` are unambiguously separate pages.

**`$ref` paths** use the standard dot-notation `section.id`:

```yaml
content:
  $ref: "views.book_list_view"
```

### 5.3 Page Definition

```yaml
pages:

  book_detail:
    title: "Book"
    breadcrumb: "$props.title"     # dynamic breadcrumb from stack props
    confirm_on_leave: true         # warn before navigating away with unsaved changes
    content:
      $ref: "views.book_edit_view"
    panels:
      - id: authors
        pos: right
        width: 320
        collapsed: false
        trigger:
          event: form_load
          pass: [id]
        $ref: "views.book_authors_view"
```

#### Page properties

| Property | Description |
|----------|-------------|
| `title` | Static title shown in the stack header |
| `breadcrumb` | Dynamic title, can reference `$props` passed via stack push |
| `confirm_on_leave` | If true, prompts the user before closing with unsaved changes |
| `content` | Main content area — inline descriptor or `$ref` to a view |
| `panels` | List of split areas (see § 5.4) |

### 5.4 Panel (Split Area)

A panel is a split area attached to its parent page or to another panel,
creating nested layouts.

```yaml
panels:
  - id: detail
    pos: right          # right | bottom | left | top
    width: 400          # used when pos is right or left (pixels)
    height: 250         # used when pos is bottom or top (pixels)
    collapsed: false    # initial collapsed state
    trigger:
      event: row_click  # what event loads this panel's content
      from: content     # which area fires the event (default: content)
      pass: [id]        # which fields to forward
    $ref: "views.loan_detail_view"
```

#### Panel properties

| Property | Default | Description |
|----------|---------|-------------|
| `id` | — | Identity key for smart merge |
| `pos` | — | Attachment position: `right`, `bottom`, `left`, `top` |
| `width` | — | Size in pixels (horizontal panels) |
| `height` | — | Size in pixels (vertical panels) |
| `collapsed` | `false` | Whether the panel starts collapsed |
| `trigger` | — | Event that loads the panel content (see § 5.5) |
| `content` | — | Inline content descriptor |
| `$ref` | — | Reference to a view or page |
| `panels` | — | Nested split areas (recursive) |

**Layout order.** Panels are rendered in declaration order: the last panel in
the list becomes the outermost split, spanning the full available space on its
axis. This controls whether a bottom panel spans below the side panel or vice
versa.

### 5.5 Triggers

A trigger defines when and how a panel loads its content — typically in
response to a user action in another area.

```yaml
trigger:
  event: row_click       # event name fired by the source area
  from: content          # source area id (default: content)
  pass: [id, title]      # fields forwarded to the panel as $trigger.*
```

Inside the panel's view, forwarded fields are accessed as `$trigger.id`,
`$trigger.title`, etc.

Common events:

| Event | Fired when |
|-------|-----------|
| `row_click` | User clicks a row in a table view |
| `form_load` | A form finishes loading a record |
| `row_select` | User selects one or more rows |

### 5.6 Merge Behaviour for Pages

`pages` is a flat dict merged by key, consistent with `types` and `tables`.
Within each page, the smart-merge rules from Chapter 2 apply to all lists:

- `panels[]` — smart-merged by `id`

A derived plugin can insert, replace, or remove a panel without redefining
the entire page descriptor.

### 5.7 Auto-generated Pages

When the frontend requests a page not declared in any plugin, the backend
generates a descriptor automatically from the table schema:

```
book_list    ← explicit YAML
Book_list    ← auto-generated from table "Book"
Book_form    ← auto-generated from table "Book"
```

Resolution order in `get_page`:
1. Exact id match in the `pages` dict
2. Auto-generate from table schema

Auto-generated pages exclude primary keys (shown as read-only) and map
foreign keys to combobox widgets.

### 5.8 Lookup Mode *(planned)*

The same page can behave either as a normal navigation target or as a record
picker, depending on the runtime context. When opened with
`$ctx.mode = "lookup"`, a table view returns the selected record to the caller
instead of navigating to a detail page.

```typescript
const result = await stack.pushLookup({ page: 'author_list' });
if (result) { formData.author_id = result.id; }
```

The backend first looks for a dedicated `author_lookup` page (with reduced
columns and pre-applied filters); if not found, it falls back to the standard
list page in lookup mode.

---

## 6. Data Source

### 6.1 Purpose

A `source` block defines where a view gets its data. All view types (table,
form, and future types) share the same source structure. The `type` of the
view determines how the data is rendered; the source determines where it
comes from.

### 6.2 Source Types

#### Model source (database)

The standard source for views backed by the application database. The
querybuilder runs server-side and returns only the requested records.

```yaml
source:
  model: Book                              # table declared in `tables`
  joins:
    - Publisher: "Book.publisher_id = Publisher.id"
    - via: BookAuthor                      # M2M via junction table
      where: [BookAuthor.book_id = $trigger.id]
  order_by: [title, -publication_date]    # prefix - = descending
  id: $props.id                           # for forms: which record to load
```

Server-side filtering (`where` clauses), sorting, and joins are available
only with model sources.

#### Endpoint source (virtual)

When data comes from a custom backend endpoint rather than a direct DB query.
The endpoint returns an array; the view displays it as-is.

```yaml
source:
  endpoint: compute_invoices_for_period
  params:
    period: $props.period
```

Client-side filtering (text search, column sort) is available. Server-side
`where` / `joins` are not applicable — the data shape is owned by the
endpoint.

This source type is used for computed or aggregated data that cannot be
expressed as a simple model query: cross-period reports, multi-step
calculations, data assembled from external APIs.

#### Inline source (static array)

For prototyping, testing, or simple configuration-driven views where the
data is passed directly via props.

```yaml
source:
  data: $props.items
```

All filtering is client-side.

### 6.3 Source Properties Reference

| Property | Source type | Description |
|----------|-------------|-------------|
| `model` | model | Table name as declared in `tables` |
| `id` | model | Record id for forms — `$props.id` or `$trigger.id` |
| `joins` | model | List of join definitions (simple or M2M) |
| `order_by` | model | Default sort — list of fields, prefix `-` for descending |
| `endpoint` | endpoint | Op name passed to `api.endpoint()` |
| `params` | endpoint | Parameters forwarded to the endpoint |
| `data` | inline | Array reference, typically `$props.items` |

### 6.4 Dynamic Values (`$props` and `$trigger`)

Source properties can reference runtime values using two prefixes:

| Prefix | Origin | Typical use |
|--------|--------|-------------|
| `$props` | Stack push — values passed when navigating to the page | `id`, `title`, date range |
| `$trigger` | Event forwarded by a trigger (§ 5.5) | `id` of the clicked row |

```yaml
source:
  model: Loan
  id: $trigger.id          # loaded when row_click fires from the parent view

source:
  endpoint: get_invoices
  params:
    period: $props.period   # passed when the page is pushed onto the stack
```

### 6.5 Joins (Model Source)

#### Simple join

```yaml
joins:
  - Publisher: "Book.publisher_id = Publisher.id"
  - LibraryUser: "Loan.library_user_id = LibraryUser.id"
```

Joined fields are referenced as `Model.field` in `columns` and `order_by`.

#### Many-to-many join

```yaml
joins:
  - via: BookAuthor                      # junction table
    where: [BookAuthor.book_id = $trigger.id]
```

The junction table resolves both FK sides automatically.

### 6.6 Filtering

With a model source, filtering is handled server-side by the querybuilder.
The toolbar `search` and `filter` actions translate user input into `where`
clauses before the query is sent.

With endpoint and inline sources, the same toolbar actions operate on the
array already in memory — no additional server round-trip occurs. The
available filter operations are limited to what can be computed client-side
(text match, range, equality).

---

## 7. Table View

### 7.1 Purpose

A table view renders a list of records as an interactive data grid. It
supports sorting, searching, filtering, row selection, and per-row actions.

```yaml
views:

  book_list_view:
    type: table
    source:
      model: Book
      joins:
        - Publisher: "Book.publisher_id = Publisher.id"
      order_by: [title]
    columns:
      - field: id
      - field: title
      - field: Publisher.name
        title: Publisher
      - field: price
        align: right
        formatter: price_color
      - field: status
    actions:
      toolbar: [add, search, filter, select, export]
      row:
        - id: edit
          action: stack_push
          page: book_detail
          pass: [id, title]
        - id: delete
          action: endpoint
          op: db
          params: {table: Book, method: delete}
          confirm: true
```

### 7.2 Column Properties

| Property | Description |
|----------|-------------|
| `field` | Field name — identity key for merge. Use `Model.field` for joined fields |
| `title` | Column header (overrides type-level label) |
| `align` | Text alignment: `left`, `center`, `right` |
| `formatter` | Frontend formatter id for custom cell rendering |
| `width` | Column width in pixels |
| `sortable` | Whether the column is sortable (default: true) |

### 7.3 Actions

#### Toolbar actions

```yaml
actions:
  toolbar: [add, search, filter, select, export]
```

| Action | Description |
|--------|-------------|
| `add` | Opens a new-record form |
| `search` | Text search bar |
| `filter` | Advanced filter panel |
| `select` | Enables multi-row checkbox selection |
| `export` | Exports visible rows to CSV/Excel |

#### Row actions

```yaml
actions:
  row:
    - id: edit
      action: stack_push     # push a page onto the stack
      page: book_detail
      pass: [id, title]      # fields forwarded as $props

    - id: delete
      action: endpoint       # call a backend endpoint directly
      op: db
      params: {table: Book, method: delete}
      confirm: true          # show confirmation dialog

    - id: preview
      action: lookup         # open page in lookup/read-only mode
      page: book_detail
      pass: [id]
```

### 7.4 Merge Behaviour

`columns[]` is smart-merged by `field`. `actions.row[]` is smart-merged by
`id`. `actions.toolbar` is a scalar list (plain append, no removal).

A derived plugin can add, modify, or remove columns and row actions without
redefining the full view.

---

## 8. Form View

### 8.1 Purpose

A form view renders a single record for viewing or editing. It is the
standard detail/edit surface in a management application.

```yaml
views:

  book_edit_view:
    type: form
    source:
      model: Book
      id: $props.id          # null → new record; int → load existing
    fields:
      - name: title
      - name: isbn
      - name: publisher_id
        widget: combobox
      - name: price
      - group: Details
        fields:
          - name: language
          - name: pages
          - name: description
            widget: textarea
    policy:
      editable: true
    actions:
      toolbar: [save, cancel]
```

### 8.2 Field Properties

| Property | Description |
|----------|-------------|
| `name` | Field name — identity key for merge |
| `widget` | Widget override (`combobox`, `textarea`, `password`, `email`, …) |
| `label` | Label override (overrides type-level label) |
| `help` | Tooltip shown next to the field |
| `readonly` | Makes this field read-only regardless of form policy |

#### Field groups

```yaml
fields:
  - name: title
  - group: Details          # identity key for merge is `group`
    fields:
      - name: language
      - name: pages
```

A `group` creates a collapsible section. Its nested `fields[]` list is
smart-merged by `name` — a derived plugin can add fields inside a group
without redefining it.

### 8.3 Policy

```yaml
policy:
  editable: false     # entire form is read-only (detail view)
```

When `editable: false` the form renders all fields as read-only and the
toolbar typically shows only navigation actions (no save/cancel).

### 8.4 Actions

Form toolbar actions follow the same structure as table toolbar actions.
Common form actions:

| Action | Description |
|--------|-------------|
| `save` | Save the current record |
| `cancel` | Discard changes and pop the stack |
| `delete` | Delete the record and pop the stack |

### 8.5 Merge Behaviour

`fields[]` is smart-merged by `name` (or `group` for group items). Nested
`fields[]` inside groups are also smart-merged recursively.

A derived plugin can add a field to a group, change a widget, or make a
field read-only — without redefining the form.

---

## 9. Menu

The `menus` and `menu_items` sections declare the application's navigation.
Like every other section they are merged from all plugins (§ 1.5); the tree is
assembled on demand by the `get_menu` endpoint.

### 9.1 Content vs Layout

A **generic plugin does not know the menu structure** of the app it will end up
in — hardcoding it would break the agnostic core. It declares only its own
**surfaceable entries** (content). The **app plugin owns the structure**
(layout): loading last (it `depends_on` the others), its placement decisions win
the merge.

**Opt-in by declaration.** Declaring an entry *is* the opt-in — there is no
`surface: true` flag. A plugin of pure tables or services, with no user
destinations, contributes no entries and stays invisible in the menu.

### 9.2 Flat Items + `parent`

Menu entries are a **flat, mergeable collection**; each carries a `parent`
(and an `order`). The tree is computed from the `parent` links by `get_menu`.
The consequence: **moving an entry is overriding one attribute**, not
remove-and-re-add.

```yaml
# libapp/books/menu.yaml — a content plugin declares its entries
menu_items:
  books:   { label: Books,   icon: book, parent: catalog, order: 30,
             action: stack_push, panel: book_list }
  authors: { label: Authors, icon: user, parent: catalog, order: 10,
             action: stack_push, panel: author_list }
```

A **group** is an entry with no `action`, referenced by others as their
`parent`. Its id is a deliberate hook point: several plugins can converge on the
same `parent: catalog`. Convergence is intentional, not a collision.

```yaml
# app-layout plugin — owns the structure
menu_items:
  catalog: { label: Catalog, icon: book, order: 20 }   # group / hook point
```

To relocate an entry, a later plugin overrides a single attribute:

```yaml
menu_items:
  books: { parent: archive }   # deep-merged scalar, last writer wins → moved
```

### 9.3 Multiple Roots — Each With Its Own Rules

`menus` is not one global tree but a set of **named roots**, keyed by id. Each
root is an independent entry point with its own properties — a full back-office
menu, a restricted kiosk menu, an admin menu — selected at request time by
`get_menu(id)`:

```yaml
menus:
  main:       { label: "Main menu",     home_page: hello_demo }
  production: { label: "Production",     home_page: whoisdoingwhat }
  admin:      { label: "Administration" }
```

An entry belongs to a root via its `root` attribute (defaulted by the cascade,
§ 9.5). A root may declare a `home_page` (opened by default when the menu is
shown). Authorisation *(planned)* is an **orthogonal** filter applied over
whichever root is requested — "restrict the app for a different task" is simply
`get_menu('production')`, not a separate mechanism.

### 9.4 Entry Actions

A leaf entry carries an `action` — the **same vocabulary as view row-actions**
(§ 7.3): a scalar verb plus verb-specific sibling keys, never a nested object,
so diagnostics validate menu targets with the same generic checks.

| `action` | Sibling keys | Effect |
|----------|--------------|--------|
| `stack_push` | `panel`, `props` | open a page in the active tab's stack |
| `endpoint` | `op`, `params` | invoke a backend command |

### 9.5 Default Placement — Config Cascade

`root` and `parent` default through a cascade, so a plugin states them once
instead of on every entry:

```
entry (menu_items.<id>)  →  plugin config.yaml  →  app config.yaml
```

```yaml
# app config.yaml — global fallback
menu:
  default_root: main

# plugin config.yaml — "my entries default into the catalog group"
menu:
  default_parent: catalog
```

An explicit attribute on the entry wins over the plugin default, which wins over
the app default. This is not a second opt-in — only the resolution of the
position attributes.

### 9.6 `get_menu`

`get_menu(id)` composes the tree for one root, symmetric to `get_page`:

1. **Compose** — the merged `menus` / `menu_items` (done by the loader).
2. **Resolve defaults** — fill `root` / `parent` via the cascade (§ 9.5).
3. **Auth filter** *(planned)* — recursively drop entries whose `access:` the
   caller does not satisfy (server-side, non-bypassable).
4. **Build tree** — nest by `parent`, order by `order`.
5. **Resolve `$ref`** — expand entries reused across roots.

The client receives a composed, filtered, ordered tree and does not know how it
was built — the same contract as `get_page`. **Menu is semantic, not visual:**
`get_menu` returns groups, leaves, actions, `icon`, `order`; how it renders
(sidebar tree, dropdown menubar, command bar) is Chrome's decision, and the same
root can render differently in different Chromes without changing the descriptor.

**Fallback auto-menu** *(planned).* As `get_page` auto-generates `{table}_list` /
`{table}_form` for tables with no declared page, `get_menu` will fall back to an
**auto CRUD menu over all tables** when a root is empty or undeclared: you design
the tables and get a working menu immediately, then refine by declaring explicit
`menu_items` (explicit always wins — same resolution order as `get_page`). It is
a deterministic derivation of the git-tracked `tables`, never orphan-tables
sneaking into an already-declared menu.

---

## 10. Other View Types *(future)*

Beyond `table` (Chapter 7) and `form` (Chapter 8), further view types are
planned — **kanban**, **cards**, **calendar/gantt**. They share the same
`source` block (Chapter 6) and the same merge semantics; only the rendering and
a few type-specific properties differ. A view's `type` selects the renderer, so
adding a view type is additive — no change to the plugin model.

## 11. Localization and Help *(partly implemented)*

**Localization** is implemented at the framework level (per-locale string files,
a reactive `_()` on the client, request-scoped locale on the server); its
plugin-declaration surface (per-plugin locale files, translated labels/help) is
being consolidated. **Contextual help** is designed but not yet built: a
three-level progressive disclosure (`help_short` tooltip → `help_detail` popup →
full `help_url` page), sourced from per-element Markdown files, filtered by the
user's roles, and assembled into a whole-app manual that mirrors the menu tree.

## 12. Roles and Permissions *(future)*

Role-based access control is planned as an **orthogonal filter**, not a new
mechanism: menu entries and descriptors carry an `access:` hint filtered
server-side; row-level record rules reuse the existing **query behavior** pattern
(§ 3.6) — the same lever as `Archivable`, applied to visibility. Nothing in the
core hardcodes a permission model; it is policy layered on top.

## 13. Workflows *(future)*

State machines for `transaction`-category tables: a declared set of states and
transitions that gate which fields are editable and which actions are available,
with hooks for side effects on transition. Designed to sit on top of the model
and descriptor layers, not to alter them.
