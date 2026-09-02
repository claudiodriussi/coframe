# Getting Started

How to put Coframe on your machine, write an application that runs, and compile
the client it is served with — starting from nothing but git, Node and uv.

The criterion of this document is hard: **no step may require knowledge that is
not written here.** If you have to *know* something to get through it, that is
not a hiccup in your setup — it is a defect in this text, and worth reporting.

There are two arrivals, and the first does not require the second:

| | you want to | you need |
|---|---|---|
| **Try it** | see an application run, write a schema, get a UI | uv, and nothing on disk |
| **A workstation** | develop applications, shared plugins, or the library itself | the three repositories, Node, uv |

*Last revised: 2026-09-02. Walked on Linux; see [Appendix A](#appendix-a--prerequisites-by-platform)
for what is expected of macOS and Windows.*

---

## What you need

| | |
|---|---|
| **git** | also to install: dependencies come from repositories, not from an index |
| **uv** | it installs Python too, so Python is not a prerequisite of its own |
| **Node** | `^20.19` or `>=22.12`, the floor vite declares; `corepack enable` gives you pnpm |

Only the client needs Node. If you never compile a client, git and uv are enough.

Don't have them? → [Appendix A](#appendix-a--prerequisites-by-platform). Don't
want uv? → [Appendix B](#appendix-b--without-uv), and read why it is the paved
road before leaving it.

---

## 1. A first application, before you clone anything

```bash
uvx --from "coframe @ git+https://github.com/claudiodriussi/coframe" \
    coframe new hello
cd hello
uv sync                     # creates .venv and installs
uv run app.py db-sync       # creates the database from the YAML schema
uv run server_flask.py      # the API on http://localhost:8300
```

**What you should see.** `db-sync` prints the SQL it ran — one `CREATE TABLE
users`, because an application is born knowing only who logs into it. Then, at
<http://localhost:8300/>, the server says what it is and what it hasn't got:

```json
{"application": "hello", "api": "coframe/",
 "client": "not built — run `coframe build-client`"}
```

**There is no page yet, and that is the right answer.** A client is compiled from
the client repository, which you have not cloned — chapter 4 does that. What is
already complete is the API, and it answers:

```bash
curl -X POST -H 'Content-Type: application/json' \
     -d '{"username":"admin","password":"admin"}' \
     http://localhost:8300/coframe/auth/login
# {"status": "success", "data": {"token": "eyJ..."}}
```

Keep that token: `-H "Authorization: Bearer <token>"` is how every other call
identifies itself, and the next chapters use it.

That application takes coframe from the repository at `main`: it follows the
library, and nothing on your disk. The next chapters build one that follows
*your sources* instead.

---

## 2. The workstation: three repositories

**Not inside `hello`.** That directory is an application, and an application
holds no repositories. Leave it — it stays where it is, and you can come back to
it or delete it — and make a directory for the workstation:

```bash
cd ..                       # out of hello
mkdir coframe-station && cd coframe-station

git clone https://github.com/claudiodriussi/coframe.git
git clone https://github.com/claudiodriussi/coframe-ui.git
git clone https://github.com/claudiodriussi/coframe-commons.git
```

| directory | what it is |
|---|---|
| `coframe/` | the library: plugin system, model generation, dispatcher, CLI |
| `coframe-ui/` | the client library and the generic shell |
| `coframe-commons/` | the shared plugins: types, mixins, the party model |

What you end up with, once chapter 3 adds an application of its own:

```
coframe-station/
├── coframe/
├── coframe-ui/
├── coframe-commons/
└── bookshop/           the application, a sibling of the three
```

**Nothing in the framework depends on that arrangement.** Two conveniences do:
the client finds the `devtest` bench by looking for `coframe/devtest` up to three
levels above itself, and an application that declares a shared plugin root writes
the path to it — which is shorter when they are siblings. Your applications say
where they are, and can live anywhere.

Then the environment, still from `coframe-station/`:

```bash
uv venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
uv pip install -e "./coframe[dev]"     # the library, editable, plus the tests
(cd coframe-ui && pnpm install)        # only if you compile a client
```

That venv is the workstation's: it is what gives you the `coframe` command, and
the chapters below assume it is active. It is **not** the environment your
applications run in — each of those has a `.venv` of its own, made by `uv sync`.

`[dev]` brings pytest and both web frameworks. An application installs only the
one it serves with — `coframe[flask]` or `coframe[fastapi]`.

> **If another venv was already active**, deactivate it first: `uv pip install`
> targets `$VIRTUAL_ENV`, not the directory you are in, so it would install into
> that one. `env -u VIRTUAL_ENV uv ...` does the same job without deactivating.

**What you should see.** The `coframe` command exists, and lists what it can do:

```bash
coframe --help              # without the venv activated: .venv/bin/coframe --help
```

---

## 3. An application against your sources

From `coframe-station/`, so that the application lands beside the three
repositories — an application can live anywhere, and this one is a sibling only
to keep the path it writes in chapter 5 short:

```bash
coframe new bookshop
cd bookshop
uv sync
```

Run from a checkout, `coframe new` writes a block the one in chapter 1 did not
have:

```toml
[tool.uv.sources]
coframe = { path = "/…/coframe", editable = true }
```

The application now resolves the library from your working copy: edit the
library, and it sees the change at once. **The line that proves it:**

```bash
uv run python -c "import coframe; print(coframe.__file__)"
# → /…/coframe/coframe/__init__.py    a path inside your checkout
# → /…/.venv/…/site-packages/…        the published copy: the block is missing
```

`uv sync --no-sources` resolves the way a machine without that checkout would —
which is what to run before believing an application is portable.

### The first table

Everything an application *does* lives in its plugins. Open
`plugins/bookshop/model.yaml` and declare a table:

```yaml
tables:

  Book:
    name: books
    label: "Book"
    columns:
      - name: id
        type: Integer
        primary_key: true
        autoincrement: true

      - name: title
        type: String
        length: 200
        nullable: false
        label: "Title"
        searchable: true

      - name: isbn
        type: String
        length: 20
        unique: true
        label: "ISBN"

      - name: price
        type: Numeric
        label: "Price"

      - name: published_on
        type: Date
        label: "Published"
```

```bash
uv run app.py db-check      # what differs, read-only
uv run app.py db-sync       # apply it: additions only, never a drop
```

**What you should see.** `db-sync` names the change before making it:

```
Applicable automatically (1):
  + books                                    new table (5 columns)
```

The server only ever looks: when the schema the plugins describe differs from the
database, it refuses to start and says what differs. Changing the database is
always an explicit command.

And the table you declared is already an interface. With the server running and
a token from `auth/login`:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"id":"book_list"}' http://localhost:8300/coframe/endpoint/get_page
```

```json
{"title": "Book", "_auto": true,
 "content": {"type": "table", "source": {"model": "Book"},
             "columns": [{"field": "id"}, {"field": "title", "title": "Title"}, …],
             "navigator": true}}
```

`_auto: true` — nobody wrote that descriptor. A list and a form exist for every
table, and are replaced by a declared page the day the generated one is not
enough.

---

## 4. The client, in its two forms

An application does not own a client. It contributes interface through the
`.svelte` files of its plugins, and the shell — the only re-pointable client —
compiles them in. **That is why the shell is the norm**: what makes an
application look like itself travels in its plugins.

Both commands look for the client repository next to your coframe checkout;
`COFRAME_UI=/path/to/coframe-ui` when it is elsewhere.

**In development** — two processes, two origins:

```bash
coframe dev
#   server  server_fastapi.py  →  http://localhost:8300
#           against the library at /…/coframe
#   client  shell              ←  /…/bookshop        http://localhost:5174
```

The client is served by vite on a port of its own, so the backend runs with
`COFRAME_DEV=1`: CORS, and the refreshed token exposed in a header that a browser
would otherwise hide from JS across origins. Ctrl-C stops both.

**Compiled** — one process, one origin:

```bash
coframe build-client        # → static/, which this application's server serves
uv run server_flask.py      # http://localhost:8300 — client and API together
```

No CORS is involved, because there is only one origin. When something works in
one form and not in the other, that difference is the first place to look.

> A client of your own is possible — `coframe-ui/apps/devtest` is one, and it
> hosts the playground — but it is only needed for what plugins cannot
> contribute. Today it lives inside the client repository: `@coframe/ui` is
> consumed as a workspace package, so a client outside that checkout has no way
> to depend on it.

The long forms, if you need them:

```bash
cd coframe-ui
COFRAME_APP_ROOT=/path/to/bookshop pnpm --filter shell dev    # localhost:5174
pnpm build:app /path/to/bookshop                              # → bookshop/static/
```

---

## 5. Using what isn't yours: the shared plugins

`coframe-commons` is not a Python dependency and is never installed. It is a
**plugin root**, and an application reaches it by path — which is the whole
integration:

```yaml
# bookshop/config.yaml
plugins:
  - path: ../coframe-commons/plugins
    include: [common]
  - plugins
```

`include` is positive on purpose: what a shared root gains over time stays inert
until an application asks for it by name. Dependencies are followed for you —
`include: [partners]` brings `common` along without naming it.

> Not `users`: a generated application has a `users` plugin of its own, and a
> plugin name provided by two roots is refused, by name and by both paths.

### What arrives

Mostly a **vocabulary of types**. The generated application spells a column out:

```yaml
      - name: price
        type: Numeric
```

With `common` a column says what it *is*, and how it is stored is one line, in
one place:

```yaml
      - name: id
        type: ID              # primary key, autoincrement, labelled

      - name: title
        type: Description     # not nullable by its own definition

      - name: isbn
        type: UpperCode       # indexed, normalised to upper case on write

      - name: price
        type: Money           # Numeric(10,2), and a currency widget in the form
```

```
$ uv run app.py db-check
Applicable automatically (3):
  + configs                                  new table (6 columns)
  + books.isbn                               type VARCHAR(20) -> VARCHAR(32)
  + books                                    new index ix_books_isbn (isbn)
```

Three things happened that nobody asked for by hand: the shared plugin brought
its own table, the type brought its index, and the column was widened to the
width the vocabulary declares.

The interface follows in the same move. Ask for the form of that table, and what
the types know is already in it — with no UI written anywhere:

```
{'name': 'id',           'type': 'ID',         'readonly': True}
{'name': 'title',        'type': 'Description', 'required': True}
{'name': 'isbn',         'type': 'UpperCode'}
{'name': 'price',        'type': 'Money',       'widget': 'currency', 'precision': 10, 'scale': 2}
{'name': 'published_on', 'type': 'Date'}
```

---

## 6. Verifying the workstation

The proof that this setup is reproducible is everything above, done in an empty
directory, without opening any other file. In addition:

```bash
cd coframe         && pytest                     # 444 tests
cd coframe-ui      && pnpm test                  # 124 tests
cd coframe/devtest && python server_fastapi.py   # the library's bench, port 8300
```

And the proof worth twice the others, because no already-working machine can give
it: **clone the three repositories into an empty directory on a machine that has
never had them**, and do this again. That is how you find the file left out of
git, and the path that worked only thanks to something already on your disk.

---

## Appendix A — prerequisites, by platform

Only Linux has been walked end to end. The other two say what is expected.

**Debian / Ubuntu**

```bash
sudo apt install git curl
curl -LsSf https://astral.sh/uv/install.sh | sh     # uv
sudo apt install nodejs npm && corepack enable
uv python install 3.12                              # only if you have no Python
```

**macOS**

```bash
brew install git node uv
corepack enable
```

**Windows**

Everything here is a command line; use PowerShell, and:

- activate a venv with `.venv\Scripts\activate`, not `source`;
- install uv with `winget install astral-sh.uv`, Node with `winget install OpenJS.NodeJS`;
- write paths in `config.yaml` with forward slashes — Python and Node both accept
  them, and a backslash inside a YAML string is an escape.

Nothing in this setup requires symbolic links, so Developer Mode is not needed.

---

## Appendix B — without uv

Everything except `coframe dev` works with a plain venv:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e "./coframe[dev]"
```

Two things to know before choosing this road:

1. **`[tool.uv.sources]` is silently ignored by pip.** An application generated
   from a checkout declares that its dependency is your working copy; pip does
   not read that block, and resolves coframe from `git+…@main` instead. Nothing
   warns you: you edit the library and nothing changes. Install the checkout into
   the application's environment yourself — `pip install -e /path/to/coframe` —
   and check it with the `import coframe; print(coframe.__file__)` line above.
2. **`coframe dev` requires uv**, and says so, naming what to run instead:
   `.venv/bin/python server_flask.py`, alongside `pnpm --filter shell dev`.

pnpm, on the other hand, is not optional: the client is a pnpm workspace whose
packages depend on each other with `workspace:*`, which npm cannot install.
`corepack enable` is all it takes.

---

## Appendix C — the two benches

```bash
cd coframe/devtest      && python server_fastapi.py   # 8300 — the library's bench
cd coframe-commons/demo && coframe dev --no-client    # 8302 — the shared plugins
cd coframe-ui           && pnpm dev                   # 5173 — devtest client + playground
```

`devtest` exercises the library, and is where a feature is demonstrated first.
`demo` has a venv of its own and declares coframe as a dependency: it is the
reference consumer of the shared plugins and behaves like an application outside
the repository, because that is what it has to prove.

---

*Where to go next: [PLUGIN_MODEL.md](PLUGIN_MODEL.md) — how plugins declare the
data model, the UI and the menu, and how the merge composes them.
[SCAFFOLDING.md](SCAFFOLDING.md) — the anatomy of an application: the bootstrap
sequence, the entry points, the two servers.*
