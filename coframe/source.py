from coframe.db import DB, Field, Table


class Generator:

    def __init__(self, db: DB):
        self.db = db
        self.imports = set()  # standard imports
        self.import_fields = set()  # field type found
        self.import_orm = set()  # field type found
        self.import_plugins = set()  # plugins with models source code

        self.mixins = set()  # all mixin found
        self.tables = {}  # the tables
        self.many2many = {}  # many2many intermediate tables

        self.import_orm.add("Mapped")
        self.import_orm.add("mapped_column")

        for imp in self.db.pm.config.get('source_imports', []):
            self.imports.add(imp)
        for key, data in self.db.pm.plugins.items():
            for imp in data.config.get('source_imports', []):
                self.imports.add(imp)

    def generate(self, filename="model.py"):
        self.relations = {}
        self.back_relations = {}
        self.source = ""
        for name in self.db.tables_list:
            table = self.db.tables[name]

            # we use multiple inheritance to add mixins to the class
            base = table.attributes.get('mixins', [])
            for mixin in base:
                self.mixins.add(mixin)

            # TODO into plugins we have the list of python source code, he have
            # to inspect that sources to find classes declared with the same
            # name of the table to add them to the base class and to imports

            if base:
                source = f"class {name}(Base, {', '.join(base)}):\n"
            else:
                source = f"class {name}(Base):\n"
            source += " "*4 + f"__tablename__ = '{table.table_name}'\n"

            for column in table.columns:
                source += self._gen_column(column, table)

            source = source.replace("@base", ', '.join(base))
            self.tables[name] = source

        for key, value in self.relations.items():
            self.tables[key] += '\n'
            for row in value:
                self.tables[key] += row

        for key, value in self.back_relations.items():
            self.tables[key] += '\n'
            for row in value:
                self.tables[key] += row

        mixin_source = ""
        for mixin in self.mixins:
            table = self.db.types[mixin]
            mixin_source = f"class {mixin}:\n"
            for column in table.columns:
                mixin_source += self._gen_column(column, table)

        self.source += '\n'.join(self.imports)
        self.source += '\n\n'
        self.source += f"from sqlalchemy import {', '.join(self.import_fields)}"
        self.source += '\n'
        self.source += f"from sqlalchemy.orm import {', '.join(self.import_orm)}"
        self.source += '\n\n'
        self.source += 'from coframe.db import Base\n'
        self.source += '\n\n'

        if mixin_source:
            self.source += mixin_source
            self.source += '\n\n'

        for m2m_class in self.many2many:
            self.resolve_m2m(m2m_class)

        for name in self.db.tables_list:
            self.source += self.tables[name]
            self.source += '\n\n'

        self.source += self.db.pm.config.get('source_add', '')

        with open(filename, 'w') as f:
            f.write(self.source)

    def resolve_m2m(self, m2m_class: str):
        m2m = self.many2many[m2m_class]
        if len(m2m) != 2:
            raise ValueError(f"many2many class: {m2m_class} must be declare in exactly 2 tables")

        # to be consistent, the first table name must be equal to the first part of m2m class name, if not we swap tables
        if not m2m_class.startswith(m2m[0][0].name):
            m2m[0], m2m[1] = m2m[1], m2m[0]
        table1 = m2m[0][0]
        col1 = m2m[0][1]
        table2 = m2m[1][0]
        col2 = m2m[1][1]

        indent = " "*4
        s = ""
        s += f"class {m2m_class}(Base):\n"
        s += f"{indent}__tablename__ = '{table1.table_name}_{table2.table_name}'\n"

        print(s)
        """

class BookAuthors(Base):
    __tablename__ = 'book_authors'
    book_id: Mapped[int] = mapped_column(Integer, ForeignKey('books.id'), primary_key=True)
    author_id: Mapped[int] = mapped_column(Integer, ForeignKey('authors.id'), primary_key=True)
    book: Mapped["Book"] = relationship("Book", overlaps="authors")
    author: Mapped["Author"] = relationship("Author", overlaps="books")

class Author:
    books: Mapped[List["Book"]] = relationship(secondary="book_authors", back_populates="authors", overlaps="book")

class Book:
    authors: Mapped[List["Author"]] = relationship(secondary="book_authors", back_populates="books", overlaps="author")


  Author:
    columns:
      - name: m2m.BookAuthor
        type: Book.id

  Book:
    columns:
      - name: m2m.BookAuthor
        type: Author.id

__tablename__ = books_authors # from'm2m_class' table.name + table.name, the first table is found from class name

        """

    def _gen_column(self, column: Field, table: Table):
        indent = " "*4
        s = indent

        # save m2m for later resolution
        if 'm2m_class' in column.attributes:
            m2m_class = column.attributes['m2m_class']
            m2m = self.many2many.get(m2m_class, [])
            m2m.append([table, column])
            self.many2many[m2m_class] = m2m
            return ""

        # column type
        t = column.type
        py_type = t.python_type.__name__
        sa_type = t.name
        if t.inheritance:
            sa_type = t.inheritance[-1]
        self.import_fields.add(sa_type)

        args = []
        for a in column.attr_type:
            args.append(f"{a}={column.attr_type[a]}")
        if args:
            sa_type = f"{sa_type}({', '.join(args)})"

        # foreign key
        foreign = ""
        if 'foreign_key' in column.attributes:
            fk = column.attributes['foreign_key']
            fid = column.attributes['foreign_id']
            args = [""]
            for a in column.attr_relation:
                args.append(f"{a}={column.attr_relation[a]}")
            foreign = f", ForeignKey('{fk.table_name}.{fid}'{', '.join(args)})"
            self.import_fields.add('ForeignKey')

        # column args
        args = [""]
        for a in column.attr_field:
            args.append(f"{a}={column.attr_field[a]}")

        s += f"{column.name}: Mapped[{py_type}] = mapped_column({sa_type}{foreign}{', '.join(args)})\n"
        if py_type == 'datetime':
            self.imports.add("from datetime import datetime")
        if py_type == 'date':
            self.imports.add("from datetime import date")
        if py_type == 'time':
            self.imports.add("from datetime import time")
        if py_type == 'Decimal':
            self.imports.add("from decimal import Decimal")

        if foreign:
            self.relations[table.name] = self.relations.get(table.name, [])
            self.back_relations[fk.name] = self.back_relations.get(table.name, [])
            self.relations[table.name].append(
                f"{indent}{fk.table_name}: Mapped[{py_type}] = "
                f"relationship('{fk.name}', back_populates='{table.table_name}')\n"
            )
            self.back_relations[fk.name].append(
                f"{indent}{table.table_name}: Mapped[List['{table.name}']] = "
                f"relationship('{table.name}', back_populates='{fk.table_name}')\n"
            )
            self.import_orm.add("relationship")
            self.imports.add("from typing import List")

        return s
