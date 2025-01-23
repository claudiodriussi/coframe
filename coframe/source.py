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

        self.import_orm.add("Mapped")
        self.import_orm.add("mapped_column")

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

        self.source += '\n'.join(self.imports)
        self.source += '\n\n'
        self.source += f"from sqlalchemy import {', '.join(self.import_fields)}"
        self.source += '\n'
        self.source += f"from sqlalchemy.orm import {', '.join(self.import_orm)}"
        self.source += '\n\n'
        self.source += 'from coframe.db import Base\n'
        self.source += '\n\n'

        for mixin in self.mixins:
            table = self.db.types[mixin]
            source = f"class {mixin}:\n"
            for column in table.columns:
                source += self._gen_column(column, table)
            self.source += source
            self.source += '\n\n'

        for name in self.db.tables_list:
            self.source += self.tables[name]
            self.source += '\n\n'

        with open(filename, 'w') as f:
            f.write(self.source)
        print(self.source)

    def _gen_column(self, column: Field, table: Table):
        indent = " "*4
        s = indent

        if not column.type:
            s += f"Error: undefined type for {column.name}\n"
            return s

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
            foreign = f", ForeignKey({fk.table_name}.{fid}{', '.join(args)})"

        # column args
        args = [""]
        for a in column.attr_field:
            args.append(f"{a}={column.attr_field[a]}")

        s += f"{column.name}: Mapped[{py_type}] = mapped_column({sa_type}{foreign}{', '.join(args)})\n"
        if py_type == 'datetime':
            self.imports.add("from datetime import datetime")
        if py_type == 'Decimal':
            self.imports.add("from decimal import Decimal")

        if foreign:
            self.relations[table.name] = self.relations.get(table.name, [])
            self.back_relations[fk.name] = self.back_relations.get(table.name, [])
            self.relations[table.name].append(f"{indent}{fk.table_name}: Mapped[{py_type}] = relationship('{fk.name}', back_populates='{table.table_name}') \n")
            self.back_relations[fk.name].append(f"{indent}{table.table_name}: Mapped[List['{table.name}']] = relationship('{table.name}', back_populates='{fk.table_name}')\n")
            self.import_orm.add("relationship")
            self.imports.add("from typing import List")

        return s
