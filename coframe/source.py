from coframe.db import DB, Field


class Generator:

    def __init__(self, db: DB):
        self.db = db

    def generate(self, filename="model.py"):
        self.tables = {}
        self.imports = ""

        for name in self.db.tables_list:
            base = []
            table = self.db.tables[name]
            source = f"class {name}(Base@base):\n"
            source += " "*4 + f"__tablename__ = '{table.table_name}'\n"
            for column in table.columns:
                source += self._gen_column(column)
                # source += " "*4 + f"{column['name']}\n"
                # print(column['name'])
                # print(column['field_type'].python_type.__name__)
            source = source.replace("@base", ', '.join(base))
            source += "\n"
            self.tables[name] = source
            print(source)

    def _gen_column(self, column: Field):
        s = " "*4

        if not column.type:
            s += f"Error: undefined type for {column.name}\n"
            return s

        t = column.type
        py_type = t.python_type.__name__
        sa_type = t.name
        if t.inheritance:
            sa_type = t.inheritance[-1]
        length = column.attributes.get("length", 0)
        if length:
            sa_type = f"{sa_type}({length})"
        args = [""]
        # if column.get("primary_key", False):
        #     args.append("primary_key=True")
        # if not column.get("nullable", True):
        #     args.append("nullable=False")

        s += f"{column.name}: Mapped[{py_type}] = mapped_column({sa_type}{', '.join(args)})\n"

        return s

        """

 self.inheritance = []

class Review(Base, TimestampMixin):
    __tablename__ = 'reviews'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)
    comment: Mapped[str] = mapped_column(String(500))
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'), nullable=False)
    book_id: Mapped[int] = mapped_column(Integer, ForeignKey('books.id'), nullable=False)

    book: Mapped[int] = relationship("Book", back_populates="reviews")
    user: Mapped[int] = relationship("User", back_populates="reviews")
        """
