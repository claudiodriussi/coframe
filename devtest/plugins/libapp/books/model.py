from sqlalchemy import Index
from sqlalchemy.ext.hybrid import hybrid_property


class Author:

    @hybrid_property
    def full_name(self):
        """Proprietà ibrida per il nome completo"""
        return f"{self.first_name} {self.last_name}"

    __table_args__ = (
        Index("name", "last_name", "first_name", unique=True),
    )

    def __repr__(self):
        return f"<Author {self.full_name}>"
