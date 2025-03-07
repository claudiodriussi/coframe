from sqlalchemy import Index
from sqlalchemy.orm import validates
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


class Book:

    @validates('isbn')
    def validate_isbn(self, key, value):
        """Validazione ISBN"""
        if not (len(value) == 10 or len(value) == 13):
            raise ValueError("ISBN deve essere di 10 o 13 caratteri")
        return value

    @hybrid_property
    def is_available(self):
        """Verifica se il libro è disponibile per il prestito"""
        return self.status == 'A'

    @hybrid_property
    def average_rating(self):
        """Calcola il rating medio del libro"""
        if not self.reviews:
            return 0.0
        return sum(review.rating for review in self.reviews) / len(self.reviews)

    def __repr__(self):
        return f"<Book {self.title}>"
