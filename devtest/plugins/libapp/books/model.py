from sqlalchemy import Index
from sqlalchemy.orm import validates
from sqlalchemy.ext.hybrid import hybrid_property


class Author:

    @hybrid_property
    def full_name(self):
        """Hybrid property for full name"""
        return f"{self.first_name} {self.last_name}"

    __table_args__ = (
        Index("name", "last_name", "first_name", unique=True),
    )

    def __repr__(self):
        return f"<Author {self.full_name}>"


class Book:

    @validates('isbn')
    def validate_isbn(self, key, value):
        """ISBN validation"""
        if not (len(value) == 10 or len(value) == 13):
            raise ValueError("ISBN must be 10 or 13 characters")
        return value

    @hybrid_property
    def is_available(self):
        """Check if book is available for loan"""
        return self.status == 'A'

    @hybrid_property
    def average_rating(self):
        """Calculate average book rating"""
        if not self.reviews:
            return 0.0
        return sum(review.rating for review in self.reviews) / len(self.reviews)

    def __repr__(self):
        return f"<Book {self.title}>"
