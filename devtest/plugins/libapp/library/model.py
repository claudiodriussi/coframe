from sqlalchemy.ext.hybrid import hybrid_property


class Book:
    """
    Library-specific extensions to Book.
    - status field is defined in library/model.yaml
    - reviews relationship comes from Review.book_id FK (library/model.yaml)
    """

    @hybrid_property
    def is_available(self):
        return self.status == 'A'

    @hybrid_property
    def average_rating(self):
        if not self.reviews:
            return 0.0
        return sum(review.rating for review in self.reviews) / len(self.reviews)
