import coframe
from coframe.endpoints import endpoint


def ok():
    print("ok")
    return True


@endpoint('sayhello')
def say_hello(data):
    name = data.get("name", "World")
    lang = data.get("lang", "en")

    if lang == "it":
        message = f"Ciao, '{name}'!"
    elif lang == "es":
        message = f"Hola, '{name}'!"
    else:
        message = f"Hello, '{name}'!"

    return {
        "status": "success",
        "data": message,
        "code": 200
    }


@endpoint('book_stats')
def book_stats(data):
    """What a button in a form can call: it acts, and reports in one line.

    The bench for the fourth family (relations.md §19.1). The record it works on
    arrives as an id resolved from the form's draft (`$record.id`), which is also
    why it must say something sensible when there is none: a book being entered
    has no key yet, and the button is on screen all the same.
    """
    book_id = data.get('id')
    if not book_id or (isinstance(book_id, int) and book_id < 0):
        return {'status': 'error', 'message': 'Save the book first — it has no key yet',
                'code': 400}

    app = coframe.utils.get_app()
    with app.get_session() as session:
        book = session.get(app.model.Book, book_id)
        if book is None:
            return {'status': 'error', 'message': f'No book {book_id}', 'code': 404}

        loans = session.query(app.model.Loan).filter_by(book_id=book_id).count()
        out = session.query(app.model.Loan).filter_by(book_id=book_id, returned_at=None).count()
        reviews = session.query(app.model.Review).filter_by(book_id=book_id).all()
        ratings = [r.rating for r in reviews if r.rating is not None]
        average = round(sum(ratings) / len(ratings), 1) if ratings else None

    message = f'{loans} loans ({out} out), {len(reviews)} reviews'
    if average is not None:
        message += f', average rating {average}'

    return {'status': 'success', 'message': message, 'code': 200,
            'data': {'loans': loans, 'on_loan': out,
                     'reviews': len(reviews), 'average_rating': average}}


@endpoint('books')
def query_books(data):
    app = coframe.utils.get_app()

    with app.get_session() as session:
        books = session.query(app.model.Book).all()
        data = []
        for book in books:
            if len(data) > 10:
                break
            data.append(book.title)

    return {
        "status": "success",
        "data": data,
        "code": 200
    }
