from datetime import datetime, timedelta
from pathlib import Path
import coframe.plugins
from coframe.endpoints import endpoint


@endpoint('add')
def add_numbers(data):
    a = data.get("a", 0)
    b = data.get("b", 0)

    return {
        "status": "success",
        "data": a + b
    }


def main():

    # load plugins
    plugins = coframe.plugins.PluginsManager()
    plugins.load_config("config.yaml")
    plugins.load_plugins()

    print(plugins.export_pythonpath())

    # recalc db and source code model generation
    app = coframe.db.Base.__app__
    app.calc_db(plugins)

    model_file = "model.py"
    if plugins.should_regenerate(model_file):
        print("Require regeneration.")
        generator = coframe.source.Generator(app)
        generator.generate(filename=model_file)
    else:
        print("No regeneration required.")

    import model  # type: ignore
    app.model = model

    import plugins.libapp.library as library  # type: ignore
    library.test.ok()

    # open db engine and populate empty db
    db_file = 'devtest.sqlite'
    is_db = Path(db_file).exists()
    app.initialize_db(f'sqlite:///{db_file}')
    if not is_db:
        populate_db(app)

    # a query
    with app.get_session() as session:
        books = session.query(model.Book).all()
        for book in books:
            authors_names = [author.full_name for author in book.authors]
            print(f"- {book.title} by {', '.join(authors_names)}")

    # register extra endpoints
    cp = app.cp
    cp.resolve_endpoints(["devtest.py"])

    # test some endpoints
    command = {
        "operation": "add",
        "parameters": {"a": 5, "b": 3},
    }
    result = cp.send(command)
    print(result)

    command = {
        "operation": "sayhello",
        "parameters": {"name": "Claudio", "lang": "en"},
        "timeout": 5
    }
    result = cp.send(command)
    print(result)

    # interact to db using endpoint
    command = {
        "operation": "Book",
    }
    result = cp.send(command)
    print(result)

    # using standard endpoints get
    command = {
        "operation": "db",
        "parameters": {
            "table": "Book",
            "method": "get",
            "start": 0,
            "limit": 10
        }
    }
    result = cp.send(command)
    print(result)

    # using standard endpoints get 1
    command = {
        "operation": "db",
        "parameters": {
            "table": "Book",
            "method": "get",
            "id": 1,
            "start": 0,
            "limit": 10
        }
    }
    result = cp.send(command)
    print(result)

    # using standard endpoints create
    command = {
        "operation": "db",
        "parameters": {
            "table": "Book",
            "method": "create",
            "data": {
                "isbn": "9788806219451",
                "title": "Le città invisibili",
                "publication_date": datetime(1972, 1, 1),
                "price": 14.90,
                "status": "A"
            }
        }
    }
    result = cp.send(command)
    print(result)

    # get id from just created book
    book_id = result['data']['id']

    # using standard endpoints update
    command = {
        "operation": "db",
        "parameters": {
            "table": "Book",
            "method": "update",
            "id": book_id,
            "data": {
                "price": 16.50
            }
        }
    }
    result = cp.send(command)
    print(result)

    # using standard endpoints delete
    command = {
        "operation": "db",
        "parameters": {
            "table": "Book",
            "method": "delete",
            "id": book_id
        }
    }
    result = cp.send(command)
    print(result)


def populate_db(app):
    """
    populate the test db if it is empty
    """
    print("regenerate test data...")
    model = app.model
    with app.get_session() as session:
        try:
            author1 = model.Author(
                first_name="Italo",
                last_name="Calvino",
                birth_date=datetime(1923, 10, 15),
                nationality="Italian"
            )
            author2 = model.Author(
                first_name="Umberto",
                last_name="Eco",
                birth_date=datetime(1932, 1, 5),
                nationality="Italian"
            )
            session.add_all([author1, author2])
            session.flush()

            book1 = model.Book(
                isbn="9788806219450",
                title="Il barone rampante",
                publication_date=datetime(1957, 1, 1),
                price=15.90,
                status="A"
            )
            book2 = model.Book(
                isbn="9788845274930",
                title="Il nome della rosa",
                publication_date=datetime(1980, 1, 1),
                price=18.50,
                status="A"
            )
            session.add_all([book1, book2])
            session.flush()

            book_author1 = model.BookAuthor(
                book_id=book1.id,
                author_id=author1.id,
                notes="Masterpiece"
            )
            session.add_all([book_author1])
            book_author2 = model.BookAuthor(
                book_id=book2.id,
                author_id=author2.id,
                notes="International bestseller"
            )
            session.add_all([book_author1, book_author2])

            user = model.User(
                name="Mario Rossi",
                email="mario.rossi@example.com",
                username="mrossi",
                password="hashed_password_here"
            )
            session.add(user)
            session.flush()

            loan1 = model.Loan(
                book=book1,
                user=user,
                borrowed_at=datetime.now(),
                due_date=datetime.now() + timedelta(days=30)
            )
            review1 = model.Review(
                book=book1,
                user=user,
                rating=5,
                comment="An italian masterpiece!"
            )
            session.add_all([loan1, review1])

            session.commit()

        except Exception as e:
            print(f"Error during tests: {e}")
            session.rollback()


if __name__ == "__main__":
    main()
