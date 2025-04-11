#!/usr/bin/env python

# this script lead a csv file from:
# https://www.kaggle.com/code/hoshi7/goodreads-analysis-and-recommending-books
# and import the books, useful to have some data to do tests.

import sys
import os
import csv
import random
import datetime
from typing import Dict, List, Tuple, Optional

# add the path of coframe package
sys.path.append("..")
import coframe  # noqa: E402


def parse_authors(authors_str: str) -> List[Tuple[str, str]]:
    """
    Convert the authors string into a set of tuples.
    The authors are separated ny slash and first name and last name are serarated by space

    Args:
        authors_str: String with authors (i.e. "John Doe / Jane Smith")

    Returns:
        Set of tupel (first name, last name)
    """
    result = []
    if not authors_str or authors_str.strip() == "":
        return result

    authors_list = [a.strip() for a in authors_str.split("/")]

    for author in authors_list:
        parts = author.split()
        if len(parts) == 1:
            # only last name
            result.append(("", parts[0]))
        else:
            first_name = " ".join(parts[:-1])
            last_name = parts[-1]
            result.append((first_name, last_name))

    return set(result)


def parse_publication_date(date_str: str) -> Optional[datetime.date]:
    """
    Convert a string form CSV to a datetime.date object

    Args:
        date_str: String containing the date

    Returns:
        datetime.date object or None
    """
    if not date_str or date_str.strip() == "":
        return None

    try:
        # Try some common formats
        formats = ["%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"]

        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue

        # All formats failed. We try to see if it is only a year
        if date_str.strip().isdigit() and len(date_str.strip()) == 4:
            year = int(date_str.strip())
            return datetime.date(year, 1, 1)

        return None
    except Exception as e:
        print(f"Error converting the date '{date_str}': {e}")
        return None


def import_books_from_csv(csv_path: str) -> None:
    """
    Import books and authors from csv file into the devtest db.

    Args:
        csv_path: CSV file to import
    """
    # initialize Coframe
    plugins = coframe.plugins.PluginsManager()
    plugins.load_config("config.yaml")
    plugins.load_plugins()

    app = coframe.utils.get_app()
    app.calc_db(plugins)

    # import the model
    import model  # type: ignore
    db_file = 'devtest.sqlite'
    app.initialize_db(f'sqlite:///{db_file}', model)

    # track existing authors to avoid duplicates
    existing_authors: Dict[Tuple[str, str], int] = {}

    # read the CSV file
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        with app.get_session() as session:
            # first of all read authors already in database
            existing_authors_query = session.query(model.Author).all()
            for author in existing_authors_query:
                existing_authors[(author.first_name, author.last_name)] = author.id

            # import books and create new authors if needed
            for row in reader:
                try:
                    # extract some data
                    title = row.get('title', '').strip()
                    isbn = row.get('isbn', '').strip()
                    isbn13 = row.get('isbn13', '').strip()
                    publication_date_str = row.get('publication_date', '')
                    authors_str = row.get('authors', '').strip()

                    # choose ISBN13 if ISBN is void. If ISBN is invalid the book wil be not created.
                    if not isbn:
                        if isbn13:
                            isbn = isbn13

                    # search if file is present
                    existing_book = session.query(model.Book).filter_by(isbn=isbn).first()
                    if existing_book:
                        print(f"Book already present: {title} (ISBN: {isbn})")
                        continue

                    # since this DB doesn't has prices, we calc random
                    price = round(random.uniform(5.0, 50.0), 2)

                    publication_date = parse_publication_date(publication_date_str)

                    # create the new book
                    new_book = model.Book(
                        title=title,
                        isbn=isbn,
                        publication_date=publication_date,
                        price=price,
                        status='A'
                    )
                    session.add(new_book)
                    session.flush()  # to get ID

                    # processa authors
                    authors = parse_authors(authors_str)
                    for first_name, last_name in authors:
                        # find the author
                        author_key = (first_name, last_name)
                        if author_key in existing_authors:
                            author_id = existing_authors[author_key]
                            author = session.get(model.Author, author_id)
                        else:
                            # or create a new one
                            author = model.Author(
                                first_name=first_name,
                                last_name=last_name,
                                nationality=""
                            )
                            session.add(author)
                            session.flush()  # to get ID
                            existing_authors[author_key] = author.id
                            author_id = author.id

                        # create the realtion
                        book_author = model.BookAuthor(
                            book_id=new_book.id,
                            author_id=author_id,
                            notes=""
                        )
                        session.add(book_author)

                    session.commit()
                    print(f"Add book: {title}")

                except Exception as e:
                    print(f"Error in row: {e}")
                    session.rollback()
                    continue

            try:
                session.commit()
                print("Successful imported!")
            except Exception as e:
                session.rollback()
                print(f"commit errore: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        csv_path = 'books.csv'
    else:
        csv_path = sys.argv[1]

    if not os.path.exists(csv_path):
        print(f"Error: the file {csv_path} doesn't exists.")
        sys.exit(1)

    import_books_from_csv(csv_path)
