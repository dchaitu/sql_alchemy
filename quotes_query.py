import json
import sys
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, Session, sessionmaker

engine = sa.create_engine('sqlite:///quotes.db', echo=True)

conn = engine.connect()
metadata = sa.MetaData()
Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()


class Author(Base):
    __tablename__ = "author"

    author_id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    born = sa.Column(sa.String)
    reference = sa.Column(sa.String, unique=True)


class Quote(Base):
    __tablename__ = "quote"

    quote_id = sa.Column(sa.Integer, primary_key=True,autoincrement=True)
    content = sa.Column(sa.String)
    author_id = sa.Column(sa.Integer, sa.ForeignKey('author.author_id'), nullable=False)


class Tag(Base):
    __tablename__ = "tag"

    tag_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    content = sa.Column(sa.String)

# Through table model
class QuoteTag(Base):
    __tablename__ = "quote_tag"
    quote_tag_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    quote_id = sa.Column(sa.Integer, sa.ForeignKey('quote.quote_id'), nullable=False)
    tag_id = sa.Column(sa.Integer, sa.ForeignKey('tag.tag_id'), nullable=False)



def insert_authors():
    with open("./updated_quotes.json", "r") as f:
        json_data = json.load(f)
        author_objs = []
        for author in json_data["authors"]:
            if author["name"] not in author_objs:
                insert_author(name=author["name"], born=author["born"], reference=author["reference"])
            author_objs.append(author["name"])

def insert_author(name, born, reference):
    if not session.query(Author).filter_by(name=name).first():
        query = Author(name=name, born=born, reference=reference)
        # conn.execute(query)
        session.add(query)
        session.commit()

def insert_quotes():
    with open("./updated_quotes.json", "r") as f:
        json_data = json.load(f)
        for quote in json_data["quotes"]:
            author_id = get_author_id(quote['author'])
            insert_quote(content=quote['quote'],author_id=author_id)


def insert_quote(content, author_id):
    if not session.query(Quote).filter_by(content=content).first():
        query = Quote(content=content, author_id=author_id)
        # conn.execute(query)
        session.add(query)
        session.commit()

def insert_tags():
    with open("./updated_quotes.json", "r") as f:
        json_data = json.load(f)
        for quote in json_data["quotes"]:
            tags = quote["tags"]
            for tag in tags:
                if not session.query(Quote).filter_by(content=tag).first():
                    query = Tag(content=tag)
                    session.add(query)
            session.commit()

def insert_quote_tag():
    with open("./updated_quotes.json", "r") as f:
        json_data = json.load(f)
        for quote in json_data["quotes"]:
            quote_id = quote["id"]
            tags = quote["tags"]
            for tag in tags:
                query = session.query(Tag).filter_by(content=tag).first()
                print(f"tag_id {query.tag_id} content {query.content}")
                if query:
                    result = session.query(Tag).filter_by(content=query.content).first()
                    tag_id = result.tag_id if result else None
                    print(f"tag_id: {tag_id}")
                    query = QuoteTag(tag_id=tag_id, quote_id=quote_id)
                    session.add(query)
            session.commit()



def get_author_id(author_name):
    author = session.query(Author).filter_by(name=author_name).first()
    print(f"author_id:- {author}")
    return author.author_id

def get_quote(quote_id):
    query = session.query(Quote).filter_by(quote_id=quote_id).first()
    result = query.content
    return result

def get_quotes_by_author(author_name):
    author_id = get_author_id(author_name)
    query = session.query(Quote).filter_by(author_id=author_id).all()
    quote_list = []
    for quote in query:
        quote_list.append(quote.content)
    return quote_list

def get_quotes_by_tag(tag):
    # tag_id = session.query(Tag).filter_by(content=tag).first().tag_id
    # quotes = session.query(QuoteTag).filter_by(tag_id=tag_id)
    # quote_list = []
    # for quote in quotes:
    #     quote_content = session.query(Quote).filter_by(quote_id=quote.quote_id).first().content
    #     quote_list.append(quote_content)
    results = session.query(Quote.content).join(QuoteTag, Quote.quote_id == QuoteTag.quote_id).join(Tag, QuoteTag.tag_id== Tag.tag_id).filter(Tag.content==tag).all()
    quote_list = [result[0] for result in results]

    return quote_list

def get_quotes_by_search_text(search_text):
    query = session.query(Quote).filter(Quote.content.like(f'%{search_text}%'))
    quotes = [q.content for q in query]
    return quotes




def populate_table():
    # insert_authors()
    # insert_quotes()
    # insert_tags()
    # insert_quote_tag()
    pass


if __name__ == '__main__':
    Base.metadata.create_all(bind=engine)
    # populate_table()
    if sys.argv[1] == "--quote" or sys.argv[1] == "-q":
        quote_id = int(sys.argv[2])
        quote = get_quote(quote_id)
        print(quote)

    elif sys.argv[1] == "--author" or sys.argv[1] == "-a":
        author_name = sys.argv[2]
        author_quotes = get_quotes_by_author(author_name)
        print(author_quotes)

    elif sys.argv[1] == "--tag" or sys.argv[1] == "-t":
        tag = sys.argv[2]
        author_quotes = get_quotes_by_tag(tag)
        print(author_quotes)

    elif sys.argv[1] == "--search" or sys.argv[1] == "-s":
        search_text = sys.argv[2]
        author_quotes = get_quotes_by_search_text(search_text)
        print(author_quotes)



