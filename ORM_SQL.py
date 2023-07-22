import sqlalchemy
import psycopg2
import json
import os
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import CheckConstraint

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="books")


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer)

    book = relationship(Book, backref="stocks")
    shop = relationship(Shop, backref="stocks")


class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float)
    date_sale = sq.Column(sq.Date)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer)

    stock = relationship(Stock, backref="sales")


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def read_json(file_path):
    with open(file_path, encoding="utf - 8", newline="") as f:
        json_data = json.load(f)
    for record in json_data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()

def search_sale(publ_name,publ_id):
    # запрос
    # название книги | название магазина, в котором была куплена эта книга | стоимость покупки | дата покупки

    if publ_id != 0:
        q=session.query(
            Publisher.name, Shop.name, Sale.price, Sale.date_sale
        ).join(
            Book
        ).join(
            Stock
        ).join(
            Shop
        ).join(
            Sale
        ).filter(
            Publisher.id == publ_id
        ).all()
    else:
        q=session.query(
            Publisher.name, Shop.name, Sale.price, Sale.date_sale
        ).join(
            Book
        ).join(
            Stock
        ).join(
            Shop
        ).join(
            Sale
        ).filter(
            Publisher.name == publ_name
        ).all()
    for res in q:
        print(f'{res[0]} | {res[1]} | {res[2]} | {res[3]}')

if __name__ == '__main__':
    print("Будет выполнено чтение файла tests_data.json из папки проекта в базу данных.")
    database = input("Введите имя базы данных: ")
    password = input("Введите пароль для базы данных: ")
    answer=input("Введите имя или id издателя: ")
    try:
        answer_int=int(answer)
    except:
        answer_int=0

    DSN = "postgresql://postgres:" + password + "@localhost:5432/" + database
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        read_json('tests_data.json')
        search_sale(answer,answer_int)