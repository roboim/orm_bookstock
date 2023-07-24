import sqlalchemy
import psycopg2
import json
import os
import datetime
import models
from models import Base, Publisher, Book, Shop, Stock, Sale
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import CheckConstraint, or_

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

def search_sale(publ_name = '', publ_id = 0):
    # запрос
    # название книги | название магазина, в котором была куплена эта книга | стоимость покупки | дата покупки

    q=session.query(Publisher.name, Shop.name, Sale.price, Sale.date_sale). \
        join(Book).join(Stock).join(Shop).join(Sale). \
        filter(or_(Publisher.id == publ_id, Publisher.name == publ_name)).all()
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