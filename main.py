import os
import json

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Book, Shop, Stock, Sale


def import_data(data):
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[data.get('model')]
    session.add(model(id=data.get('pk'), **data.get('fields')))
    session.commit()


def fill_db_from_file(filename):
    with open(filename, 'r') as file:
        tests_data = json.load(file)

    for d in tests_data:
        import_data(d)


def query_to_dataframe(query):
    dataframe = pd.DataFrame()

    for publisher, book, shop, stock, sale in query:
        row = {
            'book_title': book.title,
            'shop_name': shop.name,
            'sale_price': sale.price,
            'sale_date': sale.date_sale
        }
        dataframe = pd.concat([dataframe, pd.DataFrame(row, index=[0])])

    return dataframe


def get_sales_by_publisher(publisher_search_query):
    data = session.query(Publisher, Book, Shop, Stock, Sale)
    if publisher_search_query.isnumeric():
        data = data.filter(Publisher.id == publisher_search_query)
    else:
        data = data.filter(Publisher.name == publisher_search_query)

    data = data.filter(
        Publisher.id == Book.id_publisher
    ).filter(
        Book.id == Stock.id_book
    ).filter(
        Stock.id_shop == Shop.id
    ).filter(
        Sale.id_stock == Stock.id
    ).all()

    return data


if __name__ == '__main__':
    db = os.getenv('DBNAME')
    user = os.getenv('DBUSER')
    password = os.getenv('DBPASS')

    DSN = f'postgresql://{user}:{password}@localhost:5432/{db}'
    engine = sqlalchemy.create_engine(DSN)

    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    fill_db_from_file('fixtures/tests_data.json')

    search_input = input('Введите имя или идентификатор писателя: ')
    query = get_sales_by_publisher(search_input)

    print(query_to_dataframe(query).to_markdown())

    session.close()