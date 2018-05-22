import pymongo
import json


def read_data():
    client = pymongo.MongoClient('localhost')
    db = client['Qiongyou_American']
    table = db.topics.find()
    raw_data = []
    links = []
    for row in table:
        links.append(row['link'])
        raw_data.append(row)
    return links, raw_data


def save_to_file():
    _, raw_data = read_data()
    with open('Qiongyou_American_topics.json', 'w+', encoding='utf-8') as f:
        for row in raw_data:
            del row['_id']
            f.write(json.dumps(row, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    save_to_file()

