from db import Database
from query import Query
from config import init

from random import choice, randint, sample, seed
from colorama import Fore, Back, Style

# Student Id and 4 grades
init()
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

file = open("our_output.txt", "w")

records = {}

seed(3562901)

for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    file.write('inserted' + str(records[key]) + '\n')

for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        file.write('select error on' + str(key) + ':' + str(record) + ', correct:' + str(records[key]) + '\n')
    else:
        file.write('select on' + str(key) + ':' + str(record) + '\n')

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            file.write('update error on' + str(original) + 'and' + str(updated_columns) + ':' + str(
                record) + ', correct:' + str(records[key]) + '\n')
        else:
            file.write('update on' + str(original) + 'and' + str(updated_columns) + ':' + str(record) + '\n')
        updated_columns[i] = None

keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, 20):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            file.write(
                'sum error on [' + str(keys[r[0]]) + ',' + str(keys[r[1]]) + ']: ' + str(result) + ', correct: ' + str(
                    column_sum) + '\n')
        else:
            file.write('sum on [' + str(keys[r[0]]) + ',' + str(keys[r[1]]) + ']: ' + str(column_sum) + '\n')
