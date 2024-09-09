#!/usr/bin/python3

import psycopg2
import random
import select
import hashlib

def generate_conditions(nclauses, attributes = ['a', 'b', 'c', 'd'], operators = ['<', '<=', '=', '!=', '>=', '>']):

	if nclauses == 1:
		cols = [random.choice(attributes), random.choice(attributes)]
		oper = ' ' + random.choice(operators) + ' '

		clause = oper.join(cols)

		if random.randint(0,100) < 50:
			clause = 'NOT ' + clause

		return clause


	nparts = random.randint(2, nclauses)

	# distribute the clauses between query parts
	nclauses_parts = [1 for p in range(0, nparts)]

	for x in range(0, nclauses - nparts):
		nclauses_parts[random.randint(0, nparts) - 1] += 1

	parts = []
	for p in range(0, nparts):
		parts.append('(' + generate_conditions(nclauses_parts[p], attributes, operators) + ')')

	c = random.choice([' AND ', ' OR '])

	return c.join(parts)

def generate_data(nrows, attributes = ['a', 'b', 'c', 'd']):

	sql = 'insert into t (' + ','.join(attributes) + ') select '

	attrs = []
	for attr in attributes:

		x = random.choice([-1, 2, 5, 10, 20, 30])

		if x == -1:
			x = random.randint(5, 20)
			expr = '(random() * ' + str(x) + ')::int'
		else:
			expr = 'mod(i,' + str(x) + ')'

		if random.randint(0,100) < 50:
			x = random.choice([2, 5, 10, 20, 30])
			attrs.append('case when mod(i,' + str(x) + ') = 0 then null else ' + expr + ' end')
		else:
			attrs.append(expr)

	sql += ', '.join(attrs) + ' from generate_series(1,' + str(nrows) + ') s(i)'

	return sql

def wait(conn):

	while True:
		state = conn.poll()
		if state == psycopg2.extensions.POLL_OK:
			break
		elif state == psycopg2.extensions.POLL_WRITE:
			select.select([], [conn.fileno()], [])
		elif state == psycopg2.extensions.POLL_READ:
			select.select([conn.fileno()], [], [])
		else:
			raise psycopg2.OperationalError("poll() returned %s" % state)

def run_everywhere(conns, queries):

	curs = [conn.cursor() for conn in conns]

	for q in queries:
		[cur.execute(q) for cur in curs]
		[wait(conn) for conn in conns]

	[cur.close() for cur in curs]

if __name__ == '__main__':

	conns = [
		psycopg2.connect('host=localhost port=5001 user=postgres dbname=postgres', async_=True),
		psycopg2.connect('host=localhost port=5002 user=postgres dbname=postgres', async_=True),
		psycopg2.connect('host=localhost port=5003 user=postgres dbname=postgres', async_=True),
		psycopg2.connect('host=localhost port=5004 user=postgres dbname=postgres', async_=True)]

	[wait(conn) for conn in conns]

	curs = [conn.cursor() for conn in conns]

	# 100 data sets
	for cnt in [30000, 100000, 1000000]:

		for d in range(1,100):

			# generate the data on all versions
			data_sql = generate_data(cnt)
			data_md5 = hashlib.md5(data_sql.encode('utf-8')).hexdigest()

			with open('data.csv', 'a') as f:
				f.write('%s\t%s\n' % (data_md5, data_sql))

			run_everywhere(conns, ['drop table if exists t', 'create table t (a int, b int, c int, d int)', data_sql, 'commit', 'analyze'])

			# generate the clauses
			conditions = []
			for c in range(1, 6):
				for q in range(1,100):
					conditions.append({'clauses' : c, 'conditions' : generate_conditions(c)})

			with open('results.csv', 'a') as f:
				for conds in conditions:
					sql = "select * from check_estimated_rows('select * from t where " + conds['conditions'] + "')"

					[cur.execute(sql) for cur in curs]
					[wait(conn) for conn in conns]
					r = [cur.fetchone() for cur in curs]

					actual_rows = r[0][1]
					estimated_rows = [str(x[0]) for x in r]

					f.write(('%s\t%s\t%s\t%s\t%s\t%s\t%s\n') % (data_md5, cnt, conds['clauses'], conds['conditions'], 'no', actual_rows, '\t'.join(estimated_rows)))

			run_everywhere(conns, ['create statistics s (mcv) on a, b, c, d from t', 'commit', 'analyze'])

			with open('results.csv', 'a') as f:
				for conds in conditions:
					sql = "select * from check_estimated_rows('select * from t where " + conds['conditions'] + "')"

					[cur.execute(sql) for cur in curs]
					[wait(conn) for conn in conns]
					r = [cur.fetchone() for cur in curs]

					actual_rows = r[0][1]
					estimated_rows = [str(x[0]) for x in r]

					f.write(('%s\t%s\t%s\t%s\t%s\t%s\t%s\n') % (data_md5, cnt, conds['clauses'], conds['conditions'], 'yes', actual_rows, '\t'.join(estimated_rows)))
