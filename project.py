######################################################################
#
# HOMEWORK 3
#
# Due: Sun 3/17/19 23h59.
#
# Name: Camille Xue and Nate Sampo
#
# Email: cxue@olin.edu, nsampo@olin.edu
#
# Remarks, if any:
#
#
######################################################################


######################################################################
#
# Python 3 code
#
# Please fill in this file with your solutions and submit it
#
# The functions below are stubs that you should replace with your
# own implementation.
#
######################################################################



class Relation:
	def __init__ (self, columns, primary_key, tuples=[]):

	    self._columns = columns
	    self._primary_key = primary_key
	    self._tuples = set(tuples)

	def __repr__ (self):

	    result = "------------------------------------------------------------\n"
	    result += (", ".join(self._columns)) + "\n"
	    result += "------------------------------------------------------------\n"
	    result += "".join([ str(t)+"\n" for t in self._tuples ])
	    result += "------------------------------------------------------------"
	    return result

	def columns (self):

	    return self._columns

	def primary_key (self):

	    return self._primary_key

	def tuples (self):

	    return self._tuples


	########################################
	# LOW-LEVEL CRUD OPERATIONS
	########################################

	def create_tuple (self,tup):

		duplicate = True

		if len(tup) == len(self._columns):

			for primary in self._primary_key:

				temp_index = self._columns.index(primary)
				if not tup[temp_index] in [t[temp_index] for t in self._tuples]:

					self._tuples.add(tup);
					duplicate = False
					break

		if duplicate:
			raise Exception("Duplicate tuple {}".format(tup))


	def read_tuple (self,pkey):

		read = self._tuples

		if len(pkey) == len(self._primary_key):

			for primary in self._primary_key:

				temp_index = self._columns.index(primary)
				read = [t for t in read if t[temp_index] == pkey[self._primary_key.index(primary)]]

		if len(read) != 1:
			raise Exception("Cannot find tuple with key {}".format(pkey))

		return read[0]


	def delete_tuple (self,pkey):

	    self._tuples.remove(self.read_tuple(pkey))




	########################################
	# RELATIONAL ALGEBRA OPERATIONS
	########################################

	def project (self,names):

		new_primary_key = [p for p in names if p in self._primary_key and set(names).issubset(self._primary_key)]
		new_tuples = [tuple(t[i] for i in [self._columns.index(n) for n in names]) for t in self._tuples]

		return Relation(names, new_primary_key, new_tuples)


	def select (self,pred):

		new_tuples = [t for t in self._tuples if pred({a: t[self._columns.index(a)] for a in self._columns})]

		return Relation(self._columns, self._primary_key, new_tuples)


	def union (self,rel):

		if self._columns != rel.columns() or self._primary_key != rel.primary_key():
			raise Exception("Sets do not have the same schema")

		return Relation(self._columns, self._primary_key, self._tuples.union(rel.tuples()))


	def rename (self,rlist):

		new_columns = self._columns[:]
		new_primary_key = self._primary_key[:]

		for rename in rlist:
			for i in range(0, len(new_columns)):
				if new_columns[i] == rename[0]:
					new_columns[i] = rename[1]
					break

			for i in range(0, len(new_primary_key)):
				if new_primary_key[i] == rename[0]:
					new_primary_key[i] = rename[1]
					break


		return Relation(new_columns, new_primary_key, self._tuples)


	def product (self,rel):

		if len(set(self._columns) & set(rel.columns())):
			raise Exception("Relation attributes are not disjoint")

		new_columns = self._columns + rel.columns()
		new_primary_key = self._primary_key + rel.primary_key()
		new_tuples = set()

		for tup1 in self._tuples:
			for tup2 in rel.tuples():
				new_tuples.add(tup1 + tup2)

		return Relation(new_columns, new_primary_key, new_tuples)


	def aggregate (self,aggr):

		new_columns = [c[0] for c in aggr]
		new_tuples = [()]

		for a in aggr:

			number = 0

			if a[1] == "sum":

				for tup in self._tuples:
					number += tup[self._columns.index(a[2])]

				new_tuples[0] += (number,)

			elif a[1] == "count":

				new_tuples[0] += (len(self._tuples),)

			elif a[1] == "avg":

				for tup in self._tuples:
					number += tup[self._columns.index(a[2])]

				new_tuples[0] += (number/len(self._tuples),)

			elif a[1] == "max":

				for tup in self._tuples:
					number = max(number, tup[self._columns.index(a[2])])

				new_tuples[0] += (number,)

			elif a[1] == "min":

				number = float('inf')
				for tup in self._tuples:
					number = min(number, tup[self._columns.index(a[2])])

				new_tuples[0] += (number,)

		return Relation(new_columns, [], new_tuples)


	def cross_join(self, other_table):
		new_columns = self._columns + other_table.columns()
		new_tuples = []

		for tup1 in self._tuples:
			for tup2 in other_table.tuples():
				new_tuples.append(tup1 + tup2)

		return Relation(new_columns, self._primary_key, new_tuples)

	def inner_join(self, other_table, attr):
		if attr not in other_table.columns() or attr not in self._columns:
			return Relation(new_columns, self._primary_key, new_tuples)

		new_columns = self._columns + [c for c in other_table.columns() if c not in self._columns]
		new_tuples = []

		index1 = self._columns.index(attr)
		index2 = other_table.columns().index(attr)

		for tup1 in self._tuples:
			for tup2 in other_table.tuples():
				if tup1[index1] == tup2[index2]:
					tup2_list = list(tup2)
					tup2_list.pop(index2)
					new_tuples.append(tup1 + tuple(tup2_list))

		return Relation(new_columns, self._primary_key, new_tuples)


	def left_outer_join(self, rel, att):
		new_columns = self._columns + rel.columns()
		new_tuples = set()

		if att not in rel.columns():
			for tup in self._tuples:
				new_tuples.add(tup + ("NULL",)*len(rel.columns()))
			return Relation(new_columns, self._primary_key, new_tuples)

		other_columns = [c for c in rel.columns() if c not in self._columns]
		new_columns = self._columns + other_columns

		index1 = self._columns.index(att)
		index2 = rel.columns().index(att)

		for tup1 in self._tuples:
			match = 0
			for tup2 in rel.tuples():
				if(tup1[index1] == tup2[index2]):
					tup2_list = list(tup2)
					tup2_list.pop(index2)
					new_tuples.add(tup1 + tuple(tup2_list))
					match = 1
			if not match:
				new_tuples.add(tup1 + ("NULL",)*len(other_columns))

		return Relation(new_columns, self._primary_key, new_tuples)


	def right_outer_join(self, rel, att):
		new_columns = self._columns + rel.columns()
		new_tuples = set()

		if att not in self._columns:
			for tup in rel.tuples():
				new_tuples.add(("NULL",)*len(self.columns()) + tup)
			return Relation(new_columns, self._primary_key, new_tuples)

		other_columns = [c for c in rel.columns() if c not in self._columns]
		new_columns = self._columns + other_columns

		index1 = self._columns.index(att)
		index2 = rel.columns().index(att)

		for tup2 in rel.tuples():
			match = 0
			for tup1 in self._tuples:
				if(tup1[index1] == tup2[index2]):
					tup2_list = list(tup2)
					tup2_list.pop(index2)
					new_tuples.add(tup1 + tuple(tup2_list))
					match = 1
			if not match:
				new_tup = [tup2[index2] if tup1.index(e) == index1 else "NULL" for e in tup1] + [e for e in tup2 if tup2.index(e) != index2]
				new_tuples.add(tuple(new_tup))

		return Relation(new_columns, self._primary_key, new_tuples)

	def full_outer_join(self, rel, att):
		new_columns = self._columns + [c for c in rel.columns() if c not in self._columns]
		new_tuples = set()

		left = self.left_outer_join(rel, att)
		right = self.right_outer_join(rel, att)

		return left.union(right)


BOOKS = Relation(["title","year","numberPages","isbn"],
                  ["isbn"],
                  [
                      ( "A Distant Mirror", 1972, 677, "0345349571"),
                      ( "The Guns of August", 1962, 511, "034538623X"),
                      ( "Norse Mythology", 2017, 299, "0393356182"),
                      ( "American Gods", 2003, 591, "0060558121"),
                      ( "The Ocean at the End of the Lane", 2013, 181, "0062255655"),
                      ( "Good Omens", 1990, 432, "0060853980"),
                      ( "The American Civil War", 2009, 396, "0307274939"),
                      ( "The First World War", 1999, 500, "0712666451"),
                      ( "The Kidnapping of Edgardo Mortara", 1997, 350, "0679768173"),
                      ( "The Fortress of Solitude", 2003, 509, "0375724886"),
                      ( "The Wall of the Sky, The Wall of the Eye", 1996, 232, "0571205992"),
                      ( "Stories of Your Life and Others", 2002, 281, "1101972120"),
                      ( "The War That Ended Peace", 2014, 739, "0812980660"),
                      ( "Sheaves in Geometry and Logic", 1994, 630, "0387977102"),
                      ( "Categories for the Working Mathematician", 1978, 317, "0387984032"),
                      ( "The Poisonwood Bible", 1998, 560, "0060175400")
                      ])


PERSONS = Relation(["firstName", "lastName", "birthYear"],
                   ["lastName"],
                   [
                       ( "Barbara", "Tuchman", 1912 ),
                       ( "Neil", "Gaiman", 1960 ),
                       ( "Terry", "Pratchett", 1948),
                       ( "John", "Keegan", 1934),
                       ( "Jonathan", "Lethem", 1964),
                       ( "Margaret", "MacMillan", 1943),
                       ( "David", "Kertzer", 1948),
                       ( "Ted", "Chiang", 1967),
                       ( "Saunders", "Mac Lane", 1909),
                       ( "Ieke", "Moerdijk", 1958),
                       ( "Barbara", "Kingsolver", 1955),
					   ( "NATE", "SAMPO", 1998)
                   ])


AUTHORED_BY = Relation(["isbn","lastName"],
                       ["isbn","lastName"],
                       [
                           ( "0345349571", "Tuchman" ),
                           ( "034538623X", "Tuchman" ),
                           ( "0393356182" , "Gaiman" ),
                           ( "0060558121" , "Gaiman" ),
                           ( "0062255655" , "Gaiman" ),
                           ( "0060853980" , "Gaiman" ),
                           ( "0060853980" , "Pratchett" ),
                           ( "0307274939" , "Keegan" ),
                           ( "0712666451" , "Keegan" ),
                           ( "1101972120" , "Chiang" ),
                           ( "0679768173" , "Kertzer" ),
                           ( "0812980660" , "MacMillan" ),
                           ( "0571205992" , "Lethem" ),
                           ( "0375724886" , "Lethem" ),
                           ( "0387977102" , "Mac Lane" ),
                           ( "0387977102" , "Moerdijk" ),
                           ( "0387984032" , "Mac Lane" ),
                           ( "0060175400" , "Kingsolver"),
						   ( "8888888888" , "CAMILLE")
                       ])



def evaluate_query (query):
    new_rels = []
    for r in query["from"]:
        new_columns = []
        for column in r[0].columns():
            new_columns.append((column,r[1]+"."+column))
        new_rels.append(r[0].rename(new_columns))
        # print(new_rel.columns())

    if(len(query["from"]) > 1):
        rel = new_rels[0].product(new_rels[1])
        # print(rel.columns())
    else:
        rel = new_rels[0]

    for w in query["where"]:
        if(w[0] == "n=n"):
            att1 = w[1]
            att2 = w[2]
            rel = rel.select(lambda t : t[att1] == t[att2])
        if(w[0]) == "n=v":
            att = w[1]
            val = w[2]
            rel = rel.select(lambda t : t[att] == val)
        if(w[0]) == "n>v":
            att = w[1]
            val = w[2]
            rel = rel.select(lambda t : t[att] > val)

    rel = rel.project(query["select"])

    print(rel)
    return rel


def evaluate_query_aggr (query):
    new_rels = []
    for r in query["from"]:
        new_columns = []
        for column in r[0].columns():
            new_columns.append((column,r[1]+"."+column))
        new_rels.append(r[0].rename(new_columns))
        # print(new_rel.columns())

    if(len(query["from"]) > 1):
        rel = new_rels[0].product(new_rels[1])
        # print(rel.columns())
    else:
        rel = new_rels[0]

    for w in query["where"]:
        if(w[0] == "n=n"):
            att1 = w[1]
            att2 = w[2]
            rel = rel.select(lambda t : t[att1] == t[att2])
        if(w[0]) == "n=v":
            att = w[1]
            val = w[2]
            rel = rel.select(lambda t : t[att] == val)
        if(w[0]) == "n>v":
            att = w[1]
            val = w[2]
            rel = rel.select(lambda t : t[att] > val)

    aggr_rel = rel.aggregate(query["select-aggr"])
    print(aggr_rel)

    return aggr_rel


# new = evaluate_query({
#   "select":["a.lastName","b.title"],
#   "from": [(BOOKS,"b"),(AUTHORED_BY,"a")],
#   "where": [("n=n","b.isbn","a.isbn"),("n=v","a.lastName","Gaiman")]
# })

cross = PERSONS.cross_join(BOOKS)
# print(cross)

inner = BOOKS.inner_join(AUTHORED_BY, "isbn")
# print(inner)

left1 = PERSONS.left_outer_join(AUTHORED_BY, "lastName")
# print(left1)

left2 = PERSONS.left_outer_join(AUTHORED_BY, "firstName")
# print(left2)

right = PERSONS.right_outer_join(AUTHORED_BY, "lastName")
# print(right)

right2 = PERSONS.right_outer_join(AUTHORED_BY, "isbn")
# print(right2)

full = PERSONS.full_outer_join(AUTHORED_BY, "lastName")
# print(full)
