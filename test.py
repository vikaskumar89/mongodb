from pymongo import Connection
connection = Connection()
db = connection['warehouse8']
print "Database created"
record = {"name":"Naman","major":"CS","age":32}
posts = db.posts
ids = posts.insert(record)
print ids
