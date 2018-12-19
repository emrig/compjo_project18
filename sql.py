import psycopg2
from psycopg2 import sql

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=pass")
cur = conn.cursor()

"""
    This script stores and retrieves entities from SQL.
    TODO:   Resolve entities (updated DB)
            Find some sort of count relationship
            FUZZY MATCHING - How do we rank the most important matches?
            Different entity types could have the same name
            Word cloud for entity relationships? - DO this in SQL
"""

def create_tables():
    queries = []
    queries.append(f"CREATE TABLE Entities (name varchar PRIMARY KEY, type varchar(20) NOT NULL)")
    queries.append(f"CREATE TABLE Documents (doc_id varchar PRIMARY KEY, tokens text, tsv tsvector)")
    queries.append("""
        CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
        ON Documents FOR EACH ROW EXECUTE PROCEDURE
        tsvector_update_trigger(tsv, 'pg_catalog.english', doc_id, tokens)""")
    queries.append(f"CREATE TABLE FoundIn (id serial PRIMARY KEY, name varchar NOT NULL, doc_id varchar NOT NULL, count integer,\
                        FOREIGN KEY (name) REFERENCES Entities(name) ON DELETE CASCADE, \
                        FOREIGN KEY (doc_id) REFERENCES Documents(doc_id) ON DELETE CASCADE)")
    for query in queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            cur.execute('rollback;')

def clear_tables():
    queries = []
    queries.append(f"DELETE FROM ONLY foundin")
    queries.append(f"DELETE FROM ONLY entities")
    queries.append(f"DELETE FROM ONLY documents)")
    for query in queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            cur.execute('rollback;')

def drop_tables():
    queries = []
    queries.append(f"DROP TABLE foundin")
    queries.append(f"DROP TABLE entities")
    queries.append(f"DROP TABLE documents)")
    for query in queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            cur.execute('rollback;')

def insert_entity(name, type):
    try:
        cur.execute("INSERT INTO entities (name, type) VALUES (%s, %s)", (name, type))
        conn.commit()
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')

def insert_doc(doc_id, text):
    try:
        cur.execute("INSERT INTO documents (doc_id, tokens) VALUES (%s, %s)", (doc_id, text))
        conn.commit()
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')

def insert_foundin(name, doc_id, count):
    try:
        cur.execute("INSERT INTO foundin (name, doc_id, count) VALUES (%s, %s, %s)", (name, doc_id, count))
        conn.commit()
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')

"""
    Retrieve all entities that are found in the same documents as a given entity
"""
def get_related_entities(name, type=None):
    try:
        cur.execute(sql.SQL("""select c.type, b.name, count(b.name)
        from (select doc_id from {0} 
        where name = %s) as a, {0} as b, {1} as c
        where a.doc_id = b.doc_id and b.name = c.name and b.name != %s
        group by b.name, c.type 
        order by count desc""").format(sql.Identifier('foundin'), sql.Identifier('entities')), [name, name])
        result = cur.fetchall()
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return
    if type:
        return [x for x in result if x[0] == type]
    else:
        return result

"""
    Merges entities when user presented with potential matches and proves a resolution
"""
def merge_entities(name_1, name_2):
    try:
        cur.execute("""
        UPDATE foundin
        SET    name = %s 
        WHERE  name = %s
        """, [name_1, name_2])
        cur.execute("""
        DELETE FROM entities CASCADE
        WHERE name = %s
        """, [name_2])
        conn.commit()
        return True
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return False

def delete_entity(name):
    try:
        cur.execute("""
        DELETE FROM entities CASCADE
        WHERE name = %s
        """, [name])
        conn.commit()
        return True
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return False

def get_entities():
    try:
        cur.execute("""
        SELECT name FROM entities
        """)
        return cur.fetchall()
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return False

"""
    Gets the total document frequency for a group of entities. Used to rank potential matches.
"""
def get_group_counts(names):
    try:
        cur.execute("""
            SELECT count(*) FROM foundin
            WHERE name = any (%s)
            """, [names])
        return cur.fetchall()[0][0]
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return False

"""
    Merge and entire group of entities using a given name
"""
def merge_entity_group(new_name, group, type):
    group = [x for x in group if x != new_name]
    try:
        cur.execute("""
            SELECT count(*) FROM entities
            WHERE name = %s
            """, [new_name])
        if len(cur.fetchall()) == 0:
            insert_entity(new_name, type)
        cur.execute("""
            UPDATE foundin
            SET name = %s 
            WHERE name = any (%s)
            """, [new_name, group])
        cur.execute("""
            DELETE FROM entities CASCADE
            WHERE name = any (%s)
            """, [group])
        conn.commit()
        return True
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return False

"""
    Gets all of the words in the documents that a group of entities share.
"""
def get_common_words(names):
    try:
        cur.execute(sql.SQL("""select b.tokens
        from (select doc_id from {0} 
        where name = any (%s)) as a, {1} as b
        where a.doc_id = b.doc_id""").format(sql.Identifier('foundin'), sql.Identifier('documents')), [names])
        return cur.fetchall()
    except Exception as err:
        print(err.args)
        cur.execute('rollback;')
        return False
