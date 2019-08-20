import sqlalchemy as db
from numpy import genfromtxt
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from time import time
import pandas as pd

def load_data(file_name):
    #data = genfromtxt(file_name, delimiter=',', converters={0: lambda s: str(s)})
    data = genfromtxt(file_name, delimiter=',', dtype='|U15')
    return data.tolist()


Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    nickname = Column(String)

    def __repr__(self):
        return "<User(name='%s', fullname='%s', nickname='%s')>" % (
                      self.name, self.fullname, self.nickname)


if __name__ == "__main__":
    t = time()

#Create the database
engine = create_engine('sqlite:///:memory:', echo=True)
Base.metadata.create_all(engine)
connection = engine.connect()

#Create the session
session = sessionmaker()
session.configure(bind=engine)
s = session()

try:
    file_name = "input_data.txt"
    data = load_data(file_name)

    for i in data:
        record = User(**{
            'id': i[0],
            'name': i[1],
            'fullname': i[2],
            'nickname': i[3]
        })
        s.add(record)  # Add all the records
        print(record)

    s.commit()  # Attempt to commit all the records
    query = db.select([User.nickname]).where(User.id.in_([100]))
    result = connection.execute(query).fetchall()
    df = pd.DataFrame(result)
    df.columns = result[0].keys()
    print(df)
except:
    s.rollback()  # Rollback the changes on error
finally:
    s.close()  # Close the connection
print("Time elapsed: " + str(time() - t) + " s.")