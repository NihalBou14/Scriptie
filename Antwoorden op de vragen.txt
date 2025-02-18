from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired
import pandas as pd
import os
import sqlite3
import sys
import time
    

def check_query(args):
    conn = sqlite3.connect('shopping.db',timeout=1.0)

    query, i, queryNumber, CORRECT_RESULTS = args
    message = ""
    
    try:
        start = time.process_time()
        answer = pd.read_sql_query(query, conn)
        end = time.process_time()
        processtime = end-start
        correct = CORRECT_RESULTS.get(queryNumber)
        score = 0 
        for item in correct:
            if answer.equals(item):
                score = 2
        return [score, message, processtime]
    except TypeError as e:
        #print(f"This was probably a testing query (undefined queryname {queryNumber})")
        return [1, message, 20]
    except Exception as e:
        #print(i, e)
        return [1, str(e), 20]

def calculateCorrectness(queryList, queryNumberList):
    with ProcessPool() as pool:
        future = pool.map(check_query, [(query, i, queryNumberList[i], CORRECT_RESULTS) for i, query in enumerate(queryList)], timeout=TIMEOUT)

        iterator = future.result()
        result = []
        error = []
        processtime = []
        while True:
            try:
                item = next(iterator)
                result.append(item[0])
                error.append(item[1])
                processtime.append(item[2])
            except StopIteration:
                break
            except TimeoutError as e:
                print("Query took longer than %d seconds" % e.args[1])
                result.append(3)
                error.append("timeout")
                processtime.append(20)
            except Exception as e:
                print("Some other error: ", e.args)
                result.append(1)
                error.append(str(e))
                processtime.append(20)
    return result, error, processtime

if __name__ == '__main__':
    os.remove("shopping.db")
    conn = sqlite3.connect('shopping.db',timeout=1.0)
    cur = conn.cursor()

    db_location = 'shoppingDB.sql'

    with open(os.path.join(sys.path[0], db_location), "r") as f:
        sql = f.read()
        cur.executescript(sql)

    TIMEOUT = 5
    CORRECT_RESULTS = {
    'query3_3': #Select all different names and ids of the customers who never made a purchase at a store with the name ‘Kumar’. 
        # Also include the customers who never made a purchase, but are still represented in the customer relation of our database.
       [pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Kumar'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Coop'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Hoogvliet'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Jumbo'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Sligro'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Albert Hein'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Lidl'
                                )
                            ''', conn),
        pd.read_sql_query('''SELECT DISTINCT c.cName, c.cID
                            FROM customer c
                            WHERE c.cID NOT IN (
                                SELECT p.cID
                                FROM purchase p, store s
                                WHERE p.sID=s.sID AND s.sName='Dirk'
                                )
                            ''', conn)

                ],
    'query3_4': #Select all different names and ids of customers who made at least one purchase at a store with the name ‘Kumar’ and never a purchase at a store with a different name than ‘Kumar’.        
        [pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Kumar' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Kumar'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Jumbo' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Jumbo'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Dirk' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Dirk'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Hoogvliet' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Hoogvliet'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Albert Hein' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Albert Hein'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Sligro' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Sligro'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Coop' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Coop'
                            )
                            ''', conn),
         pd.read_sql_query('''SELECT DISTINCT c1.cName, c1.cID
                            FROM purchase p1, store s1, customer c1
                            WHERE p1.sID=s1.sID AND s1.sName='Lidl' AND p1.cID=c1.cID AND c1.cID NOT IN (
                                SELECT p2.cID
                                FROM purchase p2, store s2
                                WHERE p2.sID=s2.sID AND s2.sName<>'Lidl'
                            )
                            ''', conn)
                ],
    'query3_2': #Select all distinct combinations of names and ids of customers who have both a shoppinglist and a purchase, both for the same date in 2018.
        [pd.read_sql_query('''SELECT DISTINCT c.cID, c.cName
                            FROM customer as c, shoppinglist as sl, purchase as p
                            WHERE c.cID = sl.cID
                            AND p.cID = c.cID
                            and sl.date = p.date
                            and sl.date LIKE'%2018%';
                            ''', conn)
                ],
    'query4_3': #Write a SQL query for the following RA query
        [pd.read_sql_query('''select sName
                            from store
                            except
                            select sName
                            from (
                               select s.sName, c.city
                                from store as s, (
                                    select city from customer
                                    union
                                    select city from store) as c
                                except
                                select sName, city from store) as a;
                            ''', conn)     
                ],
    'query4_4': #List the names of all of those customers, and only of those customers, 
        #that spent an amount of money on any one date that is at least 75% of the maximal amount of money ever spent by a single customer on a single day.
        [pd.read_sql_query('''with ods(cID, date, amount) as (   
                                    select cID, date, sum(quantity * price)   
                                    from purchase   
                                    group by cID, date),
                                mods(amount) as (   
                                    select max(amount)
                                    from ods)
                                select cName from customer, ods, mods
                                where customer.cID = ods.cID and     
                                ods.amount >= 0.75 * mods.amount;
                            ''', conn)
                ],
    'query4_5':
        [pd.read_sql_query('''select x.city as city, sum(x.numCustomersPerCity) as numCustomersPerCity
                            from (
                            select y.city, count(*) as numCustomersPerCity
                            from (
                                select distinct c.city, c.cid
                                from customer as c, store as s, purchase as p
                                where c.cid = p.cid and s.sid = p.sid and s.city = "Eindhoven"
                            ) as y
                            group by y.city
                            union
                            select city, 0 as numCustomersPerCity
                            from store
                            union
                            select city, 0 as numCustomersPerCity
                            from customer
                            ) as x
                            group by x.city;''', conn)]
    }

    filename = "output.csv"
    data = pd.read_csv(filename, sep='─', header=0)

    correctness = calculateCorrectness(data['query'].tolist(), data['question'].tolist())
    data['correct'] = correctness[0]
    data['errormessage'] = correctness[1]
    data['runningtime'] = correctness[2]
    data.head(20)

    data.to_csv("output_correct.csv", sep='─', index=False, header=True)



