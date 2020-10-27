import sqlite3
import re
from collections import Counter
DBNAME = 'P:/src/shakespeare/plays.db'
conn = sqlite3.connect(DBNAME)
c = conn.cursor()
#rows = c.execute('''SELECT id, text from lines where is_stagedir=0''')
#f = open('P:/src/shakespeare/plays.txt','wt')
#for row in rows:
#    f.write(str(row[0])+'|'+row[1]+'\n')
#f.close()
f = open('P:/src/shakespeare/plays.txt','r')
fulltext = []
lineindex = {}
for line in f:
    try:
        split = line.split('|')
        fulltext.append(split[1].lower())
        lineindex[split[0]] = split[1]
    except IndexError:
        print(line)
s = '\n'.join(fulltext)
print(len(s))
words=Counter(re.findall(r"(\w[\w']*)",s))
print(len(words))
n = 1000
unique_words = []
for word, count in words.items():
    if count == 1:
        unique_words.append(word)
print(len(unique_words))
for word in unique_words:
    row = c.execute('''
                    SELECT L.id, L.text, L.speaker, 
                            P.title, A.act_num
                    FROM lines as L
                    JOIN plays as P, acts as A
                    ON L.play_id = P.id 
                        AND L.act_id = A.id 
                    WHERE L.text LIKE ?
                    ''',('%'+word+'%',)).fetchone()
    s = '{}|{}|{}|{}|Act {}'.format(word, row[1], row[2], row[3], row[4])
    print(s)
    
