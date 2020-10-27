
# coding: utf-8


import os
import re
import urllib.request
import xml.etree.ElementTree
import xml.dom.minidom
import time
import asyncio
import sqlite3
import aiosqlite
import aiohttp
import aiofiles
import nest_asyncio
from typing import Dict, Text, List, Match, Any, Coroutine
nest_asyncio.apply()


MOBY_INDEX = [
    ("All's Well That Ends Well",
        'http://www.ibiblio.org/xml/examples/shakespeare/all_well.xml'),
    ("As You Like It",
        'http://www.ibiblio.org/xml/examples/shakespeare/as_you.xml'),
    ("Antony and Cleopatra",
        'http://www.ibiblio.org/xml/examples/shakespeare/a_and_c.xml'),
    ("A Comedy of Errors",
        'http://www.ibiblio.org/xml/examples/shakespeare/com_err.xml'),
    ("Coriolanus",
        'http://www.ibiblio.org/xml/examples/shakespeare/coriolan.xml'),
    ("Cymbeline",
        'http://www.ibiblio.org/xml/examples/shakespeare/cymbelin.xml'),
    ("A Midsummer Night's Dream",
        'http://www.ibiblio.org/xml/examples/shakespeare/dream.xml'),
    ("Hamlet",
        'http://www.ibiblio.org/xml/examples/shakespeare/hamlet.xml'),
    ("Henry IV, Part I",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_iv_1.xml'),
    ("Henry IV, Part II",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_iv_2.xml'),
    ("Henry V",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_v.xml'),
    ("Henry VIII",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_viii.xml'),
    ("Henry VI, Part 1",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_vi_1.xml'),
    ("Henry VI, Part 2",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_vi_2.xml'),
    ("Henry VI, Part 3",
        'http://www.ibiblio.org/xml/examples/shakespeare/hen_vi_3.xml'),
    ("The Life and Death of King John",
        'http://www.ibiblio.org/xml/examples/shakespeare/john.xml'),
    ("Julius Caesar",
        'http://www.ibiblio.org/xml/examples/shakespeare/j_caesar.xml'),
    ("King Lear",
        'http://www.ibiblio.org/xml/examples/shakespeare/lear.xml'),
    ("Love's Labor's Lost",
        'http://www.ibiblio.org/xml/examples/shakespeare/lll.xml'),
    ("Macbeth",
        'http://www.ibiblio.org/xml/examples/shakespeare/macbeth.xml'),
    ("The Merchant of Venice",
        'http://www.ibiblio.org/xml/examples/shakespeare/merchant.xml'),
    ("Much Ado About Nothing",
        'http://www.ibiblio.org/xml/examples/shakespeare/much_ado.xml'),
    ("Measure for Measure",
        'http://www.ibiblio.org/xml/examples/shakespeare/m_for_m.xml'),
    ("The Merry Wives of Windsor",
        'http://www.ibiblio.org/xml/examples/shakespeare/m_wives.xml'),
    ("Othello",
        'http://www.ibiblio.org/xml/examples/shakespeare/othello.xml'),
    ("Pericles",
        'http://www.ibiblio.org/xml/examples/shakespeare/pericles.xml'),
    ("Richard II",
        'http://www.ibiblio.org/xml/examples/shakespeare/rich_ii.xml'),
    ("Richard III",
        'http://www.ibiblio.org/xml/examples/shakespeare/rich_iii.xml'),
    ("Romeo and Juliet",
        'http://www.ibiblio.org/xml/examples/shakespeare/r_and_j.xml'),
    ("The Taming of the Shrew",
        'http://www.ibiblio.org/xml/examples/shakespeare/taming.xml'),
    ("The Tempest",
        'http://www.ibiblio.org/xml/examples/shakespeare/tempest.xml'),
    ("Timon of Athens",
        'http://www.ibiblio.org/xml/examples/shakespeare/timon.xml'),
    ("Titus Andronicus",
        'http://www.ibiblio.org/xml/examples/shakespeare/titus.xml'),
    ("Troilus and Cressida",
        'http://www.ibiblio.org/xml/examples/shakespeare/troilus.xml'),
    ("Two Gentlemen of Verona",
        'http://www.ibiblio.org/xml/examples/shakespeare/two_gent.xml'),
    ("Twelfth Night",
        'http://www.ibiblio.org/xml/examples/shakespeare/t_night.xml'),
    ("A Winter's Tale",
        'http://www.ibiblio.org/xml/examples/shakespeare/win_tale.xml'),
    ]

TEST_MOBY_INDEX = [
    ("All's Well That Ends Well",
        'http://www.ibiblio.org/xml/examples/shakespeare/all_well.xml'),
    ("As You Like It",
        'http://www.ibiblio.org/xml/examples/shakespeare/as_you.xml') ]



DBFILE = "./shakedb.sqlite"

async def create_tables(conn: sqlite3.Connection) -> Coroutine:
    # Now create the tables
    tables: List[Text] = ['''CREATE TABLE IF NOT EXISTS plays
                        (id        INTEGER PRIMARY KEY,
                        title     TEXT,
                        subtitle  TEXT,
                        scene     TEXT
                        );''', 
                  '''CREATE TABLE IF NOT EXISTS acts
                         (id        INTEGER PRIMARY KEY,
                          play_id   INTEGER,
                          act_num   INTEGER,
                          title     TEXT
                  );''', 
                    '''CREATE TABLE IF NOT EXISTS scenes
                         (id        INTEGER PRIMARY KEY,
                          play_id   INTEGER,
                          act_id    INTEGER,
                          scene_num INTEGER,
                          title     TEXT
                    );''',
                    '''CREATE TABLE IF NOT EXISTS actor_lines
                         (id        INTEGER PRIMARY KEY,
                          play_id   INTEGER,
                          act_id    INTEGER,
                          scene_id  INTEGER,
                          speaker   TEXT,
                          is_stagedir INTEGER,
                          the_text  TEXT
                          );''',
                    '''CREATE TABLE IF NOT EXISTS person
                         (id        INTEGER PRIMARY KEY,
                          play_id   INTEGER,
                          name      TEXT,
                          description TEXT,
                          group_name TEXT
                          );'''
                 ]
    tasks = [conn.execute(sql) for sql in tables]
    await asyncio.gather(*tasks)
    await conn.commit()

def _get_text(nodelist: List) -> Text:
    rc: List = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)        


def handle_title(title):
    return _get_text(title.childNodes)

def handle_personae_group(group):
    people = []
    for element in group.childNodes:
        if isinstance(element, xml.dom.minidom.Text):
            continue
        if element.tagName == 'GRPDESCR':
            group_name = _get_text(element.childNodes)
        else:
            people.append(_get_text(element.childNodes))
    people_with_group = [(group_name, person) for person in people]
    return (group_name, people_with_group)

def handle_personae(personae):
    result = {}
    result['persona'] = []
    result['persona_group'] = []
    for element in personae.childNodes:
        if isinstance(element, xml.dom.minidom.Text):
            continue
        if element.tagName == 'TITLE':
            continue
        if element.tagName == "PERSONA":
            result['persona'].append(('',_get_text(element.childNodes)))
        else:
            group = handle_personae_group(element)
            result['persona_group'].append(group[0])
            result['persona'] += group[1]
    return result

def handle_stage_dir(stagedir):
    return _get_text(stagedir.childNodes)

def handle_speech(speech):
    result = {}
    result['lines'] = []
    for line in speech.childNodes:
        if isinstance(line, xml.dom.minidom.Text):
            continue
        if line.tagName == "SPEAKER":
            result['speaker'] = _get_text(line.childNodes)
        else:
            result['lines'].append(_get_text(line.childNodes))
    return result

def handle_scene(scene):
    result = {}
    result['title'] = handle_title(scene.getElementsByTagName('TITLE')[0])
    result['content'] = []
    for element in scene.childNodes:
        if isinstance(element,xml.dom.minidom.Text) or element.tagName == "TITLE":
            continue
        if element.tagName == "STAGEDIR":
            result['content'].append((0,handle_stage_dir(element)))
        else:
            result['content'].append((1,handle_speech(element)))
    return result

def handle_act(act):
    result: Dict[Text, Text] = {}
    result['title'] = handle_title(act.getElementsByTagName('TITLE')[0])
    scenes = act.getElementsByTagName('SCENE')
    result['scenes'] = []
    for scene in scenes:
        result['scenes'].append(handle_scene(scene))
    return result

class Play():
    
    def __init__(self, title: Text, url: Text) -> None:
        self.title: Text = title
        self.url: Text = url
    
    async def store(self, play: Dict[Text, Dict], conn: aiosqlite.Connection):
        print(play['title'])
        
        cursor: aiosqlite.Cursor = await conn.execute('''
                    INSERT INTO plays
                    (title, subtitle, scene)
                    VALUES (?,?,?)''',
                    (play['title'], play['subtitle'], play['scene_description']))
        play_id: int = cursor.lastrowid
        # create the groups
        person_ids = {}
        for person in play['personae']['persona']:
            # split the name
            matches: List[Text] = re.split(r',', person[1])
            match: Match[Any] = re.match(r'[A-z ]*', matches[0])
            name = match.group(0).strip(' .')
            if len(matches) == 1:
                persona = (name,'')
            elif len(matches) == 2:
                persona = (name, re.match(r'[A-z ]*',matches[1]).group(0).strip(' .'))
            else:
                persona = (person[1],'')
            cursor = await conn.execute('''
                INSERT INTO person
                (play_id, name, description,group_name)
                VALUES (?,?,?,?)''',
                (play_id, persona[0], persona[1], person[0]))
            person_ids[persona[0]] = cursor.lastrowid
        act_num = 1
        for act in play['acts']:
            print(play['title'], act['title'])
            cursor = await conn.execute('''
                INSERT INTO acts
                (title, play_id, act_num)
                VALUES (?,?,?)''',
                (act['title'],play_id,act_num))
            act_id = cursor.lastrowid
            scene_num = 1
            for scene in act['scenes']:
                cursor = await conn.execute('''
                    INSERT INTO scenes
                    (play_id, act_id, title, scene_num)
                    VALUES (?,?,?,?)''',
                    (play_id, act_id, scene['title'], scene_num))
                scene_id = cursor.lastrowid
                # and finally write the lines
                for content in scene['content']:
                    if content[0] == 0:
                        # This is stage direction
                        await conn.execute('''
                        INSERT INTO actor_lines
                        (play_id, act_id, scene_id, speaker,
                        is_stagedir, the_text)
                        VALUES (?,?,?,?,?,?)''',
                        (play_id, act_id, scene_id, '',
                            1, content[1]))
                    else:
                        try:
                            speaker = content[1]['speaker']
                            for line in content[1]['lines']:
                                cursor = await conn.execute('''
                                    INSERT INTO actor_lines
                                    (play_id, act_id, scene_id, speaker,
                                    is_stagedir, the_text)
                                    VALUES (?,?,?,?,?,?)''',
                                    (play_id, act_id, scene_id, speaker,
                                    0, line))
    
                        except KeyError:
                            print(content)
                # Increment scene_num
                scene_num = scene_num + 1
            # Increment act num
            act_num = act_num + 1
        # Commit this play.
        await conn.commit()

    async def fetch_and_store(self, session: aiohttp.ClientSession, conn: aiosqlite.Connection) -> Coroutine:
        async with session.get(self.url) as response:
            play_text = await response.text()
            play = xml.dom.minidom.parseString(play_text)
            result: Dict = {}
            result['title'] = handle_title(play.getElementsByTagName('TITLE')[0])              
            result['subtitle'] = handle_title(play.getElementsByTagName('PLAYSUBT')[0])
            result['scene_description'] = handle_title(play.getElementsByTagName('SCNDESCR')[0])
            result['personae'] = handle_personae(play.getElementsByTagName('PERSONAE')[0])
            result['acts'] = []
            acts = play.getElementsByTagName('ACT')
            for act in acts:
                result['acts'].append(handle_act(act))
            await self.store(result, conn)

async def create_database() -> Coroutine:
    try:
        os.remove(DBFILE)
    except FileNotFoundError:
        pass
    async with aiosqlite.connect(DBFILE) as dbconn:
        print('Creating tables')
        await create_tables(dbconn)
        async with aiohttp.ClientSession() as session:
            plays: List[Play] = [Play(play[0], play[1]) for play in MOBY_INDEX]
            tasks: List[Task] = [play.fetch_and_store(session, dbconn) for play in plays]
            await asyncio.gather(*tasks)

async def get_stuff(like: Text) -> Coroutine:
    async with aiosqlite.connect(DBFILE) as dbconn:
        dbconn.row_factory = aiosqlite.Row
        async with dbconn.execute('''SELECT 
                                actor_lines.the_text, actor_lines.speaker, plays.title 
                            FROM 
                                actor_lines, plays 
                            WHERE
                                plays.id = actor_lines.play_id AND
                                actor_lines.the_text LIKE ?
                            ''', (like,)) as cursor:
            async for row in cursor:
                x = 0
                x = x + 1
                print(tuple(row))
                print(x)

async def get_tables() -> Coroutine:
    async with aiosqlite.connect(DBFILE) as dbconn:
        dbconn.row_factory = aiosqlite.Row
        async with dbconn.execute('''SELECT * 
                                    from sqlite_master 
                                    WHERE type='table' ''') as cursor:
            async for row in cursor:
                print(str(tuple(row)).replace(' ',''))

async def main() -> Coroutine:
    await create_database()
    await get_stuff('%piss%')
    await get_tables()

if __name__ == "__main__":
    t = time.time()
    asyncio.run(main())
    print(time.time() - t)
