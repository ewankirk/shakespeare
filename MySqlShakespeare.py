# -*- coding: utf-8 -*-

"""
Obtain and import the Moby/Bosak xml-i-fied versions of shakespeare's plays
available from:

    <http://www.ibiblio.org/xml/examples/shakespeare/>
"""
from __future__ import print_function
import os
import re
import urllib.request
import xml.etree.ElementTree
import xml.dom.minidom
import mysql.connector

FILE_CACHE = "P:/src/databases/cache"
GUT_INDEX = 'http://www.gutenberg.org/dirs/GUTINDEX.ALL'

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

    
 

class Cache(object):
    """Provide a local filesystem cache for material.
    """

    def __init__(self):
        pass

    def path(self, remote_url, version=''):
        """Get local path to text of remote url.
        @type: string giving version of text (''|'cleaned')
        """
        protocolEnd = remote_url.index(':') + 3  # add 3 for ://
        path = remote_url[protocolEnd:]
        base, name = os.path.split(path)
        name = version + name
        offset = os.path.join(base, name)
        localPath = self.path_from_offset(offset)
        return localPath

    def download_url(self, url, overwrite=False):
        """Download a url to the local cache
        @overwrite: if True overwrite an existing local copy otherwise don't
        """
        localPath = self.path(url)
        dirpath = os.path.dirname(localPath)
        if overwrite or not(os.path.exists(localPath)):
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            # use wget as it seems to work more reliably on wikimedia
            # see extensive comments on issue in shakespeare.eb.Wikimedia class
            # rgrp: 2008-03-18 use urllib rather than wget despite these issues
            # as wget is fairly specific to linux/unix and even there may not
            # be installed.
            # cmd = 'wget -O %s %s' % (localPath, url) 
            # os.system(cmd)
            urllib.request.urlretrieve(url, localPath)

    def path_from_offset(self, offset):
        "Get full path of file in cache given by offset."
        return os.path.join(FILE_CACHE, offset)


class DownloadHandler():
    ''' Downloads and creates the disk cache of all the gutenberg
        and moby files for shakespeare's works
    '''
    
    def __init__(self):
        self.cache = Cache()
        self._gutindex_local_path = self.cache.path(GUT_INDEX)
        self.cache.download_url(GUT_INDEX)


    def make_gutenberg_url(self, year, idStr):
        return 'http://www.gutenberg.org/dirs/etext%s/%s10.txt' % (year[2:], idStr)
    
    def get_relevant_works(self):
        """Get list of shakespeare works and urls.

        Results are sorted by work title.

        Notes regarding list of plays:

          * no Folio edition of Troilus and Cressida
          * no Folio edition of Pericles
        """
        # results have format [ title, url, comments ]
        # folio in comments indicates it is a first folio
        results = [ ["Sonnets", 'http://www.gutenberg.org/dirs/etext97/wssnt10.txt', ''] ]
        plays = self._extract_shakespeare_works()
        for play in plays:
            url = self.make_gutenberg_url(play[1], play[2])
            results.append([play[0], url, play[3]])
        # add in by hand some exceptions
        results.append(["The Winter's Tale",
                'http://www.gutenberg.org/files/1539/1539.txt', '']
                )
        def compare_list(item1, item2):
            if item1[0] > item2[0]: return 1
            else: return -1
            
        return sorted(results, key=lambda x: x[0])
    
    def _extract_shakespeare_works(self):
        """Get non-copyrighted Shakespeare works from Gutenberg
        Results consist of folio and one other 'standard' version.
        @return: list consisting of tuples in form [title, year, id, comment]
        """
        ff = open(self._gutindex_local_path, encoding='latin-1')
        results = []
        for line in ff.readlines():
            result = self.parse_line_for_folio(line)
            if result:
                results.append(result + ['folio'])
            resultNormal = self.parse_line_for_normal(line)
            if resultNormal:
                results.append(resultNormal + [''])
        return results
    
    def parse_line_for_normal(self, line):
        """Parse GUTINDEX for 'normal' gutenberg shakespeare versions (i.e. not
        folio and out of copyright).
        """
        # normal shakespeare are those with id starting [2
        # most have 'by William Shakespeare' but also have 'by Shakespeare'
        # (Othello) and 'by Wm Shakespeare' (Titus Andronicus)
        # everything is by William Shakespeare except for Othello
        if ('Shakespeare' in line and '[2' in line
                and 'mp3' not in line and 'Apocrypha' not in line):
            year = line[4:8]
            tmp = line[9:]
            endOfTitle = tmp.find(', by')
            title = tmp[:endOfTitle]
            startOfId = tmp.find('[2')
            endOfId = tmp.find(']', startOfId)
            idStr = tmp[startOfId+1:endOfId]
            xstart = idStr.find('x')
            idStr = idStr[:xstart]
            return [title, year, idStr]
        
    def parse_line_for_folio(self, line):
        if '[FF]' in line:
            year = line[4:8]
            tmp = line[9:]
            endOfTitle = tmp.find(', by')
            title = tmp[:endOfTitle]
            startOfId = tmp.find('[FF]') + 5
            endOfId = tmp.find(']', startOfId)
            idStr = tmp[startOfId+1:endOfId]
            xstart = idStr.find('x')
            idStr = idStr[:xstart]
            return [title, year, idStr]
        else:
            return None

    def clean_gutenberg(self, line=None):
        '''See parent class.
        '''
        textsToProcess = self._filter_index(line) 
        for item in textsToProcess:
            url = item[1]
            src = self.cache.path(url)
            dest = self.cache.path(url, 'plain')
            if os.path.exists(dest):
                if self.verbose:
                    print('Skip clean of %s as clean version already exists' % src)
                continue
            if self.verbose:
                print('Formatting %s to %s' % (src, dest))
            infile = file(src)
            if src.endswith('wssnt10.txt'): # if it is the sonnets need a hack
                # delete last 140 characters
                tmp1 = infile.read()
                infile = StringIO.StringIO(tmp1[:-120])
            formatter = shakespeare.gutenberg.GutenbergCleaner(infile)
            ff = file(dest, 'w')
            out = formatter.extract_text()
            ff.write(out)
            ff.close()

    def add_to_db_gutenberg(self):
        """Add all gutenberg texts to the db list of texts.
        
        If a text already exists in the db it will be skipped.
        """
        for text in self._index:
            title = text[0]
            name = self.title_to_name(title) + '_gut'
            url = text[1]
            notes = 'Sourced from Project Gutenberg (url=%s). %s' % (text[1],
                    text[2])
            if text[2] == 'folio':
                name += '_f'
            
            numExistingTexts = shakespeare.model.Material.query.filter_by(
                        name=name).count()
            if numExistingTexts > 0:
                if self.verbose:
                    print('Skip: Add to db. Gutenberg text already exists with name: %s' % name)
            else:
                if self.verbose:
                    print('Add to db. Gutenberg text named [%s]' % name)
                shakespeare.model.Material(name=name,
                                        title=title,
                                        creator='Shakespeare, William',
                                        url=url,
                                        notes=notes)


    def _get_text(self, nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)        
        
    def handle_title(self,title):
        return self._get_text(title.childNodes)

    def handle_personae_group(self,group):
        people = []
        for element in group.childNodes:
            if isinstance(element, xml.dom.minidom.Text):
                continue
            if element.tagName == 'GRPDESCR':
                group_name = self._get_text(element.childNodes)
            else:
                people.append(self._get_text(element.childNodes))
        people_with_group = [(group_name, person) for person in people]
        return (group_name, people_with_group)
        
    def handle_personae(self,personae):
        result = {}
        result['persona'] = []
        result['persona_group'] = []
        current_group = ''
        for element in personae.childNodes:
            if isinstance(element, xml.dom.minidom.Text):
                continue
            if element.tagName == 'TITLE':
                continue
            if element.tagName == "PERSONA":
                result['persona'].append(('',self._get_text(element.childNodes)))
            else:
                group = self.handle_personae_group(element)
                result['persona_group'].append(group[0])
                result['persona'] += group[1]
        return result
        
    def handle_stage_dir(self,stagedir):
        return self._get_text(stagedir.childNodes)
        
    def handle_speech(self, speech):
        result = {}
        result['lines'] = []
        for line in speech.childNodes:
            if isinstance(line, xml.dom.minidom.Text):
                continue
            if line.tagName == "SPEAKER":
                result['speaker'] = self._get_text(line.childNodes)
            else:
                result['lines'].append(self._get_text(line.childNodes))
        return result
        
    def handle_scene(self, scene):
        result = {}
        result['title'] = self.handle_title(scene.getElementsByTagName('TITLE')[0])
        result['content'] = []
        for element in scene.childNodes:
            if isinstance(element,xml.dom.minidom.Text) or element.tagName == "TITLE":
                continue
            if element.tagName == "STAGEDIR":
                result['content'].append((0,self.handle_stage_dir(element)))
            else:
                result['content'].append((1,self.handle_speech(element)))
        return result
        
    def handle_act(self, act):
        result = {}
        result['title'] = self.handle_title(act.getElementsByTagName('TITLE')[0])
        scenes = act.getElementsByTagName('SCENE')
        result['scenes'] = []
        for scene in scenes:
            result['scenes'].append(self.handle_scene(scene))
        return result

    def handle_play(self, play):
        result = {}
        result['title'] = self.handle_title(play.getElementsByTagName('TITLE')[0])              
        result['subtitle'] = self.handle_title(play.getElementsByTagName('PLAYSUBT')[0])
        result['scene_description'] = self.handle_title(play.getElementsByTagName('SCNDESCR')[0])
        result['personae'] = self.handle_personae(play.getElementsByTagName('PERSONAE')[0])
        result['acts'] = []
        acts = play.getElementsByTagName('ACT')
        for act in acts:
            result['acts'].append(self.handle_act(act))
        return result
    
    def get_moby_works(self):
        self.plays = []
        for item in MOBY_INDEX:
            self.cache.download_url(item[1])
            dom = xml.dom.minidom.parse(self.cache.path(item[1]))
            play = self.handle_play(dom)
            print(play['title'])
            self.plays.append(play)


    def create_mysql(self):
        conn = mysql.connector.connect(user='pi', password='raspberry',host='192.168.1.200',
                                       database='shakespeare')
        c = conn.cursor()
        # Now create the tables
        c.execute('''CREATE TABLE IF NOT EXISTS plays
                        (id        INTEGER PRIMARY KEY AUTO_INCREMENT,
                        title     TEXT,
                        subtitle  TEXT,
                        scene     TEXT
                        );''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS acts
                 (id        INTEGER PRIMARY KEY AUTO_INCREMENT,
                  play_id   INTEGER,
                  act_num   INTEGER,
                  title     TEXT
                  );''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS scenes
                 (id        INTEGER PRIMARY KEY AUTO_INCREMENT,
                  play_id   INTEGER,
                  act_id    INTEGER,
                  scene_num INTEGER,
                  title     TEXT
                  );''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS actor_lines
                 (id        INTEGER PRIMARY KEY AUTO_INCREMENT,
                  play_id   INTEGER,
                  act_id    INTEGER,
                  scene_id  INTEGER,
                  is_stagedir INTEGER,
                  the_text  TEXT,
                  speaker   TEXT
                  );''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS word_to_line
                  (word_id  INTEGER,
                   line_id  INTEGER,
                   line_pos INTEGER
                   );''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS words
                  (id       INTEGER PRIMARY KEY AUTO_INCREMENT,
                   word     TEXT
                   );''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS person
                 (id        INTEGER PRIMARY KEY AUTO_INCREMENT,
                  play_id   INTEGER,
                  name      TEXT,
                  description TEXT,
                  group_name TEXT
                  );''')

        conn.commit()

    def write_to_mysql(self):
        self.create_mysql()
        conn = mysql.connector.connect(user='pi', password='raspberry',host='192.168.1.200',
                                       database='shakespeare')
        c = conn.cursor()
        for play in self.plays:
            print(play['title'])
            c.execute('''
                    INSERT INTO plays
                    (title, subtitle, scene)
                    VALUES (%s,%s,%s)''',
                    (play['title'], play['subtitle'], play['scene_description']))
            play_id = c.lastrowid
            # create the groups
            for person in play['personae']['persona']:
                # split the name
                matches = re.split(r',', person[1])
                #matches = re.split(r'([A-Z]{2}[A-Z ]*,{0,1})(.*)', person[1])
                name = re.match(r'[A-z ]*', matches[0]).group(0).strip(' .')
                if len(matches) == 1:
                    persona = (name,'')
                elif len(matches) == 2:
                    persona = (name, re.match(r'[A-z ]*',matches[1]).group(0).strip(' .'))
                else:
                    persona = (person[1],'')
                c.execute('''
                    INSERT INTO person
                    (play_id, name, description,group_name)
                    VALUES (%s,%s,%s,%s)''',
                    (play_id, persona[0], persona[1], person[0]))

            act_num = 1
            for act in play['acts']:
                print('act ', act['title'])
                c.execute('''
                    INSERT INTO acts
                    (title, play_id, act_num)
                    VALUES (%s,%s,%s)''',
                    (act['title'],play_id,act_num))
                act_id = c.lastrowid
                scene_num = 1
                for scene in act['scenes']:
                    c.execute('''
                        INSERT INTO scenes
                        (play_id, act_id, title, scene_num)
                        VALUES (%s,%s,%s,%s)''',
                        (play_id, act_id, scene['title'], scene_num))
                    scene_id = c.lastrowid
                    # and finally write the lines
                    for content in scene['content']:
                        if content[0] == 0:
                            # This is stage direction
                            c.execute('''
                              INSERT INTO actor_lines
                              (play_id, act_id, scene_id,
                               is_stagedir, the_text, speaker)
                               VALUES (%s,%s,%s,%s,%s,%s)''',
                               (play_id, act_id, scene_id,
                                1, content[1], 'None'))
                        else:
                            try:
                                speaker = content[1]['speaker']
                            except KeyError:
                                print(content)
                            for line in content[1]['lines']:
                                c.execute('''
                                    INSERT INTO actor_lines
                                    (play_id, act_id, scene_id,
                                     is_stagedir, the_text, speaker)
                                     VALUES (%s,%s,%s,%s,%s,%s)''',
                                    (play_id, act_id, scene_id,
                                     0, line, speaker))
                                line_id = c.lastrowid

                    # Increment scene_num
                    scene_num = scene_num + 1
                # Increment act num
                act_num = act_num + 1
            # Commit this play.
            conn.commit()

    def create_full_text(self):
        schema = Schema(
                    line_id = NUMERIC(stored=True),
                    content = TEXT(stored=True),
                    speaker = TEXT(stored=True),
                    play_id = NUMERIC(stored=True),
                    act_id = NUMERIC(stored=True),
                    scene_id = NUMERIC(stored=True)
                    )
        ix = create_in(FULL_TEXT_DIR, schema )
        writer = ix.writer()
        conn = sqlite3.connect(DBNAME)
        c = conn.cursor()
        rows = c.execute('''
                    SELECT L.id, L.text, L.speaker, 
                            P.id, A.id, S.id
                    FROM lines as L
                    JOIN plays as P, acts as A, scenes as S
                    ON L.play_id = P.id 
                        AND L.act_id = A.id 
                        AND L.scene_id =S.id
                    WHERE L.is_stagedir=0''')
        x = 0
        for row in rows:
            writer.add_document(line_id=row[0],
                                content=row[1],
                                speaker=row[2],
                                play_id=row[3],
                                act_id=row[4],
                                scene_id=row[5])
            x = x + 1
            if x % 1000 == 0:
                print(x)
        print('committing to the full text search')
        writer.commit()
        print('done')
        


def main():
    handler = DownloadHandler()
    handler.get_moby_works()
    handler.write_to_mysql()

    conn = mysql.connector.connect(user='pi', password='raspberry',host='192.168.1.200',
                                   database='shakespeare')
    c = conn.cursor()
    #c.execute('''select actor_lines.the_text, actor_lines.speaker, plays.title from actor_lines, plays WHERE
    #                    plays.id = actor_lines.play_id AND
    #                    actor_lines.the_text LIKE %s
    #                    ''', ('%the%',))
    c.execute('''select count(actor_lines.the_text) from actor_lines, plays WHERE
                        plays.id = actor_lines.play_id AND
                        actor_lines.the_text LIKE %s
                        ''', ('%the%',))
    x = 0
    for row in c:
        print(row)
    print(x)

        
if __name__ == '__main__':
    main()
 