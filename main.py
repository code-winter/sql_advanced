import psycopg2
import sqlalchemy


def filling_database():
    """
    Fills the database with some information
    """
    db = ' '
    engine = sqlalchemy.create_engine(db)
    query = engine.connect()
    # this clears the DB prior to filling, avoids collisions
    query.execute("""TRUNCATE artists CASCADE;""")
    query.execute("""TRUNCATE genre CASCADE;""")
    query.execute("""TRUNCATE artist_genre CASCADE;""")
    query.execute("""TRUNCATE albums CASCADE;""")
    query.execute("""TRUNCATE albums_artists CASCADE;""")
    query.execute("""TRUNCATE tracks CASCADE;""")
    query.execute("""TRUNCATE comps CASCADE;""")

    query.execute("""
       INSERT INTO artists
           VALUES
               (0, 'Boy Pablo'),
               (1, 'Roy Pablo'),
               (2, 'Poy Rablo'),
               (3, 'Toy Roy'),
               (4, 'Girl Padme'),
               (5, 'Chowder'),
               (6, 'Troy Gizmo'),
               (7, 'GiztheWiz');
       """)
    query.execute("""
       INSERT INTO genre
           VALUES
               (0, 'Rock'),
               (1, 'Pop'),
               (2, 'Metal'),
               (3, 'Hip-Hop'),
               (4, 'Jazz'),
               (5, 'Soul');
       """)
    query.execute("""
       INSERT INTO artist_genre
           VALUES
               (0, 0, 1),
               (1, 1, 3),
               (2, 2, 0),
               (3, 3, 2),
               (4, 4, 4),
               (5, 5, 3),
               (6, 6, 4),
               (7, 7, 5),
               (8, 7, 4),
               (9, 0, 2);
       """)
    query.execute(("""
       INSERT INTO albums
           VALUES
               (0, 'Hybrid', 2020),
               (1, 'The One and Only', 2020),
               (2, 'Escape The Plan', 2021),
               (3, 'Courage of the Beholder', 2011),
               (4, 'March the Third', 2019),
               (5, 'Colonial', 2009),
               (6, 'Uphill Climb', 2015),
               (7, 'System Meltdown', 2019),
               (8, 'Sweet & Sour', 2020),
               (9, 'Twisted Helix', 2019);
       """))
    query.execute("""
       INSERT INTO albums_artists
           VALUES 
               (0, 0, 1),
               (1, 1, 2),
               (2, 2, 3),
               (3, 3, 4),
               (4, 4, 5),
               (5, 1, 6),
               (6, 6, 7),
               (7, 3, 8),
               (8, 7, 9),
               (9, 4, 0);
       """)
    query.execute("""
       INSERT INTO tracks
           VALUES
               (0, 'Keeping My Promise', 180, 1),
               (1, 'My Own', 200, 2),
               (2, 'Far Away Land', 160, 3),
               (3, 'Double my fee', 221, 4),
               (4, 'Create the Creator', 90, 5),
               (5, 'Surge', 190, 6),
               (6, 'Keepsake', 301, 7),
               (7, 'Fly, You FOOLS!', 280, 8),
               (8, 'Stranger and Stranger', 101, 9),
               (9, 'Tiny Giant', 79, 4),
               (10, 'Knightmare', 170, 8),
               (11, 'Festive Shower of Gifts', 208, 4),
               (12, 'Thermonuclear', 381, 7),
               (13, 'A Dull Switchblade', 100, 1),
               (14, 'Twin Winds of the Savannah', 199, 9),
               (15, 'Heavy Metal Machinery', 331, 9),
               (16, 'Scratch Card', 79, 2),
               (17, 'Cold Flame, Hot Snow', 119, 0),
               (18, 'Labyrinth of Thoughts', 142, 6),
               (19, 'Stay a Little More', 240, 0);
       """)
    query.execute("""
       INSERT INTO comps
           VALUES
               (0, 'State of Art', 2020),
               (1, 'For a Rainy Day', 2019),
               (2, 'Piece of Peace', 2021),
               (3, 'Unusual', 2021),
               (4, 'Work Your Body', 2018),
               (5, 'Moody and Sad', 2018),
               (6, 'Stories', 2010),
               (7, 'Bright Sunshine', 2019),
               (8, 'Jolly Good!', 2020),
               (9, 'One of Many, One of the Best', 2017),
               (10, 'Beach, Sand, Summer', 2016);

       """)
    query.execute("""
       INSERT INTO comps_tracks
           VALUES
               (0, 0, 1),
               (1, 1, 2),
               (2, 2, 1),
               (3, 3, 4),
               (4, 4, 8),
               (5, 5, 6),
               (6, 6, 0),
               (7, 7, 8),
               (8, 7, 19),
               (9, 10, 10);

       """)


def main():
    db = ' '
    engine = sqlalchemy.create_engine(db)
    query = engine.connect()
    filling_database()

    res = query.execute("""
    SELECT g.genre_name, COUNT(a.artist_name) AS count FROM genre AS g
    JOIN artist_genre AS ag ON g.genre_id = ag.genre
    JOIN artists AS a ON ag.artist = a.artist_id
    GROUP BY g.genre_name
    ORDER BY count DESC
    """).fetchall()
    print('\nAmount of artists performing in a genre:')
    print(res)

    res = query.execute("""
    SELECT a.album_name, COUNT(t.track_title) AS count FROM albums AS a
    JOIN tracks AS t ON a.album_id = t.album
    WHERE a.album_release_year BETWEEN 2019 AND 2020
    GROUP BY a.album_name
    ORDER BY count DESC
    """).fetchall()
    print('\nAmount of tracks released in 2019-2020 in albums:')
    print(res)

    res = query.execute("""
     SELECT a.album_name, AVG(t.track_runtime) AS avg FROM albums AS a
     JOIN tracks AS t ON a.album_id = t.album
     GROUP BY a.album_name
     ORDER BY avg DESC
     """).fetchall()
    print('\nAverage track runtime by albums:')
    print(res)

    res = query.execute("""
     SELECT a.artist_name FROM artists AS a
     JOIN albums_artists AS aa ON a.artist_id = aa.artist
     JOIN albums AS al ON aa.album = al.album_id
     WHERE NOT al.album_release_year = 2020
     """).fetchall()
    print('\nArtists that did not release albums in 2020:')
    print(res)

    res = query.execute("""
        SELECT c.comp_name FROM comps AS c
        JOIN comps_tracks AS ct ON c.comp_id = ct.comp
        JOIN tracks AS t ON ct.track = t.track_id
        JOIN albums AS al ON t.album = al.album_id
        JOIN albums_artists AS aa ON al.album_id = aa.album
        JOIN artists AS a ON aa.artist = a.artist_id
        WHERE a.artist_name iLIKE '%%Roy Pablo%%'
        GROUP BY c.comp_name
        """).fetchall()
    print('\nCompilations with artist named Roy Pablo:')
    print(res)

    res = query.execute("""
        SELECT al.album_name FROM albums AS al
        JOIN albums_artists AS aa ON al.album_id = aa.album
        JOIN artists AS a ON aa.artist = a.artist_id
        JOIN artist_genre AS ag ON a.artist_id = ag.artist
        GROUP BY album_name
        HAVING COUNT(ag.genre) > 1
        """).fetchall()
    print('\nAlbums with artists in multiple genre:')
    print(res)

    res = query.execute("""
        SELECT DISTINCT t.track_title FROM tracks AS t
        FULL OUTER JOIN comps_tracks AS ct ON t.track_id = ct.track
        FULL OUTER JOIN comps AS c ON ct.comp = c.comp_id
        WHERE comp_name IS NULL
        """).fetchall()
    print('\nTracks that are not in compilations:')
    print(res)

    res = query.execute("""
        SELECT a.artist_name FROM artists AS a
        JOIN albums_artists AS aa ON a.artist_id = aa.artist
        JOIN albums AS al ON aa.album = al.album_id
        JOIN tracks AS t ON al.album_id = t.album
        WHERE t.track_runtime = (
            SELECT MIN(track_runtime) FROM tracks
            )
        GROUP BY a.artist_name
        """).fetchall()
    print('\nArtists with shortest track(s):')
    print(res)

    res = query.execute("""
        SELECT al.album_name FROM albums AS al
        JOIN tracks AS t ON al.album_id = t.album
        GROUP BY al.album_name
        HAVING COUNT(t.track_id) = (
            SELECT COUNT(t.track_id) AS count FROM tracks AS t
            JOIN albums AS al ON t.album = al.album_id
            GROUP BY al.album_name
            ORDER BY count
            LIMIT 1
            )
        """).fetchall()
    print('\nAlbums with least tracks:')
    print(res)


if __name__ == '__main__':
    main()


