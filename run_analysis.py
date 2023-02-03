import configparser
import psycopg2
from analysis_queries import union_record_counts, weekly_user_avg_play, level_song_play_by_artist


def check_records(cur):

    cur.execute(union_record_counts)
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

def user_song_plays(cur):
    cur.execute(weekly_user_avg_play)
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

def artist_preference(cur):
    cur.execute(level_song_play_by_artist)
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    ## for analysis purpose turning on auto commit
    conn.set_session(autocommit=True)

    print("Check record counts...")
    check_records(cur)

    print("Weekly songplay analysis...")
    user_song_plays(cur)

    print("Artist preference analysis...")
    artist_preference(cur)
    

    conn.close()


if __name__ == "__main__":
    main()