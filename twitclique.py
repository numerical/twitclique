import os
import sys
import pathlib
import logging
import argparse
from functools import reduce
from collections import defaultdict
from datetime import datetime

import twitter
from sqlalchemy import Column, ForeignKey, BigInteger, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    user_id = Column(BigInteger, primary_key=True)
    screen_name = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    last_cached = Column(DateTime)
    timestamp = Column(DateTime, default=datetime.utcnow)

class Friends(Base):
    __tablename__ = 'friends'

    follower = Column(BigInteger, ForeignKey('users.user_id'), nullable=False, primary_key=True)
    followee = Column(BigInteger, ForeignKey('users.user_id'), nullable=False, primary_key=True)

def _get_db():
    db_fname = pathlib.Path(__file__).parent / '_twit_cache.db'
    engine = create_engine(f'sqlite:///{db_fname.absolute()}')
    Base.metadata.create_all(engine)
    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)
    session = db_session()

    return session

def _set_log(v):
    if not v or v == 0:
        level = logging.WARN
    elif v == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    logging.basicConfig(level=level)

def _get_api(rate_limit=True):
        tck = os.getenv('TWITTER_CONSUMER_KEY')
        logging.debug('CONSUMER_KEY %s', tck)
        tcs = os.getenv('TWITTER_CONSUMER_SECRET')
        logging.debug('CONSUMER_SECRET %s', tcs)
        tak = os.getenv('TWITTER_ACCESS_KEY')
        logging.debug('ACCESS_KEY %s', tak)
        tas = os.getenv('TWITTER_ACCESS_SECRET')
        logging.debug('ACCESS_SECRET %s', tas)
        return twitter.Api(tck, tcs, tak, tas, sleep_on_rate_limit=rate_limit)

def get_ids(twitter, handle):
    user = twitter.GetUser(screen_name=handle)
    friends = twitter.GetFriendIDs(screen_name=handle)
    logging.info('%s friend count %d', handle, len(friends))
    followers = twitter.GetFollowerIDs(screen_name=handle)
    logging.info('%s follower count %d', handle, len(followers))
    return user, friends, followers

def get_users(twitter, handles, n=100):
    users = list()
    handles = list(handles)
    for handleset in (handles[i:i + n] for i in range(0, len(handles), n)):
        users.extend(twitter.UsersLookup(user_id=handleset))
    logging.info('Retrieved %d new users', len(users))
    return users

def update_friends_and_followers(db, user, friends, followers):
    friends_instances = [Friends(follower=user.id, followee=f) for f in friends]
    update_friends(db, friends_instances)
    follower_instances = [Friends(follower=f, followee=user.id) for f in friends]
    update_friends(db, follower_instances)

def update_friends(db, instances):
    fquery = db.query(Friends)
    instances = [instance for instance in instances
        if not db.query(fquery.filter(
            Friends.follower == instance.follower,
            Friends.followee == instance.followee).exists()).scalar()
    ]
    logging.info('Updating db with %d friends instances', len(instances))
    db.add_all(instances)
    db.commit()

def get_friends(db, user):
    fquery = db.query(Friends)
    friends = fquery.filter(Friends.follower == user.user_id).all()
    logging.info('Found %d friends for %s', len(friends), user.user_id)
    return db.query(User).filter(User.user_id.in_([f.followee for f in friends])).all()

def get_followers(db, user):
    fquery = db.query(Friends)
    followers = fquery.filter(Friends.followee == user.user_id).all()
    logging.info('Found %d followers for %s', len(followers), user.user_id)
    return db.query(User).filter(User.user_id.in_([f.follower for f in followers])).all()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', action='count')
    parser.add_argument('--loose', action='store_true', help='use friends instead of real_friends')
    parser.add_argument('subcommand', choices=['common', 'clique', 'mutuals'])
    parser.add_argument('handles', nargs='+')
    args = parser.parse_args()
    _set_log(args.v)

    logging.info('Handles: %s', ', '.join(args.handles))

    db = _get_db()
    twitter = _get_api()
    d = defaultdict(dict)
    for handle in args.handles:
        user = db.query(User).filter(User.screen_name == handle).first()
        if not user or user.last_cached is None:
            logging.info('Updating user %s', handle)
            userinfo, friends, followers = get_ids(twitter, handle)
            if not user:
                user = User(
                    user_id=userinfo.id,
                    screen_name=userinfo.screen_name,
                    name=userinfo.name,
                    last_cached=datetime.utcnow()
                )
                db.add(user)
            elif user.last_cached is None:
                user.last_cached = datetime.utcnow()
            else:
                return 1
            db.commit()
            total_ids = set(friends).union(followers)
            new_handles = total_ids - {x[0] for x in
                    db.query(User.user_id).filter(User.user_id.in_(total_ids)).all()}
            new_users = get_users(twitter, new_handles)
            db.add_all(
                [User(user_id=u.id, screen_name=u.screen_name, name=u.name) for u in new_users]
            )
            db.commit()
            update_friends_and_followers(db, userinfo, friends, followers)
        logging.info('Getting db info for %s', user.user_id)
        friends = get_friends(db, user)
        followers = get_followers(db, user)
        d['friends'][user.user_id] = friends
        d['followers'][user.user_id] = followers
        d['real_friends'][user.user_id] = set(friends).intersection(followers)
        print(d)

    if args.subcommand == 'common':
        logging.info('finding all common friends between %s', ', '.join(d))
        vals = d['friends'].values() if args.loose else d['real_friends']
        clique = list(reduce(lambda x, y: set.intersection(x, y), map(set, vals)))
        print(f'Found {len(clique)} friends in common')
        for userid in clique:
            print(f'Handle: @{userid.screen_name} -- {userid.name}')
    elif args.subcommand == 'mutuals':
        logging.info('finding all mutual connections between %s', ', '.join(d))
        raise NotImplementedError
    elif args.subcommand == 'clique':
        raise NotImplementedError
    else:
        logging.critical('wat')


if __name__ == '__main__':
    sys.exit(main())
