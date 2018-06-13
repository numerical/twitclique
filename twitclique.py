import os
import sys
import time
import pickle
import pathlib
import logging
import argparse
from functools import reduce
from collections import defaultdict

import twitter

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', action='count')
    parser.add_argument('--loose', action='store_true', help='use friends instead of real_friends')
    parser.add_argument('subcommand', choices=['common', 'clique', 'mutuals'])
    parser.add_argument('handles', nargs='+')
    args = parser.parse_args()
    _set_log(args.v)

    logging.info('Handles: %s', ', '.join(args.handles))

    cache_dir = pathlib.Path(__file__).parent / '_twit_cache'
    t = _get_api()
    d = defaultdict(dict)
    for handle in args.handles:
        hcache = cache_dir / handle
        if hcache.exists():
            with hcache.open('rb') as fo:
                ts, friends, followers, real_friends = pickle.load(fo)
                logging.info('Loaded %s from cache with timestamp: %s', handle, time.ctime(ts))
        else:
            friends = t.GetFriends(screen_name=handle)
            logging.info('%s friend count %d', handle, len(friends))
            followers = t.GetFollowers(screen_name=handle)
            logging.info('%s follower count %d', handle, len(followers))
            real_friends = set(friends).intersection(followers)
            logging.info('%s real_friends count %d', handle, len(real_friends))
            with hcache.open('wb') as fo:
                pickle.dump((time.time(), friends, followers, real_friends), fo)
        d['real_friends'][handle] = real_friends
        d['friends'][handle] = friends
        d['followers'][handle] = followers

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
    else:
        logging.critical('wat')


if __name__ == '__main__':
    sys.exit(main())
