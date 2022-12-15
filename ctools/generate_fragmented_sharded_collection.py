#!/usr/bin/env python3
#

import argparse
import asyncio
import bson
import json
import logging
import math
import random
import sys
import uuid

from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from common import Cluster, ShardCollectionUtil, yes_no
from tqdm import tqdm

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

maxInteger = sys.maxsize
minInteger = -sys.maxsize - 1


def fmt_bytes(num):
    suffix = "B"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos(warn_only=False)

    ns = {'db': args.ns.split('.', 1)[0], 'coll': args.ns.split('.', 1)[1]}
    shard_collection = ShardCollectionUtil(
        args.ns,
        uuid=uuid.uuid4(),
        shard_key={'account_id': 1, 'account_sub_id': 1},
        unique=True,
        fcv=await cluster.FCV,
    )

    shardIds = await cluster.shardIds

    logging.info(f"Enabling sharding for database {ns['db']}")
    await cluster.adminDb.command({'enableSharding': ns['db']})

    logging.info(
        f'Placing {args.num_chunks} chunks over {shardIds} for collection {args.ns} with a shard key of {args.shard_key_type}'
    )

    uuid_shard_key_byte_order = None
    if args.shard_key_type == 'uuid-little':
        uuid_shard_key_byte_order = 'little'
        logging.info(f'Will use {uuid_shard_key_byte_order} byte order for generating UUIDs')
    elif args.shard_key_type == 'uuid-big':
        uuid_shard_key_byte_order = 'big'
        logging.info(f'Will use {uuid_shard_key_byte_order} byte order for generating UUIDs')
    oid = args.shard_key_type == 'oid'

    logging.info(f'Cleaning up old entries for {args.ns} ...')
    dbName, collName = args.ns.split('.', 1)
    await cluster.client[dbName][collName].drop()
    await cluster.configDb.collections.delete_one({'_id': args.ns})
    logging.info(f'Cleaned up old entries for {args.ns}')

    sem = asyncio.Semaphore(10)

    ###############################################################################################
    # Create the collection on each shard
    ###############################################################################################
    shard_connections = {}

    async def safe_create_shard_indexes(shard):
        async with sem:
            logging.info('Creating shard key indexes on shard ' + shard['_id'])
            client = shard_connections[shard['_id']] = await cluster.make_direct_shard_connection(
                shard)
            db = client[ns['db']]

            await db.command({
                'applyOps': [{
                    'op': 'c',
                    'ns': ns['db'] + '.$cmd',
                    'ui': shard_collection.uuid,
                    'o': {
                        'create': ns['coll'],
                    },
                }]
            }, codec_options=cluster.system_codec_options)

            await db.command({
                'createIndexes': ns['coll'],
                'indexes': [{
                    'key': {
                        'account_id': 1,
                        'account_sub_id': 1
                    },
                    'name': 'Shard key index'
                }]
            })

    tasks = []
    async for shard in cluster.configDb.shards.find({}):
        tasks.append(asyncio.ensure_future(safe_create_shard_indexes(shard)))
    await asyncio.gather(*tasks)

    ###############################################################################################
    # Create collection and chunk entries on the config server
    ###############################################################################################

    def gen_chunks(num_chunks):
        def make_shard_key(i):
            if uuid_shard_key_byte_order:
                return uuid.UUID(bytes=i.to_bytes(16, byteorder=uuid_shard_key_byte_order))
            elif oid:
                return bson.objectid.ObjectId(i.to_bytes(12, byteorder='big'))
            else:
                return i

        for i in range(num_chunks):
            if len(shardIds) == 1:
                shardId = shardIds[0]
            else:
                sortedShardIdx = math.floor(i / (num_chunks / len(shardIds)))
                shardId = random.choice(
                    shardIds[:sortedShardIdx] + shardIds[sortedShardIdx + 1:]
                ) if random.random() < args.fragmentation else shardIds[sortedShardIdx]

            obj = {'shard': shardId}

            if i == 0:
                obj = {
                    **obj,
                    **{
                        'min': {
                            'account_id': bson.min_key.MinKey,
                            'account_sub_id': bson.min_key.MinKey
                        },
                        'max': {
                            'account_id': make_shard_key(i * 10000),
                            'account_sub_id': bson.min_key.MinKey
                        },
                    }
                }
            elif i == num_chunks - 1:
                obj = {
                    **obj,
                    **{
                        'min': {
                            'account_id': make_shard_key((i - 1) * 10000),
                            'account_sub_id': bson.min_key.MinKey
                        },
                        'max': {
                            'account_id': bson.max_key.MaxKey,
                            'account_sub_id': bson.max_key.MaxKey
                        },
                    }
                }
            else:
                obj = {
                    **obj,
                    **{
                        'min': {
                            'account_id': make_shard_key((i - 1) * 10000),
                            'account_sub_id': bson.min_key.MinKey
                        },
                        'max': {
                            'account_id': make_shard_key(i * 10000),
                            'account_sub_id': bson.min_key.MinKey
                        }
                    }
                }

            yield obj


    async def safe_write_chunks(shard, chunks_subset, progress):
        async with sem:
            write_chunks_entries = asyncio.ensure_future(
                cluster.configDb.chunks.insert_many(chunks_subset, ordered=False))

            await write_chunks_entries
            progress.update(len(chunks_subset))

    with tqdm(total=args.num_chunks, unit=' chunks') as progress:
        progress.write('Writing chunks entries ...')
        batch_size = 1000

        shard_to_chunks = {}
        tasks = []
        for c in shard_collection.generate_config_chunks(gen_chunks(args.num_chunks)):
            shard = c['shard']
            if not shard in shard_to_chunks:
                shard_to_chunks[shard] = [c]
            else:
                shard_to_chunks[shard].append(c)

            if len(shard_to_chunks[shard]) == batch_size:
                tasks.append(
                    asyncio.ensure_future(
                        safe_write_chunks(shard, shard_to_chunks[shard], progress)))
                del shard_to_chunks[shard]

        for s in shard_to_chunks:
            tasks.append(asyncio.ensure_future(safe_write_chunks(s, shard_to_chunks[s], progress)))

        await asyncio.gather(*tasks)
        progress.write('Chunks write completed')

    logging.info('Writing collection entry')
    await cluster.configDb.collections.insert_one(shard_collection.generate_collection_entry())


if __name__ == "__main__":

    def kb_to_bytes(kilo):
        return int(kilo) * 1024

    argsParser = argparse.ArgumentParser(
        description='Tool to generated a sharded collection with various degree of fragmentation')
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        type=str)
    argsParser.add_argument('--ns', help='The namespace to create', type=str)
    argsParser.add_argument('--num-chunks', help='The number of chunks to create',
                            dest='num_chunks', type=int)
    argsParser.add_argument('--shard-key-type', help='The type to use for a shard key',
                            dest='shard_key_type', type=str,
                            choices=['integer', 'uuid-little', 'uuid-big', 'oid'], default='integer')
    argsParser.add_argument(
        '--fragmentation',
        help="""A number between 0 and 1 indicating the level of fragmentation of the chunks. The
           fragmentation is a measure of how likely it is that a chunk, which needs to sequentially
           follow the previous one, on the same shard, is actually not on the same shard.""",
        dest='fragmentation', type=float, default=0.10)

    list = " ".join(sys.argv[1:])

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    logging.info(f"Starting with parameters: '{list}'")

    args = argsParser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
