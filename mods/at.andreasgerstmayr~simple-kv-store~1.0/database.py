import vertx
from core.event_bus import EventBus

config = vertx.config()
logger = vertx.logger()
shard = config['shard']

logger.info("Starting key value store verticle shard: %d" % shard)


datastore = {}

def get_key(message):
    key = message.body['key']

    if key in datastore:
        message.reply({'data': datastore[key]})
    else:
        message.reply({'error': "Key '%s' not found." % key})

def put_key(message):
    key = message.body['key']
    data = message.body['data']

    datastore[key] = data
    message.reply({})

def delete_key(message):
    key = message.body['key']

    if key in datastore:
        del datastore[key]
        message.reply({})
    else:
        message.reply({'error': "Key '%s' not found." % key})

def list_all_keys(message):
    message.reply({'keys': datastore.keys()})

EventBus.register_handler('kvstore.%d.get' % shard, handler=get_key)
EventBus.register_handler('kvstore.%d.put' % shard, handler=put_key)
EventBus.register_handler('kvstore.%d.delete' % shard, handler=delete_key)
EventBus.register_handler('kvstore.%d.allkeys' % shard, handler=list_all_keys)
