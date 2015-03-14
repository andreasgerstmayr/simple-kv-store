import random
from java.io import ByteArrayInputStream, DataInputStream
import vertx
from core.event_bus import EventBus

config = vertx.config()
logger = vertx.logger()
verticle_id = random.randint(100, 999)

logger.info("Starting server verticle %d" % verticle_id)


# see http://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#readUTF()
def java_utf8_message_dispatcher(buffer):
    byte_array = [buffer.get_byte(i) for i in range(buffer.length)]
    byteArrayInputStream = ByteArrayInputStream(byte_array)
    dataInputStream = DataInputStream(byteArrayInputStream)

    while dataInputStream.available() > 0:
        try:
            yield dataInputStream.readUTF()
        except:
            pass

def log_and_send_response(command, shard=None, sock=None, response=""):
    shard = shard if shard is not None else '-'
    logger.info("Verticle %d, shard %s: %s ==> %s" % (verticle_id, shard, command, response))

    if response:
        sock.write_str(response + '\n')

def process_command(sock, command):
    parts = command.split(' ')
    action = parts[0]
    if action in ['GET', 'PUT', 'DELETE']:
        key = int(parts[1])
        shard = key % config['database']['shards']
    else:
        key = None
        shard = None

    if action == 'GET':
        def reply_handler(message):
            if 'data' in message.body:
                log_and_send_response(command, shard, sock, message.body['data'])
            else:
                log_and_send_response(command, shard, sock, message.body['error'])
        EventBus.send('kvstore.%d.get' % shard, {'key': key}, reply_handler)

    elif action == 'PUT':
        def reply_handler(message):
            if 'error' in message.body:
                log_and_send_response(command, shard, sock, message.body['error'])
            else:
                log_and_send_response(command, shard, sock, "Stored key '%d'" % key)
        EventBus.send('kvstore.%d.put' % shard, {'key': key, 'data': parts[2]}, reply_handler)

    elif action == 'DELETE':
        def reply_handler(message):
            if 'error' in message.body:
                log_and_send_response(command, shard, sock, message.body['error'])
            else:
                log_and_send_response(command, shard, sock, "Deleted key '%d'" % key)
        EventBus.send('kvstore.%d.delete' % shard, {'key': key}, reply_handler)

    elif action == 'ALLKEYS':
        class KeyAccumulation:
            def __init__(self):
                self.all_keys = []
                self.pending_responses = 0

            def reply_handler(self, message):
                if 'keys' in message.body:
                    self.all_keys.extend(message.body['keys'])

                    self.pending_responses -= 1
                    if self.pending_responses == 0:
                        log_and_send_response(command, None, sock, repr(self.all_keys))

            def send_requests(self, shards):
                self.pending_responses = shards
                for shard in range(shards):
                    EventBus.send('kvstore.%d.allkeys' % shard, {}, self.reply_handler)

        keyAcc = KeyAccumulation()
        keyAcc.send_requests(config['database']['shards'])

    else:
        log_and_send_response(command, None, sock, 'Unknown command')


server = vertx.create_net_server()

@server.connect_handler
def connect_handler(sock):
    logger.info("Verticle %d: new connection" % (verticle_id))

    def data_handler(buffer):
        last_byte = buffer.get_byte(buffer.length-1)

        if last_byte == ord('\n'): # telnet
            message_dispatcher = lambda buffer: [buffer.to_string().strip()]
        else: # java utf8 format
            message_dispatcher = java_utf8_message_dispatcher

        for command in message_dispatcher(buffer):
            process_command(sock, command)

    def closed_handler():
        logger.info("Verticle %d: closed connection" % (verticle_id))

    sock.data_handler(data_handler)
    sock.close_handler(closed_handler)

server.listen(config['server']['port'], config['server']['host'])
