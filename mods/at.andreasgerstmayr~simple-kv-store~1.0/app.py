import vertx

config = vertx.config()
logger = vertx.logger()


class DeploymentManager:
    def __init__(self, num_verticles):
        self.pending_verticles = num_verticles

    def deploy_handler(self, err, deployment_id):
        if err is not None:
            err.printStackTrace()
        else:
            self.pending_verticles -= 1

            if self.pending_verticles == 0:
                logger.info('Successfully deployed all verticles.')

def get_database_config(config, shard):
    dbconfig = config.copy()
    dbconfig['shard'] = shard
    return dbconfig


deployment_manager = DeploymentManager(config["database"]["shards"] + 1)

# start all database shards
for shard in range(config["database"]["shards"]):
    vertx.deploy_verticle("database.py", get_database_config(config, shard), handler=deployment_manager.deploy_handler)

# start multiple instances of server
vertx.deploy_verticle("server.py", config, config["server"]["verticles"], deployment_manager.deploy_handler)
