import boto3


class CommandFile:
    def __init__(self):
        self.kv = boto3.client('dynamodb')

    def lock(self, table_to_ingest):
        i = {'Lock': {'S': table_to_ingest}}
        self.kv.put_item(TableName='Locks', Item=i,
                         Expected={'Lock': {'Exists': False}})

    def unlock(self, table_to_ingest):
        i = {'Lock': {'S': table_to_ingest}}
        self.kv.delete_item(TableName='Locks', Key=i)

    def get_last_updated(self, table_to_ingest):
        k = {'Table': {'S': table_to_ingest}}
        results = self.kv.get_item(TableName='LastUpdate', Key=k)
        if results is None:
            return None
        item = results['Item']['Timestamp']['S']
        return item

    def put_last_updated(self, table_to_ingest, last_updated_timestamp):
        i = {'Table': {'S': table_to_ingest},
             'Timestamp': {'S': last_updated_timestamp}}
        self.kv.put_item(TableName='LastUpdate', Item=i)
