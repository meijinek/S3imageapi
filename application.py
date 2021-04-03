from flask import Flask
from flask_restful import Resource, Api, reqparse
from flask_dynamo import Dynamo
from boto3.session import Session
from helpers import decimal_to_float, convert_to_decimal, remove_image_from_dicts
from image_operations import pull_and_upload_image, create_presigned_url, delete_image

# Elastic Beanstalk requires the Flask instance to be called application
application = Flask(__name__)
api = Api(application)

#  setting up the dynamodb table
#  no profile_name for elasticbeanstalk, will use an IAM role
boto_sess = Session(region_name="eu-west-2")

application.config['DYNAMO_TABLES'] = [
    {
         'TableName':  'ItemTableWithImages',
         'AttributeDefinitions': [{
             'AttributeName': 'name',
             'AttributeType': 'S'
         }],
         'KeySchema': [{
             'AttributeName': 'name',
             'KeyType': 'HASH'
         }],
         'BillingMode': 'PAY_PER_REQUEST'
    }
]
application.config['DYNAMO_SESSION'] = boto_sess

dynamo = Dynamo(application)

with application.app_context():
    dynamo.create_all()


class Item(Resource):
    #  parses json in incoming http request
    #  we only want price in the json body
    #  name is provided in the route
    parser = reqparse.RequestParser()
    parser.add_argument(
            'price',
            type=float,
            required=True,
            help="Cannot be left blank"
        )

    @classmethod
    def find_by_name(cls, name):
        result = dynamo.tables['ItemTableWithImages'].get_item(Key={
            'name': name
            }
        )
        if 'Item' in result:
            return decimal_to_float(result['Item'])

    @classmethod
    def insert(cls, item):
        dynamo.tables['ItemTableWithImages'].put_item(Item={
            'name': item['name'],
            'price': convert_to_decimal(item['price']),
            'image': pull_and_upload_image(item['name'])
            }
        )

    @classmethod
    def update(cls, item):
        delete_image(Item.find_by_name(item['name'])['image'])  # delete the existing image first
        dynamo.tables['ItemTableWithImages'].update_item(Key={
            'name': item['name']
            },
            UpdateExpression='SET price = :p, image = :i',
            ExpressionAttributeValues={
                ':p': convert_to_decimal(item['price']),
                ':i': pull_and_upload_image(item['name'])
            }
        )

    def get(self, name):
        item = self.find_by_name(name)
        if item:
            #  Adding a presigned url for image download
            item['download_url'] = create_presigned_url(object_name=item['image'], expiration=60)
            item['url_expires_in'] = 60
            #  remove the image key
            del item['image']
            return item
        return {'message': 'item not found'}

    def post(self, name):
        if self.find_by_name(name):
            return {'message': f'an item with name {name} already exists'}, 400
        data = Item.parser.parse_args()
        item = {'name': name, 'price': data['price']}

        try:
            Item.insert(item)
        except:
            return {'message': 'an error occurred inserting the item.'}, 500

        return item

    def delete(self, name):
        result = dynamo.tables['ItemTableWithImages'].delete_item(Key={
                'name': name
            },
            ReturnValues='ALL_OLD'
        )
        # Attributes key will be returned by boto3 if the item exists in the table
        if 'Attributes' in result:
            try:
                delete_image(result['Attributes']['image'])
            except:
                return {'message': 'item deleted but there was an issue removing image from S3'}
            return {'message': 'item deleted'}
        return {'message': 'item does not exist'}

    def put(self, name):
        data = Item.parser.parse_args()
        item = self.find_by_name(name)
        updated_item = {'name': name, 'price': data['price']}
        if item is None:
            try:
                Item.insert(updated_item)
            except:
                return {'message': 'an error occurred inserting the item.'}, 500
        else:
            try:
                Item.update(updated_item)
            except:
                return {'message': 'an error occurred updating the item.'}, 500
        return updated_item


#  might be worth iterating through the list of dictionaries and remove
#  the key called 'image' from each dictionary
class ItemList(Resource):
    def get(self):
        result = dynamo.tables['ItemTableWithImages'].scan(Limit=100)
        if len(result['Items']) == 0:
            return {'message': 'no items in the database found'}
        return decimal_to_float(remove_image_from_dicts(result['Items']))


api.add_resource(Item, '/item/<string:name>')
api.add_resource(ItemList, '/items')


#  app will keep restarting in beanstalk with gunicorn unless the if statement is added
#  https://github.com/benoitc/gunicorn/issues/1801#issuecomment-622409647
if __name__ == "__main__":
    application.run()
