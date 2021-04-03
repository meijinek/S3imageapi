# DESCRIPTION

When publishing an item in a store, the code will pull 5 random images of the item from google search and then upload a random one to S3.

The image will be then linked to the item record in DynamoDB.

Deleting the item from DynamoDB via a DELETE API call will also remove the linked image from S3.
