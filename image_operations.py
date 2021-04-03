import base64
import boto3
import os
import logging

from botocore.exceptions import ClientError
from icrawler import ImageDownloader
from icrawler.builtin import GoogleImageCrawler
from six.moves.urllib.parse import urlparse
from random import randint


#  this will create a base64 string from the image name as the filename
class Base64NameDownloader(ImageDownloader):

    def get_filename(self, task, default_ext):
        url_path = urlparse(task['file_url'])[2]
        if '.' in url_path:
            extension = url_path.split('.')[-1]
            if extension.lower() not in [
                    'jpg', 'jpeg', 'png', 'bmp', 'tiff', 'gif', 'ppm', 'pgm'
            ]:
                extension = default_ext
        else:
            extension = default_ext
        # works for python 3
        filename = base64.b64encode(url_path.encode()).decode()
        return '{}.{}'.format(filename, extension)


def pull_and_upload_image(item):
    try:
        google_crawler = GoogleImageCrawler(
            # downloader_cls=PrefixNameDownloader,
            downloader_cls=Base64NameDownloader,
            downloader_threads=1,
            storage={'root_dir': 'test_images'})
        google_crawler.crawl(item, max_num=5)
    except ex as ex:
        print('Unable to download and save image', ex)
        return None

    #  select a random file from the 5 downloaded
    try:
        file_to_upload = os.listdir('./test_images')[randint(0, 4)]
    except ex as ex:
        print('Fewer than 5 images were downloaded, aborting...', ex)
        return None

    sess = boto3.session.Session()

    s3_con_cli = sess.client(service_name='s3', region_name='eu-west-2')
    s3_con_re = sess.resource('s3', region_name='eu-west-2')

    s3_con_re.meta.client.upload_file(f'./test_images/{file_to_upload}', 'oortcloud-test1', file_to_upload)
    waiter = s3_con_cli.get_waiter('object_exists')
    waiter.wait(Bucket='oortcloud-test1', Key=file_to_upload)

    #  clean the directory up after download
    try:
        for file in os.listdir('./test_images'):
            os.remove(f'./test_images/{file}')
    except ex as ex:
        print('Unable to remove the downloaded file from temp location', ex)

    return file_to_upload


def create_presigned_url(object_name, bucket_name='oortcloud-test1', expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    sess = boto3.session.Session()

    s3_con_cli = sess.client(service_name='s3', region_name='eu-west-2')
    try:
        response = s3_con_cli.generate_presigned_url('get_object',
                                                     Params={'Bucket': bucket_name,
                                                             'Key': object_name},
                                                     ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response


def delete_image(object_name, bucket_name='oortcloud-test1'):
    sess = boto3.session.Session()

    s3_con_cli = sess.client(service_name='s3', region_name='eu-west-2')

    s3_con_cli.delete_object(
        Bucket=bucket_name,
        Key=object_name
    )
