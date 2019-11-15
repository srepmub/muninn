import logging
import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Integer
import muninn.config as config
from muninn.exceptions import Error

import boto3
import botocore

logging.getLogger("boto3").setLevel(logging.CRITICAL)


class _S3Config(Mapping):
    _alias = "s3"

    host = Text
    port = Integer
    bucket = Text
    access_key = Text
    secret_access_key = Text


def create(configuration):
    options = config.parse(configuration.get("s3", {}), _S3Config)
    _S3Config.validate(options)
    return S3StorageBackend(**options)


class S3StorageBackend(StorageBackend):  # TODO '/' in keys to indicate directory, 'dir/' with contents?
    def __init__(self, bucket, host, port, access_key, secret_access_key):
        super(S3StorageBackend, self).__init__()

        self.bucket = bucket

        self._resource = boto3.resource(
            service_name='s3',
            aws_access_key_id='minio',
            aws_secret_access_key='minio1300',
            endpoint_url='http://localhost:9000',
        )

    def prepare(self):
        if not self.exists():
            self._resource.create_bucket(Bucket=self.bucket)

    def exists(self):
        try:
            # TODO ugly trick using creation_date
            creation_date = self._resource.Bucket(self.bucket).creation_date
            return (creation_date is not None)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

    def destroy(self):  # TODO individually deleting objects?
        if self.exists():
            bucket = self._resource.Bucket(self.bucket)
            bucket.objects.all().delete()
            bucket.delete()

    def product_path(self, product):  # TODO needed?
        return os.path.join(product.core.archive_path, product.core.physical_name)

    def put(self, paths, properties, plugin, use_current_path, use_symlinks):
        if use_symlinks:
            raise Error("S3 storage backend does not support symlinks")
        if use_current_path:
            raise Error("S3 storage backend does not (yet) support ingesting already ingested products")

        archive_path = plugin.archive_path(properties)
        properties.core.archive_path = archive_path
        physical_name = properties.core.physical_name

        # Upload file(s)
        for path in paths:
            key = os.path.join(archive_path, physical_name)

            # Add enclosing dir
            if plugin.use_enclosing_directory:
                key = os.path.join(key, os.path.basename(path))

            # Upload file
            self._resource.Object(self.bucket, key).upload_file(path)

    def put2(self, file_path, archive, product):
        plugin = archive.product_type_plugin(product.core.product_type)

        archive_path = product.core.archive_path
        physical_name = product.core.physical_name

        key = os.path.join(archive_path, physical_name)
        if plugin.use_enclosing_directory:
            key = os.path.join(key, physical_name)

        self._resource.Object(self.bucket, key).upload_file(file_path)

    def get(self, product_path, target_path, plugin, use_symlinks):
        if use_symlinks:
            raise Error("S3 storage backend does not support symlinks")

        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path):  # TODO slow?
            basename = os.path.basename(obj.key)
            target = os.path.join(target_path, basename)
            self._resource.Object(self.bucket, obj.key).download_file(target)

    def delete(self, product_path, properties, plugin):
        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path):  # TODO slow?
            obj.delete()
