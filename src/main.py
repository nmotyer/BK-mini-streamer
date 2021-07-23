import shutil
import boto3
import os
import zipfile
from s3stream import S3File
from lxml import etree

os.environ['AWS_PROFILE'] = 'personal'

s3 = boto3.resource('s3')

def read_s3_zip():
    s3_object = s3.Object(bucket_name="bk-abn-raw", key="ABN/public_split_1_10.zip")
    s3_file = S3File(s3_object)
    return s3_file

def stream_file(s3_file):
    with zipfile.ZipFile(s3_file) as zf:
        zfiles = zf.namelist()
        print(zfiles)
        with zf.open(zfiles[0], 'r') as myxml:
            iterxml = etree.iterparse(myxml)
            for action, elem in iterxml:
                if elem.text:
                    print(elem.tag, elem.text)
    return

def entry_point(event=None, context=None):
    #print(event)
    s3_file = read_s3_zip()
    stream_file(s3_file)
    return

entry_point()
