import traceback
from pathlib import Path
from time import sleep

import pandas as pd
import requests
import simplejson as json
from tqdm import tqdm
import logging
import boto3
from botocore.exceptions import ClientError
import os
import config


def get_location(ip):
    url = f"{config.BASE_URL}/{ip}"
    rs = {
        "ip": ip,
        "url": url
    }
    try:

        response = requests.get(url)
        data = response.json()
        rs.update(data)
        return rs
    except Exception as e:
        traceback.print_exc()
        rs.update({
            "error": traceback.format_exc()
        })
        return rs


def read_source_excel(file, sheet):
    df = pd.read_excel(file, sheet_name=sheet)
    for ip in df['IP_ADDRESS']:
        yield ip


def run_crawl(input_file, sheet_name, output_file):
    i = 0
    with open(output_file, 'w') as out:
        for ip in tqdm(read_source_excel(input_file, sheet_name), desc=f"Run crawl for {sheet_name}"):
            info = get_location(ip)
            out.write(json.dumps(info))
            out.write("\n")
    return output_file


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def sheet_names(input_file):
    input = pd.ExcelFile(input_file)
    for sheet_name in input.sheet_names:
        yield sheet_name


def run(input_file, sheet_name):
    print(f"start craw {input_file} in sheet {sheet_name}")
    output_file = config.output_file_name.format(sheet_name)
    object_output = config.output_object_name.format(sheet_name)
    print(f"run craw {sheet_name} to {output_file}")
    run_crawl(input_file, sheet_name, output_file)
    print(f"put file {output_file} -> {config.bucket}/{object_output}")
    upload_file(file_name=output_file, bucket=config.bucket, object_name=object_output)
    print(f"done craw {sheet_name}")
    Path(output_file).unlink(missing_ok=True)

