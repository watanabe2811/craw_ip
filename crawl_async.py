import asyncio
import itertools
import logging
import os
import sys
import traceback
from pathlib import Path

import aioboto3
import httpx
import pandas as pd
import simplejson as json
import tqdm
from botocore.exceptions import ClientError
from httpx import AsyncClient

import config


async def get_location(client: AsyncClient, ip):
    url = f"{config.BASE_URL}/{ip}"
    rs = {
        "ip": ip,
        "url": url
    }
    try:
        response = await client.get(url)
        data = response.json()
        rs.update(data)
        return rs
    except Exception as e:
        traceback.print_exc()
        rs.update({
            "error": traceback.format_exc()
        })
        return rs
        # raise e



async def write_location(client: AsyncClient, ip, out, semaphore):
    async with semaphore:
        location = await get_location(client, ip)
        out.write(json.dumps(location, allow_nan=True))
        out.write("\n")


async def run_crawl_to_local_file(client: AsyncClient, ips: list[str], sheet_name):
    semaphore = asyncio.Semaphore(config.max_workers)  #
    local_file_name = config.output_file_name.format(sheet_name)
    with open(local_file_name, 'w') as out:
        to_do = [write_location(client, ip, out, semaphore) for ip in ips]
        to_do_inter = tqdm.tqdm(asyncio.as_completed(to_do), desc=f"run crawl for sheet {sheet_name}", total=len(ips))
        for coro in to_do_inter:
            await coro


async def read_ips_in_excel(file, sheet_names):
    def __read_source_excel():
        df = pd.read_excel(file, sheet_name=sheet_names)
        m_ips = {}
        for sheet_name in sheet_names:
            ips = []
            for ip in df[sheet_name]['IP_ADDRESS']:
                ips.append(ip)
            m_ips[sheet_name] = ips
        return m_ips

    return await asyncio.to_thread(__read_source_excel)


session = aioboto3.Session()


async def upload_local_file_to_s3(sheet_name):
    bucket = config.bucket
    file_name = config.output_file_name.format(sheet_name)
    object_name = config.output_object_name.format(sheet_name)
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    staging_path = Path(file_name)
    # Upload the file
    async with session.client("s3") as s3:
        try:
            with staging_path.open("rb") as spfp:
                await s3.upload_fileobj(spfp, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True


async def run_craw_one(client: AsyncClient, sheet_name, ips):
    await run_crawl_to_local_file(client, ips, sheet_name)
    await upload_local_file_to_s3(sheet_name)
    file_name = config.output_file_name.format(sheet_name)
    Path(file_name).unlink(missing_ok=True)


async def supervisor(mips: dict[str, list[str]]):
    async with httpx.AsyncClient() as client:
        for (sheet_name, ips) in mips.items():
            print(f"start craws {sheet_name} with {len(ips)} ip(s)", flush=True)
            await run_craw_one(client, sheet_name, ips)


async def get_sheet_names(file, start, end):
    # def __get_sheet_name():
    #     sheet_names = []
    #     with open("data/sheet_names.txt", 'r') as text_file:
    #         for line in itertools.islice(text_file, start, end):
    #             sheet_names.append(str(line).strip())
    #     return sheet_names
    def __get_sheet_name():
        input = pd.ExcelFile(file)
        return input.sheet_names[start:end]

    return await asyncio.to_thread(__get_sheet_name)


async def run(file, start, end):
    print(f"start run craws {file} from {start} -> {end}", flush=True)
    print(f"load sheet name from {start} -> {end}", flush=True)
    sheet_names = await get_sheet_names(file,start - 1, end)
    print(f"start load all ips from {file}", flush=True)
    mips = await read_ips_in_excel(file, sheet_names)
    print(f"start run crawl", flush=True)
    await supervisor(mips)
    print(f"done", flush=True)


if __name__ == '__main__':
    file = config.input_file
    args = sys.argv[1:]
    start = int(args[0])
    end = int(args[1])
    coro = run(file, start, end)
    asyncio.run(coro)
