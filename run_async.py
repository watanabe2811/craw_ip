import asyncio
import sys

import config
from crawl_async import run
if __name__ == '__main__':
    file = config.input_file
    args = sys.argv[1:]
    start = int(args[0])
    end = int(args[1])
    coro = run(file, start, end)
    asyncio.run(coro)
