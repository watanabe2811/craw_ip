import sys
import traceback

import config
import crawl
from concurrent import futures
from functools import partial
import tqdm
import itertools


def write_sheet_names(input_file):
    with open("data/sheet_names.txt", 'w') as sout:
        for sheet_name in crawl.sheet_names(input_file):
            sout.write(sheet_name)
            sout.write("\n")


def read_sheet_names(start, end):
    sheet_names = []
    with open("data/sheet_names.txt", 'r') as text_file:
        for line in itertools.islice(text_file, start, end):
            sheet_names.append(str(line).strip())
    return sheet_names


def run_craw(input_file, sheet_names):
    run_craw = partial(crawl.run, input_file=input_file)
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        to_do: list[futures.Future] = []
        count = 0
        for sheet_name in sheet_names:
            future = executor.submit(run_craw, sheet_name=sheet_name)
            to_do.append(future)
            count += 1
        done_iter = futures.as_completed(to_do)
        done_iter_process = tqdm.tqdm(done_iter, total=count, desc="Total")
        for future in done_iter:
            try:
                future.result()
            except:
                traceback.print_exc()


if __name__ == '__main__':
    input_file = config.input_file
    args = sys.argv[1:]
    start = int(args[0])
    end = int(args[1])
    print(f"start {start} -> {end}")
    sheet_names= read_sheet_names(start - 1, end)
    run_craw(input_file,sheet_names)
    print("Done")
    # write_sheet_names(input_file)
