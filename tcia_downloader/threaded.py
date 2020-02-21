import asyncio
import concurrent.futures
from threading import Queue
import time


def blocking_io(i):
    # File operations (such as logging) can block the
    # event loop: run them in a thread pool.
    time.sleep(1)
    return i + 1

    return "done"


def cpu_bound():
    # CPU-bound operations will block the event loop:
    # in general it is preferable to run them in a
    # process pool.
    return sum(i * i for i in range(10 ** 7))


async def do_something(coro):
    print("inside")
    result = await coro
    return result + 1


async def produce(q):
    for i in range(6):
        print("inside produce")
        await q.put(i)
    await q.put("poison")


async def consume():
    item = await q.get()


async def produce2(s):
    for i in range(12):
        await s.acquire()
        yield (i)


async def do(loop, pool, sem, item):
    await loop.run_in_executor(pool, blocking_io, item)
    sem.release()


# async def main():
#     q = asyncio.Queue(3)
#     loop = asyncio.get_running_loop()
#     results = []
#     # 2. Run in a custom thread pool:
#     pool = concurrent.futures.ThreadPoolExecutor(6)
#     start = time.perf_counter()
#     print("launch producer")
#     to_wait = asyncio.create_task(produce(q))
#     print("producer launched", q.qsize())
#     print(q.qsize())
#     while True:
#         print("inside consumer")
#         print(q.qsize())
#         item = await q.get()
#         if item == "poison":
#             break
#         results.append(loop.run_in_executor(pool, blocking_io, item))
#         print("dispatched")
#     final = await asyncio.gather(*results)
#     pool.shutdown()
#     # await to_wait
#     print("custom thread pool", final, time.perf_counter() - start)

#     # # 3. Run in a custom process pool:
#     # with concurrent.futures.ProcessPoolExecutor() as pool:
#     #     result = await loop.run_in_executor(pool, cpu_bound)
#     #     print("custom process pool", result)


async def main():
    s = asyncio.Semaphore(6)
    q = Queue()
    loop = asyncio.get_running_loop()
    pool = concurrent.futures.ThreadPoolExecutor(3)
    start = time.perf_counter()
    done = 0
    to_do = []
    for item in range(12):
        await s.acquire()
        to_do.append(asyncio.create_task(do(loop, pool, item)))
    # await to_wait
    print("custom thread pool", time.perf_counter() - start)

    # # 3. Run in a custom process pool:
    # with concurrent.futures.ProcessPoolExecutor() as pool:
    #     result = await loop.run_in_executor(pool, cpu_bound)
    #     print("custom process pool", result)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
