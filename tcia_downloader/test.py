def readfile(file):
    line = 1
    return line


async def make_url(proc):
    line = await proc()
    url = "http" + line
    return url


async def download_zip(urlget):
    url = await urlget()


def pipe(source, *funcs):
    result = source
    for func in funcs:
        result = func(result)
    return result
