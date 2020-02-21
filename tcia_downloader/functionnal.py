from functools import update_wrapper


def make_coroutine(func):
    def coroutine(target, *args, **kwargs):
        while True:
            try:
                arg = yield
                result = func(arg, *args, **kwargs)
                target.send(result)
            except GeneratorExit:
                target.close()
                break

    def init(target, *args, **kwargs):
        gen = coroutine(target, *args, **kwargs)
        gen.__name__ = func.__name__
        next(gen)
        return gen

    update_wrapper(init, func)

    return init


def make_sink(func):
    def sink():
        while True:
            try:
                arg = yield
                func(arg)
            except GeneratorExit:
                break

    # initialize
    result = sink()
    result.send(None)
    return result


def make_source(iterable_):
    def source(target):
        for thing in iterable_:
            target.send(thing)
        target.close()

    return source


def run_pipeline(iterable_, ops, autoclose=True):
    pipeline = chain(*ops)
    first_target = pipeline[0]
    for item in iterable_:
        first_target.send(item)
    if autoclose:
        first_target.close()


def chain(*funcs):
    coroutines = [make_coroutine(func) for func in funcs[:-1]]
    print(coroutines)
    coroutines.reverse()
    sink = make_sink(funcs[-1])
    targets = list()  # keep for later
    for i, coro in enumerate(
        coroutines, 0
    ):  # loop from end to beginning because reverse!
        if i == 0:  # last is glued to sink
            target = coro(sink)
            targets.append(target)
        else:  # others have to glue with initialized targets
            target = coro(targets[i - 1])
            targets.append(target)
    targets.reverse()  # get back the right order
    print(targets)
    targets.append(sink)
    return targets


def with_context(func, context):
    def wrapper(func, *args, **kwargs):
        gen_maker = make_coroutine(func)
        while True:
            try:
                with context as ctx:
                    gen = gen_maker(ctx, *args, **kwargs)
                    yield from gen
            except GeneratorExit:
                gen.close()
                break

    update_wrapper(wrapper, func)

    return wrapper
