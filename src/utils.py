async def safe_iter(aiter):
    it = aiter.__aiter__()
    while True:
        try:
            yield await it.__anext__()
        except StopAsyncIteration:
            break
        except AttributeError:
            continue
