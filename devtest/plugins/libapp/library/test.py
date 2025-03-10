from coframe.endpoints import endpoint


def ok():
    print("ok")
    return True


@endpoint('sayhello')
def say_hello(data):
    name = data.get("name", "World")
    lang = data.get("lang", "en")

    if lang == "it":
        message = f"Ciao, '{name}'!"
    elif lang == "es":
        message = f"Hola, '{name}'!"
    else:
        message = f"Hello, '{name}'!"

    return {
        "status": "success",
        "data": message,
        "code": 200
    }
