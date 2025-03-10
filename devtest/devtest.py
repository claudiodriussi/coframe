import coframe.plugins
from coframe.endpoints import endpoint, CommandProcessor


@endpoint('add')
def add_numbers(data):
    a = data.get("a", 0)
    b = data.get("b", 0)

    return {
        "status": "success",
        "data": a + b
    }


if __name__ == "__main__":
    plugins = coframe.plugins.PluginsManager()
    plugins.load_config("config.yaml")
    plugins.load_plugins()

    print(plugins.export_pythonpath())

    app = coframe.db.Base.__app__
    app.calc_db(plugins)

    model_file = "model.py"
    if plugins.should_regenerate(model_file):
        print("Require regeneration.")
        generator = coframe.source.Generator(app)
        generator.generate(filename=model_file)
    else:
        print("No regeneration required.")

    import plugins.libapp.library as library  # type: ignore
    library.test.ok()

    import model  # type: ignore
    engine = model.initialize_db('sqlite:///:memory:')

    cp = CommandProcessor()
    cp.resolve_endpoints(["devtest.py"])
    sources = plugins.get_sources()
    cp.resolve_endpoints(sources)

    command = {
        "operation": "add",
        "parameters": {"a": 5, "b": 3},
        "request_id": "cmd-2"
    }
    result = cp.send(command)
    print(result)

    command = {
        "operation": "sayhello",
        "parameters": {"name": "Claudio", "lang": "en"},
        "request_id": "cmd-1",
        "timeout": 5
    }
    result = cp.send(command)
    print(result)
