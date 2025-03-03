# import coframe
# import sys
import yaml

import coframe.plugins

plugins = coframe.plugins.PluginsManager()
plugins.load_config("config.yaml")
plugins.load_plugins()

print("\nFinal Result:")
print(yaml.dump(plugins.data, sort_keys=False))

plugins.print_history()

print(plugins.export_pythonpath())
print(plugins.export_pythonpath(windows=True))

app = coframe.db.Base.__app__
app.calc_db(plugins)

import plugins.libapp.library as library # type: ignore # noqa E402
library.test.ok()

generator = coframe.source.Generator(app)
generator.generate(filename="model.py")


# # generator = coframe.modelgenerator.SQLAlchemyModelGenerator(app)
# # generator.generate_model_file("db")


# # print("\npythonpath:")
# # print(sys.path)

# # print(sys.path)
# # app = coframe.app.Base.__app__
# # app.load_config("config.yaml")
# # app.load_plugins()
# # print(coframe.app.Base.__app__.config)
# # print(sys.path)
# # import plugins.library as library # noqa E402
# # library.test.ok()
# # generator = coframe.source.Generator(app)
# # generator.generate()
