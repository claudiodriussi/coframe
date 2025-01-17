import coframe
import sys

print(sys.path)
app = coframe.app.Base.__app__
app.load_config("config.yaml")
app.load_plugins()
print(coframe.app.Base.__app__.config)
print(sys.path)
import plugins.library as library # noqa E402
library.test.ok()
generator = coframe.source.Generator(app)
generator.generate()
