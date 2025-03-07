import coframe.plugins

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
