# Coframe

**Coframe** is a plugin-based and data-driven framework designed to generate SQLAlchemy model source code. It is also suitable for use as a server for database client applications.

**PLEASE NOTE: THIS SOFTWARE IS IN A VERY EARLY STAGE OF DEVELOPMENT**

### Key Characteristics:

- **Plugin-based**: Many successful open-source projects are plugin-based, allowing contributors to collaborate while following established rules without interfering with each other's work. Our goal is to enable developers to create such plugins easily.

- **Data-driven**: Tables, fields, and field types are declared in `.yaml` files. These files can include attributes used to generate model source code, as well as semantic information that can be useful for client application development. Plugins also contain Python source code files, which are automatically imported into SQLAlchemy models and made available wherever needed.

- **Rich data information**: Plugins will not only include information about database tables but also additional data such as form generation details, menu structures, and other elements useful for both client and server applications.

- **DB-agnostic**: Applications developed with Coframe are built entirely from plugins. There are no default tables or configurations, although we provide some best practices, which are treated like any other plugin.

- **Client-agnostic**: The system will include data that client applications can use to build their interfaces. Currently, our focus is on server-side development, so client-specific details are not yet fully defined.

- **Web server-agnostic**: While most clients will likely be web-based, this is not always the case. Additionally, there may be multiple web servers, not all of which are Python-based. For this reason, the system will have its own session manager, separate from the one used by the web server.

## Installation
...

## Usage
...

## License
MIT License