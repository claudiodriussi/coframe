# Coframe

**Coframe** is a plugin-based and data-driven framework designed to generate SQLAlchemy model source code and provide server-side infrastructure for database applications. It bridges the gap between database schema definition and application development with a flexible, extensible architecture.

## Status

**BETA SOFTWARE**: While functional for testing and development, Coframe is still under active development. API changes may occur, and comprehensive documentation is in progress.

## Key Features

- **Plugin Architecture**: Enables collaborative development where contributors can work independently following established patterns without conflicting with each other's code.

- **Data-Driven Design**: Database schemas are defined in YAML files, combining both technical specifications and semantic information. This approach separates structure from implementation while maintaining a single source of truth.

- **Rich Metadata**: Beyond basic database schema, plugins can define UI components, validation rules, menu structures, and other application-level concerns that drive both server and client behavior.

- **Schema Agnosticism**: Applications are constructed entirely from plugins. There are no required tables or mandatory configurations, giving you complete flexibility in defining your data model.

- **REST API Infrastructure**: The built-in command processor and endpoint system provide a standardized way to expose functionality via REST, with support for JWT authentication and context-based permissions.

- **Advanced Querying**: The querybuilder component provides a JSON-based query language that can be used from client applications to construct complex SQL queries safely.

## Installation

### Prerequisites

- Python 3.7 or higher
- Required dependencies (see requirements.txt)

### Setup

Clone the repository and set up a virtual environment:

```bash
git clone https://github.com/your-username/coframe.git
cd coframe
python -m venv venv # or python3 or py depends on your system
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

### Development Testing

The `devtest` directory contains examples to help you understand the framework:

1. Run the basic development test:
   ```bash
   cd devtest
   python devtest.py
   ```
   this will generate the `model.py` SQLAlchemy model and the `devtest.sqlite` database with some data

2. Start the Flask server:
   ```bash
   python flask-server.py
   ```

3. Open the Jupyter notebook to test API functionality:
   ```bash
   jupyter-lab server-test.ipynb
   ```

4. The querybuilder component is standalone and can work outside of
   the coframe package. You can test it with:
   ```bash
   cd querybuilder
   python query_examples.py
   ```

### Building Your App

1. Create your plugins in a dedicated directory
2. Define your data model in YAML files
3. Add custom code for business logic
4. Configure the system via `config.yaml`
5. Generate the model code and initialize the database
6. Start the server

## Architecture

Coframe consists of several key components:

- **Plugin Manager**: Loads and organizes plugin modules
- **DB Engine**: Manages SQLAlchemy models and database interactions
- **Command Processor**: Routes requests to the appropriate endpoint functions
- **Source Generator**: Creates SQLAlchemy model code from YAML definitions
- **Querybuilder**: Translates JSON query specifications to SQLAlchemy queries
- **Flask Server**: Provides REST API access to the system

## Extending Coframe

The system is designed to be extended through plugins. Each plugin can contain:

- YAML files defining data models
- Python modules with custom business logic
- Endpoint definitions for API access
- UI component specifications

## Web Framework Support

Currently, Coframe includes a Flask server integration. Future versions may support Django, FastAPI, and other Python web frameworks.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
