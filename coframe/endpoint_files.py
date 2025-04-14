import os
import json
import yaml
import base64
import mimetypes
import csv
from io import StringIO
from pathlib import Path
from typing import Dict, Any, List
import coframe
from coframe.endpoints import endpoint


@endpoint('read_file')
def read_file(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generic endpoint for reading files of various formats.

    This endpoint allows clients to read files from server-side directories that are
    configured as allowed in the Coframe configuration. It supports multiple file
    formats including structured data (JSON, YAML, CSV) and binary files.

    Parameters:
        - file_path: Path to the file to be read (relative or absolute)
        - base_dir: Base directory (optional, used for relative paths)
        - format: File format ('auto', 'json', 'yaml', 'csv', 'text', 'binary')
          If 'auto' is specified (default), the format will be determined from the file extension
        - binary_encoding: For binary files, encoding type ('base64', 'hex')
        - csv_options: Options for CSV parsing:
          - delimiter: Character used as field separator (default: ',')
          - has_header: Whether CSV has a header row (default: True)

    Returns:
        Dictionary with:
        - status: 'success' or 'error'
        - data: Parsed file content
        - file_info: Metadata about the file (name, path, type, etc.)
        - code: HTTP status code

    Configuration in config.yaml:
        read_files:
          allowed_dirs: List of allowed directories (relative or absolute paths)
          text_suffix: List of file extensions to be treated as text

    Examples:
        # Reading a JSON configuration file
        {
            "operation": "read_file",
            "parameters": {
                "file_path": "configs/app_settings.json"
            }
        }

        # Reading a CSV file with custom delimiter
        {
            "operation": "read_file",
            "parameters": {
                "file_path": "data/users.csv",
                "csv_options": {
                    "delimiter": ";",
                    "has_header": true
                }
            }
        }

        # Reading an image file (binary)
        {
            "operation": "read_file",
            "parameters": {
                "file_path": "images/logo.png",
                "binary_encoding": "base64"
            }
        }

        # Specifying a base directory
        {
            "operation": "read_file",
            "parameters": {
                "base_dir": "/var/www/html",
                "file_path": "assets/styles.css"
            }
        }
    """
    try:
        # Extract and validate file path
        file_path = data.get('file_path')
        if not file_path:
            return {"status": "error", "message": "File path is required", "code": 400}

        # Get Coframe configuration
        app = coframe.utils.get_app()
        config = app.pm.config if hasattr(app, 'pm') else {}

        # Get file reading configuration
        file_config = config.get('read_files', {})
        allowed_dirs = file_config.get('allowed_dirs', [])
        text_suffixes = file_config.get('text_suffix', ['.txt', '.md', '.xml', '.html', '.css', '.js'])

        # Expand home directory in allowed_dirs if present (e.g., ~/resources)
        allowed_dirs = [os.path.expanduser(d) for d in allowed_dirs]

        # Make absolute paths from relative ones
        allowed_dirs = [str(Path(d).absolute()) if not os.path.isabs(d) else d for d in allowed_dirs]

        # Determine the base directory
        base_dir = data.get('base_dir')
        if base_dir:
            base_dir = os.path.expanduser(base_dir)  # Expand ~ if present
            full_path = os.path.join(base_dir, file_path)
        else:
            # If not specified, use the path as is
            full_path = file_path

        # Make sure the path is safe
        path = Path(os.path.expanduser(full_path)).resolve()

        # Verify that the path is within an allowed directory
        if allowed_dirs and not is_path_allowed(path, allowed_dirs):
            return {
                "status": "error",
                "message": f"Access to this directory is not allowed. Allowed directories: {', '.join(allowed_dirs)}",
                "code": 403
            }

        # Check if the file exists
        if not path.exists() or not path.is_file():
            return {"status": "error", "message": f"File not found: {file_path}", "code": 404}

        # Determine the format
        format_type = data.get('format', 'auto').lower()
        if format_type == 'auto':
            suffix = path.suffix.lower()
            if suffix in ['.yaml', '.yml']:
                format_type = 'yaml'
            elif suffix == '.json':
                format_type = 'json'
            elif suffix == '.csv':
                format_type = 'csv'
            elif suffix in text_suffixes:
                format_type = 'text'
            else:
                # Assume it's a binary file
                format_type = 'binary'

        # Read and parse the file
        if format_type in ['yaml', 'json', 'text', 'csv']:
            # Read as text
            with open(path, 'r', encoding='utf-8') as f:
                file_content = f.read()

                if format_type == 'yaml':
                    parsed_data = yaml.safe_load(file_content)
                elif format_type == 'json':
                    parsed_data = json.loads(file_content)
                elif format_type == 'csv':
                    # Get CSV parsing options
                    csv_options = data.get('csv_options', {})
                    delimiter = csv_options.get('delimiter', ',')
                    has_header = csv_options.get('has_header', True)

                    # Parse CSV
                    csv_data = []
                    csv_file = StringIO(file_content)

                    if has_header:
                        # Parse as list of dictionaries (with headers)
                        reader = csv.DictReader(csv_file, delimiter=delimiter)
                        csv_data = list(reader)
                    else:
                        # Parse as list of lists (no headers)
                        reader = csv.reader(csv_file, delimiter=delimiter)
                        csv_data = list(reader)

                    parsed_data = csv_data
                else:  # text
                    parsed_data = file_content
        else:  # binary
            # Read as binary
            with open(path, 'rb') as f:
                binary_content = f.read()

                # Determine MIME type
                mime_type, _ = mimetypes.guess_type(str(path))
                if not mime_type:
                    mime_type = 'application/octet-stream'

                # Encode in base64 or hex
                binary_encoding = data.get('binary_encoding', 'base64')
                if binary_encoding == 'base64':
                    encoded_content = base64.b64encode(binary_content).decode('ascii')
                elif binary_encoding == 'hex':
                    encoded_content = binary_content.hex()
                else:
                    return {"status": "error",
                            "message": f"Unsupported binary encoding: {binary_encoding}", "code": 400}

                parsed_data = {
                    "content": encoded_content,
                    "mime_type": mime_type,
                    "encoding": binary_encoding,
                    "size": len(binary_content)
                }

        # Add file metadata
        try:
            relative_path = path.relative_to(Path.cwd())
        except ValueError:
            relative_path = path

        file_info = {
            "filename": path.name,
            "extension": path.suffix,
            "content_type": mimetypes.guess_type(str(path))[0] or "application/octet-stream",
            "path": str(relative_path),
            "format": format_type,
            "last_modified": os.path.getmtime(path),
            "size": os.path.getsize(path)
        }

        return {
            "status": "success",
            "data": parsed_data,
            "file_info": file_info,
            "code": 200
        }

    except yaml.YAMLError as e:
        return {"status": "error", "message": f"YAML parsing error: {str(e)}", "code": 400}
    except json.JSONDecodeError as e:
        return {"status": "error", "message": f"JSON parsing error: {str(e)}", "code": 400}
    except csv.Error as e:
        return {"status": "error", "message": f"CSV parsing error: {str(e)}", "code": 400}
    except UnicodeDecodeError:
        return {"status": "error", "message": "File appears to be binary, but was requested as text", "code": 400}
    except PermissionError:
        return {"status": "error", "message": f"Permission denied reading file: {file_path}", "code": 403}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e), "code": 500}


def is_path_allowed(path: Path, allowed_dirs: List[str]) -> bool:
    """
    Check if a path is within allowed directories.

    Args:
        path: Path to check
        allowed_dirs: List of allowed directories

    Returns:
        True if the path is allowed, False otherwise
    """
    path_str = str(path.absolute())

    for allowed_dir in allowed_dirs:
        allowed_path = Path(allowed_dir).resolve()
        if path_str.startswith(str(allowed_path)):
            return True

    return False
