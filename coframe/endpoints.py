import importlib.util
import threading
import time
import uuid
from functools import wraps
from typing import Dict, List, Any, Optional, Union, Callable
from pathlib import Path
import coframe

# Global dictionary to register endpoints
_ENDPOINTS: Dict[str, Callable] = {}


def endpoint(name: str) -> Callable[[Callable], Callable]:
    """
    Decorator to register an endpoint function.

    Args:
        name: The name of the endpoint for registration

    Returns:
        The decorated function with registration side effect
    """
    def decorator(func: Callable) -> Callable:
        _ENDPOINTS[name] = func

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)
        return wrapper
    return decorator


class CommandResult:
    """
    Class to represent the result of a command execution.

    This class provides a standardized format for command responses,
    including success/error status, data payload, and metadata.
    """
    def __init__(self,
                 status: str = "success",
                 data: Any = None,
                 message: Optional[str] = None,
                 request_id: Optional[str] = None,
                 code: int = 200) -> None:
        """
        Initialize a command result.

        Args:
            status: The status of the result ("success" or "error")
            data: The payload data (for success status)
            message: Error message (for error status)
            request_id: Unique identifier of the request
            code: Status code (similar to HTTP status codes)
        """
        self.status = status
        self.data = data
        self.message = message
        self.request_id = request_id
        self.code = code
        self.timestamp = int(time.time())

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the result to a dictionary.

        Returns:
            Dictionary representation of the command result
        """
        result = {
            "status": self.status,
            "code": self.code,
            "timestamp": self.timestamp,
        }

        if self.request_id:
            result["request_id"] = self.request_id

        if self.status == "success":
            result["data"] = self.data
        else:
            result["message"] = self.message or "Unknown error"

        return result

    def to_json(self) -> str:
        """
        Convert the result to a JSON string.

        Returns:
            JSON string representation of the command result
        """
        import json
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CommandResult':
        """
        Create a result from a dictionary.

        Args:
            data: Dictionary containing command result data

        Returns:
            A new CommandResult instance
        """
        return cls(status=data.get("status", "success"),
                   data=data.get("data"),
                   message=data.get("message"),
                   request_id=data.get("request_id"),
                   code=data.get("code", 200))

    @classmethod
    def from_json(cls, json_str: str) -> 'CommandResult':
        """
        Create a result from a JSON string.

        Args:
            json_str: JSON string containing command result data

        Returns:
            A new CommandResult instance
        """
        import json
        data = json.loads(json_str)
        return cls.from_dict(data)


class Command:
    """
    Class to represent a command to be processed.

    This class encapsulates all information needed to execute a command,
    including operation name, parameters, execution metadata, and authentication context.
    """
    def __init__(self,
                 operation: str,
                 parameters: Optional[Dict[str, Any]] = None,
                 request_id: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None,
                 version: str = "1.0",
                 depends_on: Optional[Union[str, List[str]]] = None,
                 timeout: int = 30) -> None:
        """
        Initialize a command.

        Args:
            operation: The name of the operation to execute
            parameters: Parameters to pass to the operation
            request_id: Unique identifier for the command (auto-generated if None)
            context: Execution context (tenant, user, permissions, etc.)
            version: API version string
            depends_on: Request ID(s) that must complete before this command can execute
            timeout: Maximum execution time in seconds
        """
        self.operation = operation
        self.parameters = parameters or {}
        self.request_id = request_id or str(uuid.uuid4())
        self.context = context or {}
        self.version = version
        self.depends_on = depends_on if isinstance(depends_on, list) or depends_on is None else [depends_on]
        self.timeout = timeout
        self.result: Optional[CommandResult] = None
        self.completed: threading.Event = threading.Event()
        self.started: bool = False

    @classmethod
    def from_json(cls, json_str: str) -> 'Command':
        """
        Create a command from a JSON string.

        Args:
            json_str: JSON string containing command data

        Returns:
            A new Command instance
        """
        import json
        data = json.loads(json_str)
        return cls.from_dict(data)

    def to_json(self) -> str:
        """
        Convert the command to a JSON string.

        Returns:
            JSON string representation of the command
        """
        import json
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Command':
        """
        Create a command from a dictionary.
        """
        return cls(operation=data.get("operation", ""),
                   parameters=data.get("parameters", {}),
                   request_id=data.get("request_id"),
                   context=data.get("context"),
                   version=data.get("version", "1.0"),
                   depends_on=data.get("depends_on"),
                   timeout=data.get("timeout", 30))

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the command to a dictionary.
        """
        return {
            "operation": self.operation,
            "parameters": self.parameters,
            "request_id": self.request_id,
            "context": self.context,
            "version": self.version,
            "depends_on": self.depends_on,
            "timeout": self.timeout
        }


class CommandProcessor:
    """
    Processes commands by routing them to registered endpoints.

    This class manages command execution, dependencies, and results,
    allowing for asynchronous execution with proper sequencing.
    """

    def __init__(self) -> None:
        """
        Initialize the command processor.
        """
        self.endpoints: Dict[str, Callable] = {}
        self.results: Dict[str, CommandResult] = {}
        self.pending_commands: Dict[str, Command] = {}
        self.command_lock: threading.Lock = threading.Lock()

    def resolve_endpoints(self, file_paths: List[Union[str, Path]]) -> None:
        """
        Load Python files and register their endpoints.

        Args:
            file_paths: List of paths to Python files containing endpoint definitions
        """
        import sys
        from pathlib import Path

        for file_path in file_paths:
            # Convert to Path if it's a string
            path = Path(file_path) if isinstance(file_path, str) else file_path

            if not path.exists():
                # print(f"Error: file {path} does not exist.")
                continue

            # Determine package from file path
            dir_path = path.parent
            parent_dir = dir_path.parent

            # Add parent directory to sys.path temporarily
            sys_path_modified = False
            parent_str = str(parent_dir)
            if parent_str not in sys.path:
                sys.path.insert(0, parent_str)
                sys_path_modified = True

            try:
                # Calculate module name considering package structure
                module_name = path.stem
                package_name = dir_path.name
                full_module_name = f"{package_name}.{module_name}"

                # Load the module specifying the package
                spec = importlib.util.spec_from_file_location(
                    full_module_name,
                    str(path),
                    submodule_search_locations=[str(dir_path)]
                )

                if spec is None:
                    # print(f"Error: unable to create spec for {path}")
                    continue

                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module  # Important for relative imports
                spec.loader.exec_module(module)

                # Add all endpoints found to the endpoints dictionary
                self.endpoints.update(_ENDPOINTS.copy())

                # print(f"Loaded {len(_ENDPOINTS)} endpoints from file {path}")

            except Exception as e:
                print(f"Error loading module {path}: {e}")
                import traceback
                traceback.print_exc()

            finally:
                # Remove the added path if necessary
                if sys_path_modified:
                    sys.path.remove(parent_str)

    def _execute_command(self, command: Command) -> None:
        """
        Execute a command in a separate thread.

        Args:
            command: The command to execute
        """
        if command.operation not in self.endpoints:
            result = CommandResult(status="error",
                                   message=f"Operation '{command.operation}' not found",
                                   request_id=command.request_id,
                                   code=404)
        else:
            try:
                # set the context before executing the function
                coframe.db.BaseApp.set_context(command.context)

                # Execute the endpoint function
                func = self.endpoints[command.operation]
                start_time = time.time()

                # Set a timer for timeout
                timer = None
                timeout_event = threading.Event()

                if command.timeout > 0:
                    def timeout_handler() -> None:
                        if not timeout_event.is_set():
                            timeout_event.set()
                            # Interrupt the thread? In Python this is complicated,
                            # so here we just signal that a timeout has occurred

                    timer = threading.Timer(command.timeout, timeout_handler)
                    timer.daemon = True
                    timer.start()

                # Execute the function
                try:
                    result_data = func(command.parameters)
                    elapsed = time.time() - start_time

                    # Stop the timeout timer
                    if timer:
                        timer.cancel()
                        timeout_event.set()

                    # Check if execution time exceeded timeout
                    if command.timeout > 0 and elapsed > command.timeout:
                        result = CommandResult(
                            status="error",
                            message=f"Execution timeout exceeded ({elapsed:.2f}s > {command.timeout}s)",
                            request_id=command.request_id,
                            code=408
                        )
                    else:
                        # Create a standardized result
                        if isinstance(result_data, dict) and "status" in result_data:
                            result = CommandResult(
                                status=result_data.get("status"),
                                data=result_data.get("data"),
                                message=result_data.get("message"),
                                request_id=command.request_id,
                                code=result_data.get("code", 200)
                            )
                        else:
                            result = CommandResult(
                                status="success",
                                data=result_data,
                                request_id=command.request_id
                            )
                except Exception as e:
                    # Stop the timeout timer if still active
                    if timer:
                        timer.cancel()

                    result = CommandResult(
                        status="error",
                        message=str(e),
                        request_id=command.request_id,
                        code=500
                    )

            except Exception as e:
                result = CommandResult(status="error",
                                       message=str(e),
                                       request_id=command.request_id,
                                       code=500)

        # Save the result
        with self.command_lock:
            self.results[command.request_id] = result
            command.result = result
            command.completed.set()

            # Remove the command from the pending list
            if command.request_id in self.pending_commands:
                del self.pending_commands[command.request_id]

            # Check if there are pending commands that depend on this one
            self._check_dependent_commands(command.request_id)

    def _check_dependent_commands(self, completed_request_id: str) -> None:
        """
        Check if there are pending commands that can now be executed.

        Args:
            completed_request_id: ID of the command that just completed
        """
        commands_to_execute = []

        for cmd_id, cmd in list(self.pending_commands.items()):
            if not cmd.depends_on:
                continue

            # Check if all dependencies have been completed
            all_deps_completed = True
            for dep_id in cmd.depends_on:
                if dep_id not in self.results:
                    all_deps_completed = False
                    break

            if all_deps_completed:
                commands_to_execute.append(cmd)
                del self.pending_commands[cmd_id]

        # Execute ready commands
        for cmd in commands_to_execute:
            self._start_command_thread(cmd)

    def _start_command_thread(self, command: Command) -> None:
        """
        Start a thread to execute the command.

        Args:
            command: The command to execute
        """
        command.started = True
        thread = threading.Thread(target=self._execute_command, args=(command,))
        thread.daemon = True
        thread.start()

    def send(self, command_dict: Dict[str, Any], wait: bool = True) -> Optional[Dict[str, Any]]:
        """
        Send a command for execution.
        If wait=True, waits for completion and returns the result.
        If wait=False, only starts the thread and returns None.

        Args:
            command_dict: Dictionary representation of the command
            wait: Whether to wait for the command to complete

        Returns:
            Dictionary with result if wait=True, or dictionary with request_id if wait=False
        """
        command = Command.from_dict(command_dict)

        with self.command_lock:
            # Check if all dependencies have been completed
            can_execute = True
            if command.depends_on:
                for dep_id in command.depends_on:
                    if dep_id not in self.results:
                        can_execute = False
                        break

            self.pending_commands[command.request_id] = command
            if can_execute:
                self._start_command_thread(command)

        if wait:
            return self.wait_for_result(command.request_id)
        return {"request_id": command.request_id}

    def wait_for_result(self, request_id: Optional[str] = None, timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        Wait for a result.
        If request_id is specified, waits for the specific result of that command.

        Args:
            request_id: ID of the specific command to wait for, or None for any result
            timeout: Maximum time to wait in seconds

        Returns:
            Dictionary with command result
        """
        if request_id is None:
            # Wait for any result
            while not self.results:
                time.sleep(0.1)
            latest_result = list(self.results.values())[-1].to_dict()

            # Remove the result from cache if explicitly requested without ID
            with self.command_lock:
                latest_request_id = latest_result.get("request_id")
                if latest_request_id and latest_request_id in self.results:
                    del self.results[latest_request_id]

            return latest_result

        with self.command_lock:
            # If the result is already available, return it immediately
            if request_id in self.results:
                result = self.results[request_id].to_dict()
                # Remove the result from cache after returning it
                del self.results[request_id]
                return result

            # Check if the command exists
            if request_id not in self.pending_commands:
                return CommandResult(status="error",
                                     message=f"Command with request_id '{request_id}' not found",
                                     code=404).to_dict()

            # Get the command
            command = self.pending_commands[request_id]

        # Use the command's timeout if not specified
        if timeout is None:
            timeout = command.timeout

        # Wait for the command to complete
        success = command.completed.wait(timeout)

        if not success:
            # Timeout expired
            return CommandResult(status="error",
                                 message=f"Timeout expired after {timeout} seconds",
                                 request_id=request_id,
                                 code=408).to_dict()

        # Get the result and remove it from cache
        with self.command_lock:
            if request_id in self.results:
                result = self.results[request_id].to_dict()
                del self.results[request_id]
                return result
            else:
                # This should never happen, but just in case...
                return command.result.to_dict() if command.result else CommandResult(
                    status="error",
                    message="Result not found after completion",
                    request_id=request_id,
                    code=500
                ).to_dict()
