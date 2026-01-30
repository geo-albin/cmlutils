import logging
import signal
import subprocess
import time


def open_ssh_endpoint(
    cdswctl_path: str, project_name: str, runtime_id: int, project_slug: str
) -> tuple[subprocess.Popen, int]:
    """
    Open an SSH endpoint for a CML project using cdswctl.

    Args:
        cdswctl_path: Path to the cdswctl binary
        project_name: Human-readable name of the project
        runtime_id: Runtime ID to use (-1 for default)
        project_slug: Project slug identifier (e.g., burj-h993-hp74-y3z5)

    Returns:
        tuple[subprocess.Popen, int]: A tuple containing the SSH process and port number

    Raises:
        Exception: If SSH connection fails unexpectedly

    Side Effects:
        - Creates an SSH endpoint subprocess using cdswctl
        - Logs detailed debug information about the SSH command
        - Logs error information if connection fails
    """
    command = [
        cdswctl_path,
        "ssh-endpoint",
        "-p",
        project_slug,
        "-c",
        "1.0",
        "-m",
        "0.5",
    ]
    if runtime_id != -1:
        command.append("-r")
        command.append(str(runtime_id))

    # Log detailed information before opening SSH endpoint
    logging.info("Creating SSH connection")
    logging.debug(f"Opening SSH endpoint for project: {project_slug}")
    logging.debug(f"Project name: {project_name}")
    logging.debug(f"Using cdswctl path: {cdswctl_path}")
    logging.debug(f"Runtime ID: {runtime_id if runtime_id != -1 else 'default'}")
    logging.debug(f"SSH command: {' '.join(command)}")

    ssh_call = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    logging.info("Waiting for SSH connection")
    line = ssh_call.stdout.readline()
    if line == "" or line == None:
        error = ssh_call.stderr.readlines()
        logging.error(f"SSH endpoint failed to start - no output received")
        logging.error(f"stderr output: {error}")
        return None, -1
    else:
        # Strip the line to remove trailing newline/whitespace before parsing
        line = line.strip()
        arr = line.split(" ")
        if len(arr) <= 3 or (not arr[3].isdigit()):
            stderr_lines = ssh_call.stderr.readlines()
            logging.error(f"SSH connection failed unexpectedly: {line}")
            logging.error(f"stderr output: {stderr_lines}")
            logging.debug(f"Parsed output array: {arr}")
            logging.debug(f"Expected format: output line with port number at index 3 (e.g., 'word word word PORT')")
            raise Exception(f"SSH connection failed unexpectedly: {line}")
        logging.info("SSH connection successfull")
        port_number = int(arr[3])
        logging.debug(f"SSH endpoint established on port: {port_number}")
        return ssh_call, port_number
