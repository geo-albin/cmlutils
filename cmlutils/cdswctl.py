import logging
import os
import subprocess
import tarfile
import urllib.request
import uuid
from hashlib import sha256
from pathlib import Path
from sys import platform

from cmlutils import constants
from cmlutils.utils import download_file


def _get_cdswctl_download_url(host: str) -> str:
    base_url = urllib.parse.urljoin(host, "cli/")
    if platform == "linux" or platform == "linux2":
        final_url = urllib.parse.urljoin(base_url, "linux/amd64/cdsw.tar.gz")
    elif platform == "darwin":
        final_url = urllib.parse.urljoin(base_url, "darwin/amd64/cdsw.tar.gz")
    else:
        final_url = urllib.parse.urljoin(base_url, "windows/amd64/cdsw.zip")
    return final_url


def _download_and_extract(url: str, ca_path: str):
    file_name = url.split("/")[-1]
    dir_path = _cdswctl_tmp_dir_path()
    file_path = os.path.join(dir_path, file_name)
    download_file(url=url, filepath=file_path, ca_path=ca_path)
    if Path(constants.BASE_PATH_CDSWCTL) in Path(dir_path).parents:
        tf = tarfile.open(file_path)
        tf.extractall(dir_path)
        tf.close()
    else:
        raise RuntimeError("path for cdswctl could not be validated.")
    os.remove(file_path)
    cdswctldir = os.listdir(dir_path)[0]
    old_file_path = os.path.join(dir_path, cdswctldir, "cdswctl")
    new_file_path = os.path.join(dir_path, "cdswctl")
    os.rename(old_file_path, new_file_path)
    os.removedirs(os.path.join(dir_path, cdswctldir))
    return new_file_path


def _cdswctl_tmp_dir_path() -> str:
    subdir = uuid.uuid1().hex[:10]
    dirpath = os.path.join(constants.BASE_PATH_CDSWCTL, subdir)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    return dirpath


def obtain_cdswctl(host: str, ca_path: str) -> str:
    file_url = _get_cdswctl_download_url(host)
    expected_cdswctl_path = _download_and_extract(file_url, ca_path=ca_path)
    logging.info(
        "Expected cdsw path for cdswctl for file transfer %s", expected_cdswctl_path
    )
    return expected_cdswctl_path


def cdswctl_login(cdswctl_path: str, host: str, username: str, api_key: str, ca_path: str = ""):
    """
    Authenticate with CML workspace using cdswctl login command.

    Args:
        cdswctl_path: Path to the cdswctl binary
        host: CML workspace hostname/URL
        username: CML username
        api_key: CML API key for authentication
        ca_path: Path to CA certificate or "false" to skip SSL verification

    Returns:
        subprocess.CompletedProcess: The completed subprocess result

    Raises:
        Exception: If cdswctl login fails with non-zero return code

    Side Effects:
        - Executes cdswctl login command
        - Logs debug information about the login process
        - Logs error information if login fails
    """
    logging.info("Logging into cdsw via cdswctl")
    cmd = [cdswctl_path, "login", "-n", username, "-u", host, "-y", api_key]
    
    # Add insecure flag if SSL verification is disabled
    if ca_path.lower() == "false":
        cmd.append("--insecure-skip-verify")
        logging.debug("Added --insecure-skip-verify flag to cdswctl login command")
    
    # Log the command (masking the API key for security)
    masked_cmd = cmd.copy()
    api_key_index = masked_cmd.index(api_key) if api_key in masked_cmd else -1
    if api_key_index != -1:
        masked_cmd[api_key_index] = "<api_key>"
    logging.debug(f"cdswctl login command: {' '.join(masked_cmd)}")

    # Execute login with output capture
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    # Validate login succeeded
    if result.returncode != 0:
        logging.error(f"cdswctl login failed: {result.stderr}")
        raise Exception(f"cdswctl login failed with return code {result.returncode}")

    logging.info("Login succeeded")
    logging.debug(f"cdswctl login output: {result.stdout}")

    return result
