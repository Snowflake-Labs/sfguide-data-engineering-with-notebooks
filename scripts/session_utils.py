#------------------------------------------------------------------------------
# Hands-On Lab: Intro to Data Engineering with Notebooks
# Script:       session_utils.py
# Author:       Jeremiah Hansen
# Last Updated: 2/2/2026
#------------------------------------------------------------------------------


import os
from snowflake.snowpark import Session


def get_snowpark_session() -> Session:
    """
    Get or create a Snowpark session based on the execution context.
    
    Priority order:
    1. Active session (Snowflake Notebook environment)
    2. Environment variables (CI/CD pipelines)
    3. connections.toml (local development)
    
    Environment variables supported:
    - SNOWFLAKE_ACCOUNT (required for env var auth)
    - SNOWFLAKE_USER (required for env var auth)
    - SNOWFLAKE_PASSWORD (for password auth)
    - SNOWFLAKE_PRIVATE_KEY_PATH (for key-pair auth, alternative to password)
    - SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional, for encrypted private keys)
    - SNOWFLAKE_WAREHOUSE (optional)
    - SNOWFLAKE_DATABASE (optional)
    - SNOWFLAKE_SCHEMA (optional)
    - SNOWFLAKE_ROLE (optional)
    
    Returns:
        Session: A Snowpark session (never closed by this module)
    """
    
    # 1. Try to get active session (Snowflake Notebook environment)
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        print("Using active Snowflake session (notebook environment)")
        return session
    except Exception:
        pass  # Not in a notebook environment, continue to other methods
    
    # 2. Check for environment variables (CI/CD pipeline)
    if os.environ.get("SNOWFLAKE_ACCOUNT") and os.environ.get("SNOWFLAKE_USER"):
        print("Creating session from environment variables")
        return _create_session_from_env()
    
    # 3. Fall back to connections.toml (local development)
    print("Creating session from connections.toml")
    return Session.builder.getOrCreate()


def _create_session_from_env() -> Session:
    """
    Create a Snowpark session using environment variables.
    Supports both password and key-pair authentication.
    """
    connection_params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
    }
    
    # Authentication: prefer private key if available, otherwise use password
    # NOTE: Haven't tested key pair auth yet
    if os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
        # Key-pair authentication
        private_key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
        
        with open(private_key_path, "rb") as key_file:
            private_key_data = key_file.read()
        
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        # Handle encrypted vs unencrypted private keys
        passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        private_key = serialization.load_pem_private_key(
            private_key_data,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        connection_params["private_key"] = private_key_bytes
    else:
        # Password authentication
        connection_params["password"] = os.environ.get("SNOWFLAKE_PASSWORD", "")
    
    # Optional connection parameters
    optional_params = {
        "warehouse": "SNOWFLAKE_WAREHOUSE",
        "database": "SNOWFLAKE_DATABASE",
        "schema": "SNOWFLAKE_SCHEMA",
        "role": "SNOWFLAKE_ROLE",
    }
    
    for param_name, env_var in optional_params.items():
        value = os.environ.get(env_var)
        if value:
            connection_params[param_name] = value
    
    return Session.builder.configs(connection_params).create()
