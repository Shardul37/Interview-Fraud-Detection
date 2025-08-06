# secrets_manager.py (modified)
import os
from dotenv import load_dotenv
from google.cloud import secretmanager

# Removed update_env_file function as we don't want to modify .env from within the container.
# We want to load into os.environ directly.

# Load initial environment variables from a local .env for development config (like PROJECT_ID, ENV)
# Important: Ensure this .env has GCP_PROJECT_ID and ENV="stag" for local testing.
load_dotenv(override=True)

# Initialize environment variables (these must be present in the .env used to run this script)
ENV = os.getenv("ENV", "stag") # Default to 'stag' if not set, important for secret names
PROJECT_ID = os.getenv("GCP_PROJECT_ID") # Use GCP_PROJECT_ID from your .env/config

# Initialize the Secret Manager client
try:
    if os.getenv("K_SERVICE"): # This checks if running in Google Cloud Run/Functions
        client = secretmanager.SecretManagerServiceClient() # Uses default credentials (e.g., service account assigned to Cloud Run service)
        print("Initialized Secret Manager client for Cloud Run.")
    else:
        # For local Docker testing, use the keyfile.json
        client = secretmanager.SecretManagerServiceClient.from_service_account_file(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        print("Initialized Secret Manager client from keyfile.json.")
except Exception as error:
    print(f"Error initializing Secret Manager client: {error}")
    # Re-raise to prevent app from starting without secrets if local auth fails
    raise

# Define environment-independent keys (if any, like GROQ_API_KEY)
# For this task, we're focusing on dependent keys
env_independent_keys = [
    # "GROQ_API_KEY", # Example
]

# Define environment-dependent keys
# These match the secret names in Secret Manager (e.g., stag_DATABASE_URL)
env_dependent_keys = [
    f'{ENV}_DATABASE_URL',
    f'{ENV}_RABBITMQ_URL', # Add the RabbitMQ URL
]

sw_keys = [] # Keeping this empty as it doesn't seem directly relevant to your current secrets

def get_secret_for_key(key, is_env_dependent):
    try:
        project_id = PROJECT_ID

        print(f'Fetching secret for key: {key}, Project ID: {project_id}')

        if not project_id:
            raise ValueError('GCP_PROJECT_ID is empty')

        # Secret names are directly the 'key' itself in this setup (e.g., stag_DATABASE_URL)
        secret_name = key

        # Secret path in GCP Secret Manager
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

        # Access the secret from GCP Secret Manager
        response = client.access_secret_version(name=secret_path)

        payload = response.payload.data.decode('utf-8')
        return payload # Return just the payload string

    except Exception as error:
        print(f"ERROR: get_secret_for_key -> {key}: {error}")
        return None

def secrets_manager():
    print("Starting secrets_manager to load secrets...")
    secret_list = []

    # Handle environment-independent keys (if any)
    for key in env_independent_keys:
        secret_value = get_secret_for_key(key, is_env_dependent=False)
        if secret_value is not None:
            os.environ[key] = secret_value
            secret_list.append(key)
            #print(f"DEBUG: Loaded secret into environment: {key}={os.environ[key][:15]}...") # Print first 15 chars for security

    # Handle environment-dependent keys
    for key in env_dependent_keys:
        secret_value = get_secret_for_key(key, is_env_dependent=True)
        if secret_value is not None:
            os.environ[key] = secret_value
            secret_list.append(key)
            #print(f"DEBUG: Loaded secret into environment: {key}={os.environ[key]}...") # Print first 15 chars for security

    for key in sw_keys: # This loop might not be needed if sw_keys is always empty
        secret_value = get_secret_for_key(key, is_env_dependent=False) # Changed to is_env_dependent=False based on name, confirm with manager
        if secret_value is not None:
            os.environ[key] = secret_value
            secret_list.append(key)

    print(f"Secrets loaded into environment: {secret_list}")

# Call the main function to invoke secrets_manager
if __name__ == "__main__":
    secrets_manager()