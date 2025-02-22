from dotenv import load_dotenv
import os
import subprocess
import toml

def update_r2r_config():
    # Read the current r2r.toml
    with open('r2r.toml', 'r') as f:
        config = toml.load(f)
    
    # Update database configuration from environment variables
    config['database'].update({
        'user': os.getenv('R2R_POSTGRES_USER'),
        'password': os.getenv('R2R_POSTGRES_PASSWORD'),
        'host': os.getenv('R2R_POSTGRES_HOST'),
        'port': int(os.getenv('R2R_POSTGRES_PORT', '5432')),
        'db_name': os.getenv('R2R_POSTGRES_DBNAME'),
        'project_name': os.getenv('R2R_PROJECT_NAME')
    })
    
    # Write the updated configuration back
    with open('r2r.toml', 'w') as f:
        toml.dump(config, f)

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Update r2r.toml with environment variables
    update_r2r_config()
    
    # Start the R2R server
    subprocess.run(["python", "-m", "r2r.serve"])

if __name__ == "__main__":
    main() 