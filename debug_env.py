from config import Config
import os

print("=== Current Configuration Debug ===")
print()

# Check if .env file exists
print("📁 .env file check:")
if os.path.exists('.env'):
    print("✅ .env file exists")
    try:
        with open('.env', 'r') as f:
            content = f.read()
            rabbitmq_lines = [line.strip() for line in content.split('\n') 
                            if 'RABBITMQ' in line and not line.strip().startswith('#') and line.strip()]
            if rabbitmq_lines:
                print("🔧 RabbitMQ settings in .env:")
                for line in rabbitmq_lines:
                    # Hide password for security
                    if 'PASS' in line:
                        parts = line.split('=')
                        if len(parts) >= 2:
                            print(f"  {parts[0]}={'*' * len(''.join(parts[1:]))}")
                        else:
                            print(f"  {line}")
                    else:
                        print(f"  {line}")
            else:
                print("❌ No RabbitMQ settings found in .env file")
    except Exception as e:
        print(f"❌ Error reading .env file: {e}")
else:
    print("❌ .env file does not exist in current directory")

print()

# Show environment variables
print("🌍 Environment Variables:")
env_vars = ['RABBITMQ_HOST', 'RABBITMQ_PORT', 'RABBITMQ_USER', 'RABBITMQ_PASS', 'RABBITMQ_PROCESSING_QUEUE']
for var in env_vars:
    value = os.environ.get(var)
    if value:
        if 'PASS' in var:
            print(f"  {var}: {'*' * len(value)}")
        else:
            print(f"  {var}: {value}")
    else:
        print(f"  {var}: NOT SET")

print()

# Show Config class values
print("⚙️  Config Class Values:")
print(f"  RABBITMQ_HOST: {Config.RABBITMQ_HOST}")
print(f"  RABBITMQ_PORT: {Config.RABBITMQ_PORT}")
print(f"  RABBITMQ_USER: {Config.RABBITMQ_USER}")
print(f"  RABBITMQ_PASS: {'*' * len(Config.RABBITMQ_PASS) if Config.RABBITMQ_PASS else 'NOT SET'}")
print(f"  RABBITMQ_PROCESSING_QUEUE: {Config.RABBITMQ_PROCESSING_QUEUE}")

print()

# Port analysis
print("🔍 Port Analysis:")
if Config.RABBITMQ_PORT == 15672:
    print("  ❌ WARNING: You're using port 15672 (Management UI port)")
    print("  ⚠️  This port is for web browser access, not AMQP messaging")
    print("  ✅ You should use port 5672 for AMQP messaging")
elif Config.RABBITMQ_PORT == 5672:
    print("  ✅ Using port 5672 (correct AMQP port)")
else:
    print(f"  ⚠️  Using non-standard port: {Config.RABBITMQ_PORT}")
    print("  🔍 This might be correct if your RabbitMQ is configured differently")

print()
print("=" * 50) 