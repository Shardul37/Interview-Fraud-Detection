import socket
import sys

def test_port(host, port, timeout=5):
    """Test if a port is accessible"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✅ Port {port} is accessible on {host}")
            return True
        else:
            print(f"❌ Port {port} is NOT accessible on {host}")
            return False
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed for {host}: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing port: {e}")
        return False

def test_rabbitmq_connection_with_port(host, port, user, password):
    """Test actual RabbitMQ connection on a specific port"""
    try:
        import pika
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=pika.PlainCredentials(user, password),
                connection_attempts=1,
                retry_delay=1,
                socket_timeout=5
            )
        )
        print(f"✅ RabbitMQ AMQP connection successful on port {port}")
        connection.close()
        return True
    except pika.exceptions.AMQPConnectionError as e:
        print(f"❌ AMQP Connection failed on port {port}: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error on port {port}: {e}")
        return False

if __name__ == "__main__":
    host = "34.170.213.229"
    
    # Common RabbitMQ ports
    ports_to_test = [
        (5672, "AMQP (default)"),
        (5671, "AMQPS (SSL)"),  
        (15672, "Management UI"),
        (25672, "Inter-node communication"),
        (35672, "CLI tools"),
        (61613, "STOMP"),
        (1883, "MQTT"),
        (15674, "Web STOMP"),
        (15675, "Web MQTT")
    ]
    
    print(f"Testing port accessibility on {host}...")
    print("=" * 50)
    
    accessible_ports = []
    for port, description in ports_to_test:
        print(f"Testing {port} ({description})...")
        if test_port(host, port):
            accessible_ports.append(port)
        print()
    
    print("=" * 50)
    print(f"Summary: Accessible ports on {host}")
    for port in accessible_ports:
        port_desc = next((desc for p, desc in ports_to_test if p == port), "Unknown")
        print(f"  {port} - {port_desc}")
    
    # Test actual RabbitMQ connection on accessible ports that might be AMQP
    print("\n" + "=" * 50)
    print("Testing actual RabbitMQ connections...")
    
    amqp_ports = [p for p in accessible_ports if p in [5672, 5671, 25672]]
    if amqp_ports:
        from config import Config
        for port in amqp_ports:
            print(f"\nTesting RabbitMQ connection on port {port}...")
            test_rabbitmq_connection_with_port(
                host, port, 
                Config.RABBITMQ_USER, 
                Config.RABBITMQ_PASS
            )
    else:
        print("No likely AMQP ports found accessible.") 