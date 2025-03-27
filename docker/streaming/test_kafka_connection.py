import socket
import time
import sys

def test_connection(host, port, max_retries=5):
    """Test TCP connection to a host:port with retries."""
    print(f"Testing connection to {host}:{port}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            # Create a socket object
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)  # Set timeout to 5 seconds
            
            # Attempt to connect
            result = s.connect_ex((host, port))
            
            if result == 0:
                print(f"✅ Success! Connected to {host}:{port}")
                return True
            else:
                print(f"❌ Attempt {attempt}/{max_retries}: Failed to connect to {host}:{port} - Error code: {result}")
            
            # Close the socket
            s.close()
            
            # Wait before retrying
            if attempt < max_retries:
                print(f"Waiting 2 seconds before retry...")
                time.sleep(2)
                
        except socket.error as e:
            print(f"❌ Attempt {attempt}/{max_retries}: Socket error: {e}")
            if attempt < max_retries:
                print(f"Waiting 2 seconds before retry...")
                time.sleep(2)
    
    print(f"Failed to connect to {host}:{port} after {max_retries} attempts")
    return False

if __name__ == "__main__":
    # Common Kafka addresses
    hosts_to_test = [
        ("agri_data_pipeline-kafka", 9092),
        ("kafka", 9092),
        ("localhost", 9092),
        ("127.0.0.1", 9092)
    ]
    
    print("=== Kafka Connectivity Test ===")
    print("Testing from container to various potential Kafka addresses")
    print("==================================")
    
    results = []
    for host, port in hosts_to_test:
        result = test_connection(host, port)
        results.append((host, port, result))
        print()
    
    print("=== Summary ===")
    for host, port, result in results:
        status = "✅ Success" if result else "❌ Failed"
        print(f"{status}: {host}:{port}")
    
    # Return non-zero exit code if all connections failed
    if not any(result for _, _, result in results):
        print("\nAll connection attempts failed.")
        sys.exit(1)
    else:
        print("\nAt least one connection succeeded.")
        sys.exit(0) 