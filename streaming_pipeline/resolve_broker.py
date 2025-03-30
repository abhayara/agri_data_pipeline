import socket
import time
import os

# Get broker IP from environment or use default
broker_ip = os.environ.get('BROKER_IP', '192.168.80.3')

# Attempt to resolve broker hostname
def resolve_broker():
    try:
        broker_ip = socket.gethostbyname('broker')
        print(f"✅ Resolved broker to {broker_ip}")
        return True
    except socket.gaierror:
        print("❌ Could not resolve broker hostname")
        return False

# Main function
if __name__ == "__main__":
    print("Attempting to resolve broker hostname...")
    if not resolve_broker():
        print(f"Adding broker {broker_ip} to /etc/hosts as a fallback...")
        try:
            with open('/etc/hosts', 'a') as hosts_file:
                hosts_file.write(f'{broker_ip} broker\n')
            time.sleep(1)
            resolve_broker()
        except Exception as e:
            print(f"Error updating hosts file: {e}")
