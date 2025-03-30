import socket
import time

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
        print("Adding broker to /etc/hosts as a fallback...")
        with open('/etc/hosts', 'a') as hosts_file:
            hosts_file.write('172.30.0.3 broker\n')
        time.sleep(1)
        resolve_broker()
