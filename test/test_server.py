#!/usr/bin/python3
"""测试 C++23 协程 Echo 服务器"""

import socket
import sys
import time

def test_echo_server():
    """测试 echo 服务器"""
    host = 'localhost'
    port = 9999
    
    print(f"Connecting to {host}:{port}...")
    
    try:
        # 创建 socket 连接
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, port))
        print("Connected!")
        
        # 发送数据
        message = 'Hello, Coroutine!'
        print(f"Sending: {message}")
        s.sendall(message.encode())
        
        # 接收回显数据
        data = s.recv(1024)
        received = data.decode()
        print(f"Received: {received}")
        
        s.close()
        
        if received == message:
            print('✓ Test PASSED: Echo server works correctly!')
            return True
        else:
            print('✗ Test FAILED: Data mismatch')
            return False
            
    except socket.timeout:
        print("✗ Test FAILED: Connection timeout")
        return False
    except ConnectionRefusedError:
        print("✗ Test FAILED: Connection refused")
        return False
    except Exception as e:
        print(f"✗ Test FAILED: {e}")
        return False

def test_multiple_messages():
    """测试发送多条消息"""
    host = 'localhost'
    port = 9999
    
    print(f"\nTesting multiple messages...")
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, port))
        
        messages = ['Hello', 'World', 'C++23', 'Coroutines!']
        
        for msg in messages:
            print(f"Sending: {msg}")
            s.sendall(msg.encode())
            data = s.recv(1024)
            received = data.decode()
            print(f"Received: {received}")
            
            if received != msg:
                print(f"✗ Test FAILED: Expected '{msg}', got '{received}'")
                s.close()
                return False
        
        s.close()
        print('✓ Multiple messages test PASSED!')
        return True
        
    except Exception as e:
        print(f"✗ Test FAILED: {e}")
        return False

if __name__ == '__main__':
    # 等待服务器启动

    success = True
    success = test_echo_server() and success
    success = test_multiple_messages() and success
    
    if success:
        print("\n=== All tests PASSED! ===")
        sys.exit(0)
    else:
        print("\n=== Some tests FAILED! ===")
        sys.exit(1)
