# pip install --upgrade tornado websockets asyncio
# Websocket: ws://raspberrypi.local:4616

import json
import socket
import struct
import time
import traceback
import os
import asyncio
import queue
import threading
import tornado
import tornado.ioloop
import tornado.web
import tornado.websocket
from typing import Dict
import sys


bool_is_in_terminal_mode= sys.stdout.isatty()
if bool_is_in_terminal_mode:
    
    print("Running in a terminal.")
    stop_service_script ="""
    sudo systemctl stop apint_trusted_push_iid.service
    sudo systemctl stop apint_trusted_push_iid.timer
    """
    # run code to stop current service
    os.system(stop_service_script)
    
    # WHEN YOU NEED TO RESTART IT.
    """
    sudo systemctl restart apint_trusted_push_iid.service
    sudo systemctl restart apint_trusted_push_iid.timer
    """


bool_use_udp_redirection = False
int_max_byte_size = 16

# 4615 IS RESERVED FOR PUSH IID GATE WITH CRYPTO HANDSHAKE
server_websocket_port = 4625
server_websocket_mask= "0.0.0.0"

# Relay to the gate
broadcast_ip_gate="127.0.0.1"
broadcast_port_gates= [4615]

ntp_server = "127.0.0.1"



def get_ntp_time():
    import ntplib
    from time import ctime
    c = ntplib.NTPClient()
    response = c.request(ntp_server, version=3)
    return response.tx_time

def get_local_timestamp_in_ms_utc_since1970():
        return int(time.time()*1000)
    
def get_ntp_time_from_local():
    global millisecond_offset
    return asyncio.get_event_loop().time()*1000+millisecond_offset

bool_has_npt_server = False
try:
    ntp_timestamp = get_ntp_time()*1000
    bool_has_npt_server = True
except:
    print("IMPOSSIBLE TO CONNECT AT NTP")
local_timestamp = get_local_timestamp_in_ms_utc_since1970()
if bool_has_npt_server:
    print(f"NTP: {ntp_timestamp}")
else:
    ntp_timestamp = local_timestamp

millisecond_offset = ntp_timestamp-local_timestamp
print(f"ntp_timestmap: {ntp_timestamp}")
print(f"local_timestamp: {local_timestamp}")
print(f"diff: {millisecond_offset}")

default_user=-42

class UserHandshake:
    def __init__(self):
        self.address:str = ""
        self.remote_address:str = None          
        self.websocket= None       
        self.exit_handler=False
    
    def is_connection_lost(self):
        return self.exit_handler or self.websocket is None
        
        
                
guid_handshake_to_valide_user = {}
index_handshake_to_valide_user_list = {}

bool_use_debug_print = True
def debug_print(text):
    if bool_use_debug_print:
        print(text)
        
## If true bytes can only by of 16 bytes
bool_use_iid_max_size_kick_bytes=True 
## If true, the server accept text message
bool_allow_text_broadcasting= False
 

queue_broadcast_text_message = queue.Queue()

queue_broadcast_byte_iid_message = queue.Queue()
queue_broadcast_byte_message = queue.Queue()
 
 
async def relay_iid_message_as_local_udp_thread(byte):
    try:
        print(f"Relay UDP {byte}")
        for port in broadcast_port_gates:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(byte, (broadcast_ip_gate, port))
            sock.close()
    except Exception as e:
        print(f"UDP RELAY ERROR: {e}")
        traceback.print_exc()
            
int_push_count = 0
async def push_byte_or_close( user: UserHandshake,  b: bytes):
    global int_push_count
    int_push_count+=1
    print(f"Push count: {int_push_count}")
    if user is None or user.is_connection_lost():
        return
    try:
        ## Check buffer size before write message
        
        await user.websocket.write_message(b, binary=True)
    except tornado.websocket.WebSocketClosedError:

        print(f"WebSocketClosedError: Connection closed for user ") #{user.index}")
        user.websocket.close()
        remove_user_from_connected(user)
        
async def push_text_or_close( user: UserHandshake,  b: str):
    if user is None or user.is_connection_lost():
        return
    try:
        await user.websocket.write_message(b)
    except tornado.websocket.WebSocketClosedError:
        print(f"WebSocketClosedError: Connection closed for user {user.index}")
        user.websocket.close()
        remove_user_from_connected(user) 



 
bool_need_push_debug_text_bytes = False
bool_need_push_debug_iid = False
async def push_waiting_text_message():
    while not queue_broadcast_text_message.empty():
        message = queue_broadcast_text_message.get()
        for user in list_of_user_connected:
            await push_text_or_close(user, message)
    
    if bool_need_push_debug_text_bytes and is_terminal_mode():
        termnial_print(f"Pushing Text: {queue_broadcast_byte_iid_message}")
                
async def push_waiting_byte_message():
    while not queue_broadcast_byte_message.empty():
        message = queue_broadcast_byte_message.get()
        for user in list_of_user_connected:
            await push_byte_or_close(user, message)
            
    if bool_need_push_debug_text_bytes and is_terminal_mode():
        termnial_print(f"Pushing Bytes: {len(queue_broadcast_byte_iid_message)}")
    
async def push_waiting_byte_iid_message():
    
    
    while not queue_broadcast_byte_iid_message.empty():
        message = queue_broadcast_byte_iid_message.get()
        if bool_use_udp_redirection:
            await relay_iid_message_as_local_udp_thread(message)
        for user in list_of_user_connected:
            await push_byte_or_close(user, message)
                
    if bool_need_push_debug_iid and is_terminal_mode():
        termnial_print(f"Pushing IID: {queue_broadcast_byte_iid_message}")
                
async def push_waiting_message():
    await push_waiting_byte_iid_message()
    await push_waiting_byte_message()
    await push_waiting_text_message()
 
 
def is_terminal_mode():
    return bool_is_in_terminal_mode

def termnial_print(text):
    if bool_is_in_terminal_mode:
        print(text)
 
async def handle_text_message(user: UserHandshake, message: str):
    if not bool_allow_text_broadcasting:
        return
    queue_broadcast_text_message.put(message)
    termnial_print(message)
    
    

def user_to_json(user):
                return json.dumps(user.__dict__, indent=4, default=str)  



list_of_user_connected: UserHandshake= []
def add_user_to_connected(user: UserHandshake):
    list_of_user_connected.append(user)
    print (f"USER COUNT, ADD: {len(list_of_user_connected)}")
    
def remove_user_from_connected(user: UserHandshake):
    list_of_user_connected.remove(user)
    print (f"USER COUNT, REMOVE: {len(list_of_user_connected)}")
    
def remove_of_list_close_websocket():
    for user in list_of_user_connected:
        if user.is_connection_lost():
            remove_user_from_connected(user)


class WebSocketHandler(tornado.websocket.WebSocketHandler):
        async def open(self):
            print("WebSocket opened")
            self.user = UserHandshake()
            self.user.websocket = self
            self.user.exit_handler=False
            self.user.remote_address = self.request.remote_ip
            list_of_user_connected.append(self.user)
            
            print (f"New connection from {self.user.remote_address}")
            print(user_to_json(self.user))

            welcome = f"""
            WELCOME TO TRUSTED INTEGER IID SERVER
            DOCUMENTATION: https://github.com/EloiStree/2025_01_01_APIntTrustedLobbyIID
            WHAT IS AN IID: https://github.com/EloiStree/IID
            """
            
            if bool_use_iid_max_size_kick_bytes:
                welcome+="""
                RULE, ONLY IID BYTES SERVER: MAX 16 BYTES OR YOUR ARE KICKED 
                """
            if bool_allow_text_broadcasting:
                welcome+="""
                RULE, TEXT BROADCASTING IS ALLOWED (DONT ABUSE OF IT)
                """
            else:
                welcome+="""
                RULE, TEXT BROADCASTING IS DISABLED: YOUR TEXT MESSAGE ARE NOT BROADCASTED
                """
            await self.user.websocket.write_message(welcome)
            

            
        def is_connection_lost(self):
            return self.user.exit_handler or self.user.websocket is None
            


        async def on_message(self, message):
            global queue_broadcast_byte_message
            global queue_broadcast_byte_iid_message
            global queue_broadcast_text_message
            
            if isinstance(message, str):                    
                queue_broadcast_text_message.put(message)
            else:
                message_length = len(message)
                # Cast message to bytes
                bytes_to_stack = bytes(message)

                if message_length in {4, 8, 12, 16}:
                    if message_length == 4:
                        int_value = struct.unpack('<i', bytes_to_stack)[0]
                        bytes_to_stack = struct.pack('<iiQ', default_user, int_value, get_local_timestamp_in_ms_utc_since1970())
                        queue_broadcast_byte_iid_message.put(bytes_to_stack)

                    elif message_length == 8:
                        int_index, int_value = struct.unpack('<ii', bytes_to_stack)
                        bytes_to_stack = struct.pack('<iiQ', int_index, int_value, get_local_timestamp_in_ms_utc_since1970())
                        queue_broadcast_byte_iid_message.put(bytes_to_stack)

                    elif message_length == 12:
                        int_value, date = struct.unpack('<iQ', bytes_to_stack)
                        bytes_to_stack = struct.pack('<iiQ', default_user, int_value, date)
                        queue_broadcast_byte_iid_message.put(bytes_to_stack)

                    elif message_length == 16:
                        queue_broadcast_byte_iid_message.put(bytes_to_stack)

                else:
                    queue_broadcast_byte_message.put(message)


        def on_close(self):
            print("WebSocket closed")
            self.user.exit_handler=True
            remove_user_from_connected(self.user)

        def check_origin(self, origin):
            return True
        
def make_app():
    return tornado.web.Application([
        (r"/", WebSocketHandler),  # WebSocket endpoint
    ])    


async def udp_async_server():
    while True:        
        await push_waiting_message()
        await asyncio.sleep(0.0001)


        
def loop_udp_server():
    while True:
        try :
            termnial_print("WAIT FOR MESSAGES START")
            asyncio.run(udp_async_server())
        except Exception as e:
            termnial_print (f"RELAYER CRASHED: {e}")
            if is_terminal_mode():
                traceback.print_exc()
        time.sleep(2)

def print_local_ip():
    print("IPv4")
    hostname = socket.gethostname()
    print(f"Hostname: {hostname}")
    
    output = os.popen("hostname -I").read().strip()
    hostname = output
    ip_addresses = output.split()
    stack=""
    for ip in ip_addresses:
        if "." in ip:  
            stack+= ip+"\n"
    hostname_ip=stack
    print (f"Local IP: {hostname_ip}")
    

if __name__ == "__main__":

    print_local_ip()    
    
    server_thread = threading.Thread(target=loop_udp_server)
    server_thread.daemon = True 
    server_thread.start()
    

    tornado.websocket.WebSocketHandler.max_message_size = 100 * 1024 * 1024

    while True:
        try:
            app = make_app()
            app.listen(server_websocket_port)  
            print(f"Server started on ws://{server_websocket_mask}:{server_websocket_port}/")
            tornado.ioloop.IOLoop.current().start()
        except Exception as e:
            print (f"SERVER TORANDO CRASHED: {e}")
            traceback.print_exc()
        time.sleep(2)
        
    
    


