import asyncio
import pickle
import websockets
import time

def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


# CLIENT SIDE

async def ws_get_ob(symbol):
    url = "ws://127.0.0.1:6666"
    msg = {}
    count = 0
    max_count = 10
    async with websockets.connect(url) as ws:
        done = False
        while not done:
            count += 1
            if count == max_count:
                print('ws_get_ob(' + symbol + '), max count reached')
                exit()
            try:
                await ws.send('get_ob(' + symbol + ')')
                msg = pickle.loads(await ws.recv())
                if 'asks' in msg and 'bids' in msg:
                    done = True
                else:
                    time.sleep(count)
            except Exception as e:
                print("ws_get_ob", type(e), e)
            # print(type(msg), msg, len(msg))
        return msg


async def ws_get_bal():
    url = "ws://127.0.0.1:6666"
    async with websockets.connect(url) as ws:
        await ws.send('get_bal()')
        msg = pickle.loads(await ws.recv())
        return msg


if __name__ == '__main__':
    ws_get_ob("BLOCK/BTC")
    ws_get_ob("LTC/BTC")
    print("done")
