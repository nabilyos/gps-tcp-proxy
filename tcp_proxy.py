# tcp_proxy.py
import asyncio
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TCP-Proxy")


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Just copy bytes from reader → writer"""
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception as e:
        logger.debug(f"Pipe error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def handle_client(
    local_reader: asyncio.StreamReader,
    local_writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int
) -> None:
    client_addr = local_writer.get_extra_info('peername')
    logger.info(f"New connection from {client_addr}")

    try:
        remote_reader, remote_writer = await asyncio.open_connection(target_host, target_port)
        logger.info(f"Connected to target {target_host}:{target_port} for client {client_addr}")

        # Bidirectional forwarding
        pipe1 = pipe(local_reader, remote_writer)
        pipe2 = pipe(remote_reader, local_writer)

        await asyncio.gather(pipe1, pipe2)

    except Exception as e:
        logger.error(f"Error with client {client_addr}: {e}")
    finally:
        local_writer.close()
        await local_writer.wait_closed()
        logger.info(f"Connection closed {client_addr}")


async def main(
    listen_host: str = "0.0.0.0",
    listen_port: int = 4001,
    target_host: str = "127.0.0.1",
    target_port: int = 5001
):
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, target_host, target_port),
        listen_host, listen_port
    )

    addr = server.sockets[0].getsockname()
    logger.info(f"TCP proxy listening on {addr} → forwarding to {target_host}:{target_port}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    # ← Change these values!
    LISTEN_PORT = 4001          # port where GPS trackers will connect
    TARGET_HOST = "tracker.server.com"   # or IP
    TARGET_PORT = 5012

    asyncio.run(main(listen_port=LISTEN_PORT, target_host=TARGET_HOST, target_port=TARGET_PORT))
