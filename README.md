# *NioFluxMQ*

> Built on [`NioFlux`](https://github.com/vortezwohl/NioFlux) networking framework.

*A thread-safe message queue implementation.*

## Installation

```bash
pip install -U nioflux-mq
```

```bash
uv add -U nioflux-mq
```

## Quick Start

1. Start a server

    ```bash
    python -m nioflux_mq.server
    ```

    ```
    PS D:\project\NioFluxMQ> py -m nioflux_mq.server
    [INFO] 2025-09-01 14:14:35,188 nioflux.mq : \
        _   __ _____                             
       / | / / ____/                             
      /  |/ / /___                               
     / /|  / ____/                               
    /_/ |_/_/                                    
    =0.0.0.0:26105=                              
    NioFluxMQServer started.  
    ```

2. Access MQ via clients

    1. Connect to a server

        ```python
        from nioflux_mq.client.client import NioFluxMQClient

        client = NioFluxMQClient(host='127.0.0.1', port=26105)
        ```

    2. Register a topic

        ```python
        client.register_topic('topic_0')
        ```

    3. Register a consumer

        ```python
        client.register_consumer('consumer_0')
        ```

    4. Sent a message to a topic

        ```python
        client.produce(b'message_0', 'topic_0', ttl=1)
        ```

    5. Consume a message

        ```python
        message = client.consume('consumer_0', 'topic_0').data
        client.advance('consumer_0', 'topic_0')
        ```

    6. Retreat the pointer if you missread some messages

        ```python
        client.retreat('consumer_0', 'topic_0')
        ```
