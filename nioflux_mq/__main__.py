from nioflux_mq.server import NioFluxMQServer


if __name__ == '__main__':
    NioFluxMQServer(host='0.0.0.0', port=None).run()
