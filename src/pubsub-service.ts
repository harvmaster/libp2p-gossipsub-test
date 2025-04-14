import { createLibp2p, Libp2p, Libp2pOptions } from 'libp2p'
import { GossipSub, gossipsub, GossipsubEvents } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify, IdentifyInit, identifyPush } from '@libp2p/identify'
import { KadDHT, kadDHT } from '@libp2p/kad-dht'
import { bootstrap } from '@libp2p/bootstrap'
import { mdns } from '@libp2p/mdns'
import { tcp } from '@libp2p/tcp'
import { ping } from '@libp2p/ping'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { peerIdFromString } from '@libp2p/peer-id'
import { autoNAT } from '@libp2p/autonat'
import { circuitRelayServer, circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { dcutr } from '@libp2p/dcutr'
import { uPnPNAT } from '@libp2p/upnp-nat'
import { rendezvousClient } from "@canvas-js/libp2p-rendezvous/client"
import { keys } from '@libp2p/crypto';

import { EventEmitter } from './event-emitter'

/**
 * TODO: Remove this. It should probably be available somewhere from libp2p-gossipsub
 */
type PubSubInterface = {
  subscribe: (topic: string) => void
  unsubscribe: (topic: string) => void
  publish: (topic: string, data: Uint8Array) => void
  addEventListener: <K extends keyof GossipsubEvents>(type: K, listener: (detail: GossipsubEvents[K]) => void, options?: boolean | AddEventListenerOptions | undefined) => void
  removeEventListener: <K extends keyof GossipsubEvents>(type: K, listener: (detail: GossipsubEvents[K]) => void, options?: boolean | EventListenerOptions | undefined) => void
}

export type PubSubEventMap = {
  'message': {
    topic: string
    data: string
  }
  'peer:connect': {
    peerId: string
  }
  'peer:disconnect': {
    peerId: string
  }
  'start': undefined
  'stop': undefined
}

export const BOOTSTRAP_CONFIG = {
  list: [
    // '/dnsaddr/relay.dev.libp2p.io/p2p/QmWDn2LY8nannvSWJzruUYoLZ4vV83vfCBwd8DipvdgQc3',
    // '/dnsaddr/relay.dev.libp2p.io/p2p/QmWDn2LY8nannvSWJzruUYoLZ4vV83vfCBwd8DipvdgQc4',

    // '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
    // '/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa',
    // '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb',
    // "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    // "/dnsaddr/bootstrap.libp2p.io/ipfs/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    // "/dnsaddr/bootstrap.libp2p.io/ipfs/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",

    "/dns/mc.hzuccon.com/tcp/45555/p2p/16Uiu2HAmMdUqriL1gLRpCppbUmt2ZCv9HSmGn9tboN37MnyJLJno",
    "/dns/mc.hzuccon.com/tcp/45556/p2p/16Uiu2HAmE78qTWuXGES1MJfVfGfQGWNqhB2gjiYWNJqq2ShEdE96",
  ]
}

export const IDENTIFY_CONFIG: IdentifyInit = {
  protocolPrefix: 'libp2p-pubsub',
}

const serverId = '12D3KooWRBYahbuA5xnVWu2DDa1F4v8tayJ9ATsSLEBWBqeSeNtK'

export class PubSubService extends EventEmitter<PubSubEventMap> {
  constructor(private node: Libp2p<{
    pubsub: PubSubInterface
    dht: KadDHT;
  }>) {
    super()

    // Handle pubsub messages
    this.node.services.pubsub.addEventListener('message', (message) => {
      const textDecoder = new TextDecoder()

      if (message.detail.topic === '_peer-discovery._p2p._pubsub') {
        // const peerId = textDecoder.decode(message.detail.data)
        console.log('pubsub peer discovered:', message.detail.data)
      //   const peerId = textDecoder.decode(message.detail.data)

      //   console.log('peerId', peerId)

      // this.node.dial(peerIdFromString(peerId))
        return;
      }

      // console.log('message', message)
      this.emit('message', {
        topic: message.detail.topic,
        data: textDecoder.decode(message.detail.data)
      })
    })

    this.node.addEventListener('peer:discovery', async (evt) => {
      // console.log('peer:discovery', evt)

      const peerId = evt.detail.id
      try {
        await this.node.dial(peerId)
      } catch (err) {
        console.error(`Error dialing peer ${peerId.toString()}:`)
      }
      //   console.log('peer:discovery', peerId)
      // try {

      //   const stream = await this.node.dialProtocol(peerId, ['/meshsub/1.1.0', '/meshsub/1.2.0', '/gossipsub/1.0.0']).catch(() => undefined);
        
      //   // If the stream is undefined, the peer does not support the protocol
      //   if (!stream) {
      //     // this.node.hangUp(peerId)
      //     return;
      //   }

      //   console.log(`Peer ${peerId.toString()} supports Gossipsub.`)
      // } catch (err) {
      //   console.error(`Error handling peer ${peerId.toString()}:`, err)
      // }
    })

    // Handle peer connections
    this.node.addEventListener('peer:connect', async (evt) => {
      const peerId = peerIdFromString(evt.detail.publicKey?.toString() ?? '')

      // // If the peer does not support gossipsub, we  drop it
      // const stream = await this.node.dialProtocol(peerId, ['/meshsub/1.1.0', '/meshsub/1.2.0', '/gossipsub/1.0.0']).catch(() => undefined);
      // if (!stream) {
      //   this.node.hangUp(peerId)
      //   return;
      // }

      // console.log('peer:connect', peerId.toString())
      this.emit('peer:connect', {
        peerId: peerId.toString()
      })
    })

    this.node.addEventListener('peer:disconnect', (evt) => {
      this.emit('peer:disconnect', {
        peerId: evt.detail.toString()
      })
    })

    setInterval(() => {
      console.log('peers', this.node.getPeers().length)
      console.log('connections', this.node.getConnections().length)
    }, 5000)

    console.log('node', this.node.getMultiaddrs())
  }

  static async create(config: { privateKey?: Uint8Array }): Promise<PubSubService> {
    const node = await createLibp2p({
      privateKey: config.privateKey
        ? keys.privateKeyFromRaw(config.privateKey)
        : undefined,
      addresses: {
        listen: [
          '/ip4/0.0.0.0/tcp/0',
          // '/ip6/::/tcp/0',
          '/p2p-circuit',
          // '/webrtc',
          // '/ip4/0.0.0.0/tcp/0'
        ]
      },
      transports: [tcp(), circuitRelayTransport()],
      peerDiscovery: [
        // mdns(),
        bootstrap(BOOTSTRAP_CONFIG),
        pubsubPeerDiscovery(),
      ],
      streamMuxers: [yamux()],
      connectionEncrypters: [noise()],
      services: {
        pubsub: gossipsub({
          allowPublishToZeroTopicPeers: true,
          floodPublish: true,
          maxInboundStreams: 1000,
          doPX: true,
          opportunisticGraftPeers: 4,
        }),
        autoNAT: autoNAT(),
        identify: identify(),
        identifyPush: identifyPush(),
        dht: kadDHT({
          clientMode: false,
        }),
        ping: ping(),
        relay: circuitRelayServer(),
        dcutr: dcutr(),
        uPnPNAT: uPnPNAT(),
        rendezvousClient: rendezvousClient({
          // autoRegister: {
          //   namespaces: ["XO", "XO-TEST"],
          //   multiaddrs: [
          //     "/dns/mc.hzuccon.com/tcp/45555/p2p/16Uiu2HAmMdUqriL1gLRpCppbUmt2ZCv9HSmGn9tboN37MnyJLJno",
          //     "/dns/mc.hzuccon.com/tcp/45556/p2p/16Uiu2HAmE78qTWuXGES1MJfVfGfQGWNqhB2gjiYWNJqq2ShEdE96",
          //     // "/ip4/144.6.52.212/tcp/40005/p2p/12D3KooWB9KGicYypBYSAiL8XEhFZXqYS935jEB6HDbGmkfbr5t7",
          //     // "/ip4/144.6.52.212/tcp/45081/p2p/12D3KooWRhRgeP7X7W7BmoJDyFTHDa6R326DeCP9qGoX9d4uxxzQ"
          //   ],
          // },
          autoDiscover: true,
        }),
      },
      connectionManager: {
        maxConnections: 10000,
        maxIncomingPendingConnections: 10000,
      }
    })

    return new PubSubService(node)
  }

  async start() {
    await this.node.start()
    console.log('Libp2p node started with ID:', this.node.peerId.toString())
  }

  async stop() {
    await this.node.stop()
  }

  async subscribe(topic: string) {
    await this.node.services.pubsub.subscribe(topic)
    console.log(`Subscribed to topic: ${topic}`)
  }
  
  async unsubscribe(topic: string) {
    await this.node.services.pubsub.unsubscribe(topic)
    console.log(`Unsubscribed from topic: ${topic}`)
  }

  async publish(topic: string, message: string) {
    await this.node.services.pubsub.publish(topic, new TextEncoder().encode(message))
  }

  getPeerId(): string {
    return this.node.peerId.toString()
  }

  getPeers(): string[] {
    return Array.from(this.node.getPeers()).map(peer => peer.toString())
  }
}