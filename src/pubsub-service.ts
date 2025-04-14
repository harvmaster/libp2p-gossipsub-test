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
    data: Uint8Array
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
    '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
    '/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa',
    '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb'
  ]
}

export const IDENTIFY_CONFIG: IdentifyInit = {
  protocolPrefix: 'libp2p-pubsub',
}

export class PubSubService extends EventEmitter<PubSubEventMap> {
  constructor(private node: Libp2p<{
    pubsub: PubSubInterface
    dht: KadDHT;
  }>) {
    super()

    // Handle pubsub messages
    this.node.services.pubsub.addEventListener('message', (message) => {
      console.log('message', message)
      this.emit('message', {
        topic: message.detail.topic,
        data: message.detail.data
      })
    })

    this.node.addEventListener('peer:discovery', async (evt) => {
      console.log('peer:discovery', evt)

      const peerId = evt.detail.id
      try {
        // await node.dial(peerId)

        const stream = await this.node.dialProtocol(peerId, ['/meshsub/1.1.0', '/meshsub/1.2.0', '/gossipsub/1.0.0']).catch(() => undefined);
        
        // If the stream is undefined, the peer does not support the protocol
        if (!stream) {
          this.node.hangUp(peerId)
          return;
        }

        console.log(`Peer ${peerId.toString()} supports Gossipsub.`)
      } catch (err) {
        console.error(`Error handling peer ${peerId.toString()}:`, err)
      }
    })

    // Handle peer connections
    this.node.addEventListener('peer:connect', (evt) => {
      console.log('peer:connect', evt)
      this.emit('peer:connect', {
        peerId: evt.detail.toString()
      })
    })

    this.node.addEventListener('peer:disconnect', (evt) => {
      this.emit('peer:disconnect', {
        peerId: evt.detail.toString()
      })
    })
  }

  static async create(options: Partial<Libp2pOptions> = {}): Promise<PubSubService> {
    const node = await createLibp2p({
      addresses: {
        listen: ['/ip4/0.0.0.0/tcp/0']
      },
      transports: [tcp()],
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
        }),
        identify: identify(),
        identifyPush: identifyPush(),
        dht: kadDHT({
          clientMode: false,
        }),
        ping: ping(),
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