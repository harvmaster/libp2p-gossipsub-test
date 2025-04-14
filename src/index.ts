import { PubSubService } from './pubsub-service'

// Create 2 instances of the PubSubService
// Start them both
// Wait for 15 seconds for connections to be made
// Subscribe both of them to the same topic
// Publish a message from nodeA to the topic
// Wait 15 seconds and print out the message from nodeB

const run = async () => {
  console.log('Starting node...')
  const node = await PubSubService.create({
    addresses: {
      // Listen on all interfaces
      listen: ['/ip4/0.0.0.0/tcp/0']
    }
  })

  // Log all peer connections
  node.on('peer:connect', ({ peerId }) => {
    // console.log('Connected to peer:', peerId)
  })

  // Log all messages from any topic
  node.on('message', ({ topic, data }) => {
    console.log('Received message on topic:', topic)
    console.log('Message:', new TextDecoder().decode(data))
  })

  await node.start()
  console.log('Node started with ID:', node.getPeerId())

  // Subscribe to topics you're interested in
  const topics = ['global-chat', 'announcements', 'updates']
  for (const topic of topics) {
    await node.subscribe(topic)
    console.log(`Subscribed to ${topic}`)
  }

  // Periodically publish messages
  setInterval(() => {
    const topic = topics[Math.floor(Math.random() * topics.length)]
    const message = `Hello from ${node.getPeerId()} at ${new Date().toISOString()}`
    
    node.publish(topic, message)
      .then(() => console.log(`Published to ${topic}: ${message}`))
      .catch(err => console.error(`Failed to publish to ${topic}:`, err))
  }, 5000)

  // Keep the process running
  process.on('SIGINT', async () => {
    console.log('Stopping node...')
    await node.stop()
    process.exit(0)
  })
}

run().catch(console.error)