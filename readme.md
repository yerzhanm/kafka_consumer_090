simple consumer to get latest N messages
gets messages from partitions, will work correctly if messages are distributed by round robin
kafka version 0.9.0.0

### Generate project

OS X & Linux:

eclipse: `./gradlew eclipse`

idea: `./gradlew idea`

### Run

Before run set properly parameters
`final String brokers`
`final String zookeepers`
`final String topic`
`final String consumerGroup`
